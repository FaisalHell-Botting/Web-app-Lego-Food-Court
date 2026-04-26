import os
import psycopg2
import random
import json
from datetime import datetime, timedelta
import google.generativeai as genai
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel

app = FastAPI()

# تفعيل CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get('DATABASE_URL')
GEMINI_KEYS_ENV = os.environ.get('GEMINI_API_KEY', '')
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS_ENV.split(',') if k.strip()] if GEMINI_KEYS_ENV else []

PRICES = {
    'شاي': 1, 'قهوة مزاج وسط': 2, 'قهوة مزاج كبير': 3, 'نسكافيه مكس': 2, 'كفي مكس': 2, 'كابتشينو جوداي': 3,
    'كوكاكولا': 4, 'بلو أزرق': 4, 'مراعي حليب شوكلاتة': 2, 'عصير كوكتيل فواكه': 2, 'لتر عصير برتقال': 7, 'لتر عصير مانجا': 7,
    'سندويش فينو فيتا': 3, 'سندويش فينو مرتديلا': 3, 'سنيكرز': 3, 'تويكس': 3, 'مارس': 3, 'مستر بايت': 4, 
    'قسماط حجم وسط': 4, 'بسكويت مالح': 2, 'بسكويت ديمة فانيلا': 2, 'مولتو ميني': 2, 'شكلاتة تجارية ب 2شيكل': 2, 
    'شكلاتة تجارية ب 1 شيكل': 1, 'حلو نعنع سكوتش': 1, 'برنجلز أحمر صغير': 6, 'برنجلز أحمر كبير': 11, 
    'برنجلز أحمر كبير شطة': 11, 'كيك فراولة': 7
}

def get_pal_time():
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    # إنشاء الجدول لو مش موجود
    c.execute('''CREATE TABLE IF NOT EXISTS orders
                 (id SERIAL PRIMARY KEY, user_id BIGINT, details TEXT, total_price INTEGER, 
                 location TEXT, timestamp TEXT, status TEXT, is_paid INTEGER DEFAULT 0)''')
    
    # التأكد من إضافة الأعمدة الجديدة للجداول القديمة (عشان ما يعطي Error)
    try:
        c.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS receipt TEXT")
        c.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_paid INTEGER DEFAULT 0")
    except:
        pass # لو الأعمدة موجودة أصلاً رح يتجاهل الأمر
        
    # جدول التذكيرات
    c.execute('''CREATE TABLE IF NOT EXISTS reminders
                 (id SERIAL PRIMARY KEY, office TEXT, message TEXT, is_read INTEGER DEFAULT 0)''')
    
    conn.commit()
    c.close()
    conn.close()

init_db()

class StatusUpdate(BaseModel):
    id: int
    status: str

class ReminderReq(BaseModel):
    office: str
    amount: int

# ==========================================
# 1. API المستخدمين (الموقع)
# ==========================================

@app.post("/api/ai_process")
async def ai_process(request: Request):
    data = await request.json()
    text = data.get('text')
    if not GEMINI_KEYS: return {"status": "error", "message": "No API Keys"}
    try:
        genai.configure(api_key=random.choice(GEMINI_KEYS))
        model = genai.GenerativeModel('gemini-2.5-flash')
        prompt = f"أنت كاشير ذكي. استخرج الأصناف من: '{text}'. قائمة الأصناف: {list(PRICES.keys())}. رد بصيغة JSON فقط: {{'items': ['شاي'], 'unmatched': []}}"
        response = await model.generate_content_async(prompt)
        result = json.loads(response.text.replace('```json', '').replace('```', '').strip())
        total = sum(PRICES.get(item, 0) for item in result.get('items', []))
        result['total_price'] = total
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    office = data.get('office')
    items = data.get('items')
    total_price = data.get('total_price')
    receipt = data.get('receipt', '') # للزوار
    
    details = ", ".join(items)
    is_guest = "زائر" in office
    status = "مقبول" if is_guest else "انتظار"
    is_paid = 1 if is_guest else 0

    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, receipt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                  (0, details, total_price, office, get_pal_time(), status, is_paid, receipt))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/ledger/{office}")
async def get_ledger(office: str):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("SELECT id, details, total_price, timestamp, is_paid, status FROM orders WHERE location=%s ORDER BY id DESC", (office,))
        rows = c.fetchall()
        
        # فحص التذكيرات
        c.execute("SELECT id, message FROM reminders WHERE office=%s AND is_read=0", (office,))
        reminder = c.fetchone()
        if reminder:
            c.execute("UPDATE reminders SET is_read=1 WHERE id=%s", (reminder[0],))
            conn.commit()
            
        c.close()
        conn.close()
        
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "timestamp": r[3], "is_paid": r[4], "status": r[5]} for r in rows]
        total_debt = sum(r["total_price"] for r in orders if r["is_paid"] == 0 and r["status"] in ["مقبول", "مكتمل"])
        
        return {"status": "success", "orders": orders, "total_debt": total_debt, "reminder": reminder[1] if reminder else None}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/pay_debt")
async def pay_debt(request: Request):
    data = await request.json()
    office = data.get('office')
    receipt = data.get('receipt')
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        # إضافة طلب تسديد دين كحركة مالية
        c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, receipt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                  (0, "تسديد ديون سابقة", 0, office, get_pal_time(), "تأكيد دفع", 1, receipt))
        # تصفير الديون
        c.execute("UPDATE orders SET is_paid=1 WHERE location=%s", (office,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ==========================================
# 2. API الكاشير (لوحة التحكم)
# ==========================================

@app.get("/api/admin/active_orders") # تأكد أن الاسم هنا مطابق لما تطلبه في الـ HTML
async def get_active_orders():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        # جلب الطلبات
        c.execute("SELECT id, details, total_price, location, timestamp FROM orders WHERE status='انتظار' ORDER BY id DESC")
        rows = c.fetchall()
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4]} for r in rows]
        
        # حساب الإحصائيات (تأكد من وجودها لتعرض الأرقام فوق)
        # ... كود الحسابات ...
        
        c.close()
        conn.close()
        return {"orders": orders, "stats": {"sales_7_days": 100, "total_invoices": 500, "total_debts": 50}} # أرقام تجريبية
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/admin/orders")
async def get_admin_orders():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        
        # 1. الطلبات بانتظار التأكيد (الجارية)
        c.execute("SELECT id, details, total_price, location, timestamp FROM orders WHERE status='انتظار' ORDER BY id DESC")
        rows = c.fetchall()
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4]} for r in rows]

        # 2. حساب أرباح آخر 7 أيام (للطلبات المقبولة أو المكتملة)
        seven_days_ago = (datetime.utcnow() + timedelta(hours=3) - timedelta(days=7)).strftime("%Y-%m-%d")
        c.execute("SELECT SUM(total_price) FROM orders WHERE status IN ('مقبول', 'مكتمل') AND timestamp >= %s", (seven_days_ago,))
        sales_7_days = c.fetchone()[0] or 0
        
        # 3. إجمالي الفواتير داخل الكوفي كورنر (كل المبيعات المقبولة منذ البداية)
        c.execute("SELECT SUM(total_price) FROM orders WHERE status IN ('مقبول', 'مكتمل')")
        total_invoices = c.fetchone()[0] or 0

        # 4. إجمالي الديون المعلقة (كل ما هو 'مقبول/مكتمل' ولم يُدفع بعد)
        c.execute("SELECT SUM(total_price) FROM orders WHERE is_paid=0 AND status IN ('مقبول', 'مكتمل')")
        total_outstanding_debts = c.fetchone()[0] or 0
        
        c.close()
        conn.close()
        
        return {
            "orders": orders, 
            "stats": {
                "pending": len(orders),
                "sales_7_days": sales_7_days,
                "total_invoices": total_invoices,
                "total_debts": total_outstanding_debts
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/admin/history")
async def get_history():
    try:
        week_ago = (datetime.utcnow() + timedelta(hours=3) - timedelta(days=7)).strftime("%Y-%m-%d")
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("SELECT id, details, total_price, location, timestamp, status, is_paid FROM orders WHERE timestamp >= %s ORDER BY id DESC", (week_ago,))
        rows = c.fetchall()
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4], "status": r[5], "is_paid": r[6]} for r in rows]
        
        sales = sum(r['total_price'] for r in orders if r['status'] in ['مقبول', 'مكتمل'])
        c.close()
        conn.close()
        return {"orders": orders, "total_sales": sales}
    except Exception as e:
        return {"status": "error"}

@app.get("/api/admin/debts")
async def get_debts():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("SELECT location, SUM(total_price) FROM orders WHERE is_paid=0 AND status IN ('مقبول', 'مكتمل') AND location NOT LIKE 'زائر%%' GROUP BY location")
        rows = c.fetchall()
        debts = [{"office": r[0], "amount": r[1]} for r in rows if r[1] > 0]
        c.close()
        conn.close()
        return {"debts": debts}
    except Exception as e:
        return {"status": "error"}

@app.post("/api/admin/update_status")
async def update_status(data: StatusUpdate):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("UPDATE orders SET status=%s WHERE id=%s", (data.status, data.id))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error"}

@app.post("/api/admin/remind")
async def send_reminder(data: ReminderReq):
    try:
        msg = f"🔔 تذكير من الكاشير: يرجى تسديد الديون المتراكمة على مكتبك بقيمة {data.amount} شيكل."
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("INSERT INTO reminders (office, message) VALUES (%s, %s)", (data.office, msg))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error"}

@app.get("/")
def home():
    return {"status": "LE Coffee Server is Live"}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get('PORT', 8000)))
