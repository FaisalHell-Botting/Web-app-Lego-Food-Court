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
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS orders
                 (id SERIAL PRIMARY KEY, user_id BIGINT, details TEXT, total_price INTEGER, 
                 location TEXT, timestamp TEXT, status TEXT, is_paid INTEGER DEFAULT 0, receipt TEXT)''')
    
    cols = [
        ("order_type", "TEXT DEFAULT 'الكوفي كورنر'"),
        ("missing_note", "TEXT"),
        ("rating", "INTEGER DEFAULT 0"),
        ("review_text", "TEXT"),
        ("is_reviewed", "INTEGER DEFAULT 0")
    ]
    for col, definition in cols:
        try:
            c.execute(f"ALTER TABLE orders ADD COLUMN {col} {definition}")
        except psycopg2.errors.DuplicateColumn:
            pass

    c.execute('''CREATE TABLE IF NOT EXISTS reminders
                 (id SERIAL PRIMARY KEY, office TEXT, is_active INTEGER DEFAULT 1)''')
    c.close()
    conn.close()

init_db()

@app.post("/api/ai_process")
async def ai_process(request: Request):
    data = await request.json()
    text = data.get('text')
    if not GEMINI_KEYS: return {"status": "error", "message": "No API Keys"}
    try:
        genai.configure(api_key=random.choice(GEMINI_KEYS))
        model = genai.GenerativeModel('gemini-2.5-flash')
        prompt = f"أنت كاشير ذكي. استخرج الأصناف من النص: '{text}'. قائمة الأصناف المتاحة: {list(PRICES.keys())}. رد بصيغة JSON فقط: {{'items': ['شاي']}}"
        response = await model.generate_content_async(prompt)
        res_text = response.text.replace('```json', '').replace('```', '').strip()
        result = json.loads(res_text)
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
    receipt = data.get('receipt', '')
    order_type = data.get('order_type', 'الكوفي كورنر')
    
    details = ", ".join(items)
    is_guest = "زائر" in office
    status = "مقبول" if is_guest else "انتظار"
    is_paid = 1 if is_guest else 0

    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, receipt, order_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (0, details, total_price, office, get_pal_time(), status, is_paid, receipt, order_type))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/user/sync/{office}")
async def sync_user(office: str):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("SELECT id, details, total_price, status, missing_note, order_type FROM orders WHERE location=%s AND status IN ('انتظار', 'صنف_ناقص') ORDER BY id DESC LIMIT 1", (office,))
        active = c.fetchone()
        active_order = {"id": active[0], "details": active[1], "total_price": active[2], "status": active[3], "missing_note": active[4], "order_type": active[5]} if active else None

        c.execute("SELECT id FROM reminders WHERE office=%s AND is_active=1 LIMIT 1", (office,))
        can_pay_debt = True if c.fetchone() else False

        c.execute("SELECT id, details, total_price, timestamp, is_paid, status FROM orders WHERE location=%s ORDER BY id DESC", (office,))
        rows = c.fetchall()
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "timestamp": r[3], "is_paid": r[4], "status": r[5]} for r in rows]
        total_debt = sum(r["total_price"] for r in orders if r["is_paid"] == 0 and r["status"] in ["مقبول", "مكتمل"])

        review_needed = None
        for r in rows:
            if r[5] == 'مكتمل':
                c.execute("SELECT is_reviewed FROM orders WHERE id=%s", (r[0],))
                is_rev = c.fetchone()[0]
                if is_rev == 0:
                    order_time = datetime.strptime(r[3], "%Y-%m-%d %H:%M:%S")
                    if datetime.utcnow() + timedelta(hours=3) > order_time + timedelta(minutes=10):
                        review_needed = r[0]
                        break

        c.close()
        conn.close()
        return {"status": "success", "active_order": active_order, "can_pay_debt": can_pay_debt, "total_debt": total_debt, "orders": orders, "review_needed": review_needed}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/user/action")
async def user_action(request: Request):
    data = await request.json()
    action = data.get('action')
    order_id = data.get('order_id')
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        if action == 'cancel':
            c.execute("UPDATE orders SET status='ملغي' WHERE id=%s AND status='انتظار'", (order_id,))
        elif action == 'edit':
            new_details = data.get('details')
            new_price = data.get('total_price')
            c.execute("UPDATE orders SET details=%s, total_price=%s, status='انتظار', missing_note=NULL WHERE id=%s", (new_details, new_price, order_id))
        elif action == 'pay_debt':
            office = data.get('office')
            receipt = data.get('receipt')
            c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, receipt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                      (0, "تسديد ديون سابقة", 0, office, get_pal_time(), "تأكيد دفع", 1, receipt))
            c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status IN ('مقبول', 'مكتمل')", (office,))
            c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
        elif action == 'review':
            rating = data.get('rating')
            review_text = data.get('review_text')
            c.execute("UPDATE orders SET rating=%s, review_text=%s, is_reviewed=1 WHERE id=%s", (rating, review_text, order_id))
            
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/admin/dashboard")
async def admin_dashboard():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        
        c.execute("SELECT SUM(total_price) FROM orders WHERE status IN ('مقبول', 'مكتمل') AND is_paid=1")
        paid_invoices = c.fetchone()[0] or 0
        c.execute("SELECT SUM(total_price) FROM orders WHERE status IN ('مقبول', 'مكتمل')")
        total_sales = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(id) FROM orders WHERE status IN ('مقبول', 'مكتمل')")
        total_count = c.fetchone()[0] or 0
        c.execute("SELECT SUM(total_price) FROM orders WHERE status IN ('مقبول', 'مكتمل') AND is_paid=0")
        total_debt = c.fetchone()[0] or 0

        c.execute("SELECT id, details, total_price, location, timestamp, order_type FROM orders WHERE status='انتظار' ORDER BY id ASC")
        active_orders = [{"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4], "order_type": r[5]} for r in c.fetchall()]

        c.execute("SELECT location, SUM(total_price) as debt FROM orders WHERE is_paid=0 AND status IN ('مقبول', 'مكتمل') AND location NOT LIKE 'زائر%%' GROUP BY location ORDER BY debt DESC")
        debts = [{"office": r[0], "amount": r[1], "status": "غير مدفوع"} for r in c.fetchall() if r[1] > 0]
        
        c.execute("SELECT location, total_price, timestamp FROM orders WHERE is_paid=1 AND location LIKE 'زائر%%' ORDER BY id DESC LIMIT 10")
        for r in c.fetchall(): debts.append({"office": r[0], "amount": r[1], "status": "دفع فوري"})

        c.execute("SELECT location, details, rating, review_text, timestamp FROM orders WHERE is_reviewed=1 ORDER BY id DESC LIMIT 20")
        reviews = [{"office": r[0], "details": r[1], "rating": r[2], "text": r[3], "date": r[4]} for r in c.fetchall()]

        c.close()
        conn.close()
        return {"status": "success", "stats": {"total_sales": total_sales, "total_count": total_count, "paid_invoices": paid_invoices, "total_debts": total_debt}, "active_orders": active_orders, "debts": debts, "reviews": reviews}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/admin/action")
async def admin_action(request: Request):
    data = await request.json()
    action = data.get('action')
    order_id = data.get('order_id')
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        if action == 'approve':
            c.execute("UPDATE orders SET status='مقبول' WHERE id=%s", (order_id,))
        elif action == 'missing':
            note = data.get('note')
            c.execute("UPDATE orders SET status='صنف_ناقص', missing_note=%s WHERE id=%s", (note, order_id))
        elif action == 'remind':
            office = data.get('office')
            c.execute("INSERT INTO reminders (office, is_active) VALUES (%s, 1)", (office,))
            
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get('PORT', 8000)))
