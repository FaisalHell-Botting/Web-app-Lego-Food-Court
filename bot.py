import os
import psycopg2
import json
from datetime import datetime, timedelta
import google.generativeai as genai
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn
import shutil

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

DATABASE_URL = os.environ.get('DATABASE_URL')
GEMINI_KEYS_ENV = os.environ.get('GEMINI_API_KEY', '')
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS_ENV.split(',') if k.strip()] if GEMINI_KEYS_ENV else []

if GEMINI_KEYS:
    genai.configure(api_key=GEMINI_KEYS[0])

PRICES = {
    'شاي': 1, 'قهوة مزاج وسط': 2, 'قهوة مزاج كبير': 3, 'نسكافيه مكس': 2, 'كفي مكس': 2, 'كابتشينو جوداي': 3,
    'كوكاكولا': 4, 'بلو أزرق': 4, 'مراعي حليب شوكلاتة': 2, 'عصير كوكتيل فواكه': 2, 'لتر عصير برتقال': 7, 'لتر عصير مانجا': 7,
    'سندويش فينو فيتا': 3, 'سندويش فينو مرتديلا': 3, 'سنيكرز': 3, 'تويكس': 3, 'مارس': 3, 'مستر بايت': 4,
    'قسماط حجم وسط': 4, 'بسكويت مالح': 2, 'بسكويت ديمة فانيلا': 2, 'مولتو ميني': 2, 'شكلاتة تجارية ب 2شيكل': 2,
    'شكلاتة تجارية ب 1 شيكل': 1, 'حلو نعنع سكوتش': 1, 'برنجلز أحمر صغير': 6, 'برنجلز أحمر كبير': 11,
    'برنجلز أحمر كبير شطة': 11, 'كيك فراولة': 7
}

def get_pal_time():
    return datetime.utcnow() + timedelta(hours=3)

# ─── API Endpoints ─────────────────────────────────────────────

@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        
        # Ensure tables exist (Adding new columns if needed for safe migration)
        c.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                location VARCHAR(50),
                is_guest INT DEFAULT 0,
                items TEXT,
                total_price INT,
                status VARCHAR(50) DEFAULT 'انتظار',
                created_at TIMESTAMP,
                approved_at TIMESTAMP,
                delivery_type VARCHAR(50) DEFAULT 'داخل الكفي كورنر',
                receipt_image VARCHAR(255) DEFAULT NULL,
                is_paid INT DEFAULT 0,
                missing_note TEXT
            )
        """)
        c.execute("CREATE TABLE IF NOT EXISTS reminders (id SERIAL PRIMARY KEY, office VARCHAR(50), is_active INT DEFAULT 1)")
        
        items_str = json.dumps(data.get('items', []), ensure_ascii=False)
        total = data.get('totalPrice', 0)
        loc = data.get('location', '')
        is_guest = 1 if data.get('isGuest') else 0
        delivery_type = data.get('deliveryType', 'داخل الكفي كورنر')
        
        c.execute(
            "INSERT INTO orders (location, is_guest, items, total_price, created_at, delivery_type) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            (loc, is_guest, items_str, total, get_pal_time(), delivery_type)
        )
        order_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success", "order_id": order_id}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/ai_order")
async def ai_order(request: Request):
    data = await request.json()
    text = data.get('text')
    if not GEMINI_KEYS: return {"error": "API Key not configured"}
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-lite')
        prompt = f"""
        قم بتحليل طلب العميل التالي واستخراج الأصناف والكميات المتطابقة مع القائمة المتاحة فقط:
        القائمة المتاحة: {', '.join(PRICES.keys())}
        الطلب: "{text}"
        أرجع النتيجة بصيغة JSON كمصفوفة من الكائنات، كل كائن يحتوي على "name" (اسم الصنف من القائمة) و "qty" (الكمية).
        لا تقم بإرجاع أي نص إضافي سوى الـ JSON.
        """
        response = model.generate_content(prompt)
        result_text = response.text.replace('```json', '').replace('```', '').strip()
        parsed_items = json.loads(result_text)
        return {"status": "success", "items": parsed_items}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/user/data")
async def user_data(office: str):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) if hasattr(psycopg2, 'extras') else conn.cursor()
        
        # Get Active Reminder
        c.execute("SELECT id FROM reminders WHERE office=%s AND is_active=1 ORDER BY id DESC LIMIT 1", (office,))
        has_reminder = bool(c.fetchone())
        
        # Get Active Orders
        c.execute("SELECT id, status, total_price, missing_note, items FROM orders WHERE location=%s AND status IN ('انتظار', 'جاري_التحضير', 'صنف_ناقص') ORDER BY id DESC", (office,))
        orders_data = c.fetchall()
        active_orders = [{"id": r[0], "status": r[1], "total": r[2], "note": r[3], "items": json.loads(r[4]) if r[4] else []} for r in orders_data]
        
        # Get Unpaid Debts (status 'مقبول' and is_paid=0)
        c.execute("SELECT id, created_at, items, total_price FROM orders WHERE location=%s AND status='مقبول' AND is_paid=0", (office,))
        debts_data = c.fetchall()
        total_debt = sum([r[3] for r in debts_data])
        debts_list = [{"id": r[0], "date": r[1].strftime("%Y-%m-%d %H:%M") if r[1] else "", "items": json.loads(r[2]) if r[2] else [], "total": r[3]} for r in debts_data]
        
        c.close()
        conn.close()
        return {"active_orders": active_orders, "has_reminder": has_reminder, "total_debt": total_debt, "debts_list": debts_list}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/user/pay_debt")
async def pay_debt(office: str = Form(...), amount: int = Form(...), file: UploadFile = File(...)):
    try:
        file_location = f"uploads/{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}"
        with open(file_location, "wb+") as file_object:
            shutil.copyfileobj(file.file, file_object)
            
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        # Insert as a special active order for admin to confirm
        c.execute(
            "INSERT INTO orders (location, is_guest, items, total_price, status, created_at, receipt_image) VALUES (%s, 0, %s, %s, 'طلب_تسديد', %s, %s)",
            (office, '[]', amount, get_pal_time(), f"/{file_location}")
        )
        # Deactivate reminder
        c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/admin/action")
async def admin_action(request: Request):
    data = await request.json()
    action = data.get('action')
    oid = data.get('order_id')
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        if action == 'approve':
            # automatically becomes debt if not guest
            c.execute("UPDATE orders SET status='مقبول', approved_at=%s WHERE id=%s", (get_pal_time(), oid))
        elif action == 'cancel':
            c.execute("UPDATE orders SET status='ملغي' WHERE id=%s", (oid,))
        elif action == 'confirm_payment':
            # Find the payment request
            c.execute("SELECT location, total_price FROM orders WHERE id=%s", (oid,))
            req = c.fetchone()
            if req:
                office = req[0]
                # Mark all unpaid orders for this office as paid
                c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status='مقبول' AND is_paid=0", (office,))
                # Mark the payment request itself as paid
                c.execute("UPDATE orders SET status='مدفوع', is_paid=1 WHERE id=%s", (oid,))
        elif action == 'remind':
            office = data.get('office')
            c.execute("INSERT INTO reminders (office, is_active) VALUES (%s, 1)", (office,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/admin/data")
async def admin_data():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        
        # Active Orders + Debt Payment Requests
        c.execute("SELECT id, location, items, total_price, status, created_at, delivery_type, receipt_image FROM orders WHERE status IN ('انتظار', 'جاري_التحضير', 'طلب_تسديد') ORDER BY id ASC")
        active_orders = [{"id": r[0], "location": r[1], "items": json.loads(r[2]) if r[2] else [], "total": r[3], "status": r[4], "time": r[5].strftime("%H:%M") if r[5] else "", "delivery_type": r[6], "receipt_image": r[7]} for r in c.fetchall()]
        
        # Ledger (Debts)
        c.execute("SELECT location, SUM(total_price) FROM orders WHERE status='مقبول' AND is_paid=0 GROUP BY location")
        debts = [{"office": r[0], "amount": r[1]} for r in c.fetchall()]
        
        c.close()
        conn.close()
        return {"active_orders": active_orders, "debts": debts}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
