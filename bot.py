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

# تفعيل CORS ليعمل الموقع ولوحة التحكم بسلاسة
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# الإعدادات من Render
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

# --- نماذج البيانات (API Models) ---
class OrderStatusUpdate(BaseModel):
    id: int
    status: str

# ---Endpoints للموظفين (الويب أب) ---

@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    office = data.get('office')
    items = data.get('items')
    total_price = data.get('total_price')
    details = ", ".join(items)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                  (0, details, total_price, office, get_pal_time(), "انتظار", 0))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/ai_process")
async def ai_process(request: Request):
    data = await request.json()
    text = data.get('text')
    
    if not GEMINI_KEYS: return {"status": "error", "message": "No API Keys"}

    try:
        genai.configure(api_key=random.choice(GEMINI_KEYS))
        model = genai.GenerativeModel('gemini-2.5-flash')
        prompt = f"أنت كاشير ذكي. استخرج الأصناف ورقم المكتب من النص: '{text}'. قائمة الأصناف: {list(PRICES.keys())}. رد بصيغة JSON فقط: {{'office': '15', 'items': ['شاي'], 'unmatched': []}}"
        response = await model.generate_content_async(prompt)
        result = json.loads(response.text.replace('```json', '').replace('```', '').strip())
        
        # حساب السعر الإجمالي للأصناف المكتشفة
        total = sum(PRICES.get(item, 0) for item in result.get('items', []))
        result['total_price'] = total
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Endpoints للكاشير (لوحة التحكم) ---

@app.get("/api/admin/orders")
async def get_admin_orders():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        
        # جلب الطلبات بانتظار التأكيد
        c.execute("SELECT id, details, total_price, location, timestamp FROM orders WHERE status='انتظار' ORDER BY id DESC")
        rows = c.fetchall()
        orders = [{"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4]} for r in rows]

        # جلب الإحصائيات (مبيعات اليوم والديون)
        today = datetime.utcnow().strftime("%Y-%m-%d")
        c.execute("SELECT SUM(total_price) FROM orders WHERE timestamp LIKE %s AND status='مقبول'", (f"{today}%",))
        sales = c.fetchone()[0] or 0
        
        c.execute("SELECT SUM(total_price) FROM orders WHERE is_paid=0 AND status='مقبول'")
        debts = c.fetchone()[0] or 0
        
        c.close()
        conn.close()
        return {"orders": orders, "stats": {"pending": len(orders), "sales": sales, "debts": debts}}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/admin/update_status")
async def update_order_status(data: OrderStatusUpdate):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("UPDATE orders SET status=%s WHERE id=%s", (data.status, data.id))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
def home():
    return {"status": "LE Coffee Server is Live"}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get('PORT', 8000)))