import os
import psycopg2
from datetime import datetime, timedelta
import requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# السماح للموقع بالاتصال بالسيرفر (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# الإعدادات
DATABASE_URL = os.environ.get('DATABASE_URL')
TELEGRAM_TOKEN = os.environ.get('BOT_TOKEN', '8705243157:AAEvgDT3PecE8fmwc962NnToHnJl2xpFhAQ')
CASHIER_ID = os.environ.get('CASHIER_ID', '7447129659')
PORT = int(os.environ.get('PORT', 8000))

def get_pal_time():
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")

@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    office = data.get('office')
    items = data.get('items') # مصفوفة بالأصناف
    total_price = data.get('total_price')
    details = ", ".join(items)

    try:
        # 1. حفظ الطلب في قاعدة البيانات (Supabase)
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        # نضع user_id = 0 لأن الطلب من الموقع وليس من حساب تيليجرام
        c.execute("INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                  (0, details, total_price, office, get_pal_time(), "انتظار", 0))
        order_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()

        # 2. إرسال إشعار للكاشير عبر تيليجرام
        msg = f"🌐 **طلب جديد من الموقع!** 🌐\n🚨 طلب #{order_id}\n📍 {office}\n📦 {details}\n💰 {total_price} شيكل"
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CASHIER_ID, "text": msg, "parse_mode": "Markdown"})

        return {"status": "success", "message": "تم إرسال الطلب"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# نقطة فحص للتأكد أن السيرفر يعمل
@app.get("/")
def read_root():
    return {"message": "LE Coffee API is running"}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=PORT)