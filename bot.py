import os
import psycopg2
import json
from datetime import datetime, timedelta
import google.generativeai as genai
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

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

MENU_ITEMS = [
    # ساخن
    {"id": "h1", "name": "شاي", "price": 1, "cat": "hot", "emoji": "🍵"},
    {"id": "h2", "name": "قهوة مزاج وسط", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h3", "name": "قهوة مزاج كبير", "price": 3, "cat": "hot", "emoji": "☕"},
    {"id": "h4", "name": "نسكافيه مكس", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h5", "name": "كفي مكس", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h6", "name": "كابتشينو جوداي", "price": 3, "cat": "hot", "emoji": "☕"},
    # بارد
    {"id": "c1", "name": "كوكاكولا", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c2", "name": "بلو أزرق", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c3", "name": "مراعي حليب شوكلاتة", "price": 2, "cat": "cold", "emoji": "🥛"},
    {"id": "c4", "name": "عصير كوكتيل فواكه", "price": 2, "cat": "cold", "emoji": "🍹"},
    {"id": "c5", "name": "لتر عصير برتقال", "price": 7, "cat": "cold", "emoji": "🍊"},
    {"id": "c6", "name": "لتر عصير مانجا", "price": 7, "cat": "cold", "emoji": "🥭"},
    # أكل خفيف
    {"id": "s1", "name": "سندويش فينو فيتا", "price": 3, "cat": "snack", "emoji": "🥪"},
    {"id": "s2", "name": "سندويش فينو مرتديلا", "price": 3, "cat": "snack", "emoji": "🥪"},
    # تسالي
    {"id": "t1", "name": "سنيكرز", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t2", "name": "تويكس", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t3", "name": "مارس", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t4", "name": "مستر بايت", "price": 4, "cat": "candy", "emoji": "🍬"},
    {"id": "t5", "name": "قسماط حجم وسط", "price": 4, "cat": "candy", "emoji": "🍪"},
    {"id": "t6", "name": "بسكويت مالح", "price": 2, "cat": "candy", "emoji": "🍪"},
    {"id": "t7", "name": "بسكويت ديمة فانيلا", "price": 2, "cat": "candy", "emoji": "🍪"},
    {"id": "t8", "name": "مولتو ميني", "price": 2, "cat": "candy", "emoji": "🧁"},
    {"id": "t9", "name": "شكلاتة تجارية ب 2شيكل", "price": 2, "cat": "candy", "emoji": "🍫"},
    {"id": "t10", "name": "شكلاتة تجارية ب 1 شيكل", "price": 1, "cat": "candy", "emoji": "🍫"},
    {"id": "t11", "name": "حلو نعنع سكوتش", "price": 1, "cat": "candy", "emoji": "🍬"},
    {"id": "t12", "name": "برنجلز أحمر صغير", "price": 6, "cat": "candy", "emoji": "🥔"},
    {"id": "t13", "name": "برنجلز أحمر كبير", "price": 11, "cat": "candy", "emoji": "🥔"},
    {"id": "t14", "name": "برنجلز أحمر كبير شطة", "price": 11, "cat": "candy", "emoji": "🌶️"},
    {"id": "t15", "name": "كيك فراولة", "price": 7, "cat": "candy", "emoji": "🍰"},
]

PAYMENT_INFO = os.environ.get('LE_PAYMENT_INFO', 'يرجى تحويل المبلغ على حساب بنك القدس: 123456789 (رقم حساب تجريبي)')

def get_pal_time():
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS orders
                 (id SERIAL PRIMARY KEY, user_id BIGINT DEFAULT 0, details TEXT, total_price INTEGER,
                 location TEXT, timestamp TEXT, status TEXT DEFAULT 'انتظار', 
                 is_paid INTEGER DEFAULT 0, receipt TEXT,
                 order_type TEXT DEFAULT 'الكوفي كورنر',
                 missing_note TEXT,
                 rating INTEGER DEFAULT 0,
                 review_text TEXT,
                 is_reviewed INTEGER DEFAULT 0,
                 approved_at TEXT)''')
    
    extra_cols = [
        ("order_type", "TEXT DEFAULT 'الكوفي كورنر'"),
        ("missing_note", "TEXT"),
        ("rating", "INTEGER DEFAULT 0"),
        ("review_text", "TEXT"),
        ("is_reviewed", "INTEGER DEFAULT 0"),
        ("approved_at", "TEXT"),
        ("receipt", "TEXT"),
    ]
    for col, definition in extra_cols:
        try:
            c.execute(f"ALTER TABLE orders ADD COLUMN {col} {definition}")
        except:
            pass

    c.execute('''CREATE TABLE IF NOT EXISTS reminders 
                 (id SERIAL PRIMARY KEY, office TEXT, is_active INTEGER DEFAULT 1)''')
    c.close()
    conn.close()

try:
    init_db()
except Exception as e:
    print(f"DB init error: {e}")

# ─── Static files ───────────────────────────────────────────
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_index():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"message": "LE Coffee API"}

@app.get("/admin")
async def serve_admin():
    if os.path.exists("admin.html"):
        return FileResponse("admin.html")
    return {"error": "admin.html not found"}

@app.get("/logo.png")
async def serve_logo():
    if os.path.exists("logo.png"):
        return FileResponse("logo.png")
    return {"error": "logo not found"}

# ─── Menu endpoint ────────────────────────────────────────────
@app.get("/api/menu")
async def get_menu():
    return {"items": MENU_ITEMS}

# ─── AI Chat endpoint ─────────────────────────────────────────
@app.post("/api/chat")
async def chat_with_ai(request: Request):
    data = await request.json()
    user_message = data.get("message", "")
    history = data.get("history", [])

    menu_text = "\n".join([f"- {item['name']}: {item['price']} شيكل" for item in MENU_ITEMS])
    system_prompt = f"""أنت مساعد طلبات ذكي في مقهى LE Coffee. مهمتك مساعدة المستخدم في اختيار طلبه من القائمة.

قائمة المنتجات المتاحة:
{menu_text}

قواعد مهمة:
1. اقترح منتجات فقط من القائمة أعلاه
2. عندما يحدد المستخدم طلبه بشكل واضح، أعد ملخص الطلب بهذا الشكل الدقيق:
   طلبك: [اسم المنتج] x[الكمية], [اسم المنتج] x[الكمية]
   المجموع: [المبلغ] شيكل
3. كن ودوداً وموجزاً
4. لا تقترح منتجات خارج القائمة"""

    if not GEMINI_KEYS:
        return {"reply": "عذراً، خدمة الذكاء الاصطناعي غير متاحة حالياً. يرجى الطلب من المنيو مباشرة."}

    try:
        genai.configure(api_key=GEMINI_KEYS[0])
        model = genai.GenerativeModel('gemini-1.5-flash', system_instruction=system_prompt)
        
        chat_history = []
        for msg in history:
            chat_history.append({"role": msg["role"], "parts": [msg["content"]]})
        
        chat = model.start_chat(history=chat_history)
        response = chat.send_message(user_message)
        return {"reply": response.text}
    except Exception as e:
        return {"reply": f"عذراً، حدث خطأ. يرجى الطلب من المنيو مباشرة."}

# ─── Create Order ─────────────────────────────────────────────
@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    office = data.get('office', '').strip()
    items = data.get('items', [])
    total_price = data.get('total_price', 0)
    order_type = data.get('order_type', 'الكوفي كورنر')
    receipt = data.get('receipt', None)

    is_guest = office.startswith("زائر")
    status = "مقبول" if is_guest else "انتظار"
    is_paid = 1 if is_guest else 0
    approved_at = get_pal_time() if is_guest else None

    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute(
            "INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, receipt, approved_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            (0, ", ".join(items), total_price, office, get_pal_time(), status, is_paid, order_type, receipt, approved_at)
        )
        order_id = c.fetchone()[0]
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success", "order_id": order_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─── Cancel Order ─────────────────────────────────────────────
@app.delete("/api/order/{order_id}")
async def cancel_order(order_id: int):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("UPDATE orders SET status='ملغي' WHERE id=%s AND status IN ('انتظار','صنف_ناقص')", (order_id,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─── User Sync ────────────────────────────────────────────────
@app.get("/api/user/sync/{office}")
async def sync_user(office: str):
    office = office.strip()
    is_guest = office.startswith("زائر")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()

        # Active pending order (office users only)
        active_order = None
        if not is_guest:
            c.execute(
                "SELECT id, details, total_price, status, missing_note, order_type FROM orders WHERE location=%s AND status IN ('انتظار','صنف_ناقص') ORDER BY id DESC LIMIT 1",
                (office,)
            )
            row = c.fetchone()
            if row:
                active_order = {
                    "id": row[0], "details": row[1], "total_price": row[2],
                    "status": row[3], "missing_note": row[4], "order_type": row[5]
                }

        # Order history (all completed orders)
        c.execute(
            "SELECT id, details, total_price, timestamp, is_paid, status, receipt FROM orders WHERE location=%s AND status NOT IN ('انتظار','صنف_ناقص','ملغي') ORDER BY id DESC",
            (office,)
        )
        rows = c.fetchall()
        all_orders = [
            {"id": r[0], "details": r[1], "total_price": r[2], "timestamp": r[3],
             "is_paid": r[4], "status": r[5], "receipt": r[6]}
            for r in rows
        ]

        # Separate unpaid orders (debt)
        debt_orders = [o for o in all_orders if o["is_paid"] == 0]
        total_debt = sum(o["total_price"] for o in debt_orders)

        # Check if review pending
        review_due = None
        if not is_guest:
            c.execute(
                """SELECT id, details, total_price, approved_at FROM orders 
                   WHERE location=%s AND status='مقبول' AND is_reviewed=0 AND approved_at IS NOT NULL
                   ORDER BY id DESC LIMIT 1""",
                (office,)
            )
            rev_row = c.fetchone()
            if rev_row:
                try:
                    approved_time = datetime.strptime(rev_row[3], "%Y-%m-%d %H:%M:%S")
                    now = datetime.utcnow() + timedelta(hours=3)
                    if (now - approved_time).total_seconds() >= 600:
                        review_due = {
                            "order_id": rev_row[0],
                            "details": rev_row[1],
                            "total_price": rev_row[2]
                        }
                except:
                    pass

        # Reminder/can_pay
        c.execute("SELECT id FROM reminders WHERE office=%s AND is_active=1 LIMIT 1", (office,))
        can_pay = True if c.fetchone() else False

        c.close()
        conn.close()
        return {
            "status": "success",
            "active_order": active_order,
            "orders": all_orders,               # full history for reference
            "debt_orders": debt_orders,         # only unpaid for debt section
            "total_debt": total_debt,
            "can_pay_debt": can_pay,
            "review_due": review_due,
            "payment_info": PAYMENT_INFO
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─── Submit Review ────────────────────────────────────────────
@app.post("/api/review")
async def submit_review(request: Request):
    data = await request.json()
    order_id = data.get('order_id')
    rating = data.get('rating', 5)
    text = data.get('text', '')
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute("UPDATE orders SET rating=%s, review_text=%s, is_reviewed=1 WHERE id=%s", (rating, text, order_id))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─── Admin Dashboard ──────────────────────────────────────────
@app.get("/api/admin/dashboard")
async def admin_dashboard():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()

        # Active orders (pending, missing, debt_pay)
        c.execute("SELECT id, details, total_price, location, status, order_type, missing_note, receipt FROM orders WHERE status IN ('انتظار','صنف_ناقص','تسديد_دين') ORDER BY id ASC")
        active_rows = c.fetchall()
        active_orders = [
            {"id": r[0], "details": r[1], "total_price": r[2], "location": r[3],
             "status": r[4], "order_type": r[5], "missing_note": r[6], "receipt": r[7]}
            for r in active_rows
        ]

        # Approved orders (for ledger - office users only)
        c.execute(
            "SELECT location, SUM(total_price), MAX(is_paid) FROM orders WHERE status='مقبول' AND location NOT LIKE 'زائر%' GROUP BY location"
        )
        debt_rows = c.fetchall()
        debts = [
            {"office": r[0], "amount": r[1], "status": "مدفوع" if r[2] == 1 else "غير مدفوع"}
            for r in debt_rows
        ]

        # Stats
        c.execute("SELECT SUM(total_price) FROM orders WHERE status='مقبول'")
        total_sales = c.fetchone()[0] or 0

        c.execute("SELECT COUNT(*) FROM orders WHERE status='مقبول'")
        total_count = c.fetchone()[0] or 0

        c.execute("SELECT SUM(total_price) FROM orders WHERE status='مقبول' AND is_paid=1")
        paid_invoices = c.fetchone()[0] or 0

        c.execute("SELECT SUM(total_price) FROM orders WHERE status='مقبول' AND is_paid=0 AND location NOT LIKE 'زائر%'")
        total_debts = c.fetchone()[0] or 0

        # Reviews
        c.execute("SELECT location, rating, details, review_text, timestamp FROM orders WHERE is_reviewed=1 AND rating > 0 ORDER BY id DESC LIMIT 50")
        rev_rows = c.fetchall()
        reviews = [
            {"office": r[0], "rating": r[1], "details": r[2], "text": r[3], "date": r[4]}
            for r in rev_rows
        ]

        # Guest orders (for admin view - paid with receipt)
        c.execute("SELECT id, details, total_price, location, timestamp, receipt FROM orders WHERE location LIKE 'زائر%' AND status='مقبول' ORDER BY id DESC LIMIT 20")
        guest_rows = c.fetchall()
        guest_orders = [
            {"id": r[0], "details": r[1], "total_price": r[2], "location": r[3], "timestamp": r[4], "receipt": r[5]}
            for r in guest_rows
        ]

        c.close()
        conn.close()
        return {
            "stats": {
                "total_sales": total_sales,
                "total_count": total_count,
                "paid_invoices": paid_invoices,
                "total_debts": total_debts
            },
            "active_orders": active_orders,
            "debts": debts,
            "reviews": reviews,
            "guest_orders": guest_orders
        }
    except Exception as e:
        return {"error": str(e)}

# ─── Admin Action ─────────────────────────────────────────────
@app.post("/api/admin/action")
async def admin_action(request: Request):
    data = await request.json()
    action = data.get('action')
    oid = data.get('order_id')
    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        if action == 'approve':
            c.execute("UPDATE orders SET status='مقبول', approved_at=%s WHERE id=%s", (get_pal_time(), oid))
        elif action == 'missing':
            c.execute("UPDATE orders SET status='صنف_ناقص', missing_note=%s WHERE id=%s", (data.get('note'), oid))
        elif action == 'remind':
            office = data.get('office')
            c.execute("INSERT INTO reminders (office, is_active) VALUES (%s, 1)", (office,))
        elif action == 'mark_paid':
            office = data.get('office')
            c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status='مقبول'", (office,))
            c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
        elif action == 'approve_debt_pay':
            # oid is the debt_pay request order id
            c.execute("SELECT location, total_price FROM orders WHERE id=%s AND status='تسديد_دين'", (oid,))
            debt_order = c.fetchone()
            if debt_order:
                office = debt_order[0]
                # Mark debt pay request as paid
                c.execute("UPDATE orders SET status='مدفوع', is_paid=1 WHERE id=%s", (oid,))
                # Mark all unpaid approved orders for this office as paid
                c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status='مقبول' AND is_paid=0", (office,))
                # Deactivate reminders
                c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ─── Debt Payment Request ─────────────────────────────────────
@app.post("/api/debt-pay")
async def debt_pay_request(request: Request):
    data = await request.json()
    office = data.get('office', '').strip()
    amount = data.get('amount', 0)
    receipt = data.get('receipt', '')

    try:
        conn = psycopg2.connect(DATABASE_URL)
        c = conn.cursor()
        c.execute(
            "INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, receipt) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (0, f"تسديد دين مكتب {office}", amount, office, get_pal_time(), "تسديد_دين", 0, "تسديد_دين", receipt)
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get('PORT', 8000)))
