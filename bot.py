import json
import os
import re
from datetime import datetime, timedelta

import google.generativeai as genai
import psycopg2
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL")
GEMINI_KEYS_ENV = os.environ.get("GEMINI_API_KEY", "")
GEMINI_KEYS = [k.strip() for k in GEMINI_KEYS_ENV.split(",") if k.strip()] if GEMINI_KEYS_ENV else []
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")
DEBT_PAYMENT_INFO = os.environ.get(
    "DEBT_PAYMENT_INFO",
    "بنك فلسطين\nID: 1512081\nIBAN: PS11PALS045115120810993100000\nأو محفظة بال باي\n0597489605",
)

PRICES = {
    "شاي": 1,
    "قهوة مزاج وسط": 2,
    "قهوة مزاج كبير": 3,
    "نسكافيه مكس": 2,
    "كفي مكس": 2,
    "كابتشينو جوداي": 3,
    "كوكاكولا 330ملم": 4,
    "فانتا برتقال 330ملم": 4,
    "عصير راني 250ملم": 4,
    "بلو أزرق 250ملم": 4,
    "بلو أخضر 150ملم": 2,
    "بلو أزرق 150ملم": 2,
    "مراعي حليب شوكولاتة": 2,
    "عصير كوكتيل فواكه": 2,
    "لتر عصير برتقال": 7,
    "لتر عصير مانجا": 7,
    "سندويش فينو فيتا": 4,
    "سندويش فينو مرتديلا": 4,
    "سندويش فينو نوتيلا": 3,
    "سنيكرز": 3,
    "تويكس": 3,
    "مارس": 3,
    "مستر بايت": 3,
    "قسماط حجم وسط": 4,
    "بسكويت مالح": 2,
    "بسكويت ديمة فانيلا": 2,
    "مولتو ميني": 2,
    "لفيفا": 2,
    "حلو نعنع سكوتش": 1,
    "برنجلز أحمر صغير": 6,
    "برنجلز أحمر كبير": 11,
    "برنجلز أحمر كبير شطة": 11,
    "كيك فراولة": 7,
}

MENU_ITEMS = [
    {"id": "h1", "name": "شاي", "price": 1, "cat": "hot", "emoji": "🍵"},
    {"id": "h2", "name": "قهوة مزاج وسط", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h3", "name": "قهوة مزاج كبير", "price": 3, "cat": "hot", "emoji": "☕"},
    {"id": "h4", "name": "نسكافيه مكس", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h5", "name": "كفي مكس", "price": 2, "cat": "hot", "emoji": "☕"},
    {"id": "h6", "name": "كابتشينو جوداي", "price": 3, "cat": "hot", "emoji": "☕"},
    {"id": "c1", "name": "كوكاكولا 330ملم", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c2", "name": "فانتا برتقال 330ملم", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c3", "name": "عصير راني 250ملم", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c4", "name": "بلو أزرق 250ملم", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c5", "name": "بلو أزرق 150ملم", "price": 2, "cat": "cold", "emoji": "🥤"},
    {"id": "c6", "name": "بلو أخضر 150ملم", "price": 4, "cat": "cold", "emoji": "🥤"},
    {"id": "c7", "name": "مراعي حليب شوكولاتة", "price": 2, "cat": "cold", "emoji": "🥛"},
    {"id": "c8", "name": "عصير كوكتيل فواكه", "price": 2, "cat": "cold", "emoji": "🍹"},
    {"id": "c9", "name": "لتر عصير برتقال", "price": 7, "cat": "cold", "emoji": "🍊"},
    {"id": "c10", "name": "لتر عصير مانجا", "price": 7, "cat": "cold", "emoji": "🥭"},
    {"id": "s1", "name": "سندويش فينو فيتا", "price": 3, "cat": "snack", "emoji": "🥪"},
    {"id": "s2", "name": "سندويش فينو مرتديلا", "price": 3, "cat": "snack", "emoji": "🥪"},
    {"id": "s3", "name": "سندويش فينو نوتيلا", "price": 3, "cat": "snack", "emoji": "🥪"},
    {"id": "t1", "name": "سنيكرز", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t2", "name": "تويكس", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t3", "name": "مارس", "price": 3, "cat": "candy", "emoji": "🍫"},
    {"id": "t4", "name": "مستر بايت", "price": 3, "cat": "candy", "emoji": "🍬"},
    {"id": "t5", "name": "قسماط حجم وسط", "price": 4, "cat": "candy", "emoji": "🍪"},
    {"id": "t6", "name": "بسكويت مالح", "price": 2, "cat": "candy", "emoji": "🍪"},
    {"id": "t7", "name": "بسكويت ديمة فانيلا", "price": 2, "cat": "candy", "emoji": "🍪"},
    {"id": "t8", "name": "مولتو ميني", "price": 2, "cat": "candy", "emoji": "🧁"},
    {"id": "t9", "name": "لفيفا", "price": 2, "cat": "candy", "emoji": "🍫"},
    {"id": "t11", "name": "حلو نعنع سكوتش", "price": 1, "cat": "candy", "emoji": "🍬"},
    {"id": "t12", "name": "برنجلز أحمر صغير", "price": 6, "cat": "candy", "emoji": "🥔"},
    {"id": "t13", "name": "برنجلز أحمر كبير", "price": 11, "cat": "candy", "emoji": "🥔"},
    {"id": "t14", "name": "برنجلز أحمر كبير شطة", "price": 11, "cat": "candy", "emoji": "🌶️"},
    {"id": "t15", "name": "كيك فراولة", "price": 7, "cat": "candy", "emoji": "🍰"},
]

MENU_BY_NAME = {item["name"]: item for item in MENU_ITEMS}
SNACK_ITEM_NAMES = {item["name"] for item in MENU_ITEMS if item["cat"] == "snack"}
AR_NUMBERS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
ITEM_ALIASES = {
    "قهوة": "قهوة مزاج وسط",
    "مزاج": "قهوة مزاج وسط",
    "قهوة كبير": "قهوة مزاج كبير",
    "كوكا": "كوكاكولا",
    "بلو": "بلو أزرق",
    "سندويش فيتا": "سندويش فينو فيتا",
    "سندويش مرتديلا": "سندويش فينو مرتديلا",
    "برتقال": "لتر عصير برتقال",
    "مانجا": "لتر عصير مانجا",
    "شوكولاتة مراعي": "مراعي حليب شوكولاتة",
}


def get_pal_time():
    return (datetime.utcnow() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")


def parse_time(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def normalize_digits(text):
    return (text or "").translate(AR_NUMBERS)


def clean_office_name(office):
    return (office or "").strip()


def is_guest_office(office):
    return clean_office_name(office).startswith("زائر")


def get_db():
    return psycopg2.connect(DATABASE_URL)


def fetch_current_debt(cursor, office):
    cursor.execute(
        """
        SELECT COALESCE(SUM(total_price), 0)
        FROM orders
        WHERE location=%s
          AND status='مقبول'
          AND is_paid=0
          AND location NOT LIKE 'زائر%%'
        """,
        (office,),
    )
    return cursor.fetchone()[0] or 0


def get_active_reminder(cursor, office):
    cursor.execute(
        """
        SELECT id, office, amount, payment_info, is_active, is_seen, created_at
        FROM reminders
        WHERE office=%s AND is_active=1
        ORDER BY id DESC
        LIMIT 1
        """,
        (office,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "office": row[1],
        "amount": row[2] or 0,
        "payment_info": row[3] or DEBT_PAYMENT_INFO,
        "is_active": bool(row[4]),
        "is_seen": bool(row[5]),
        "created_at": row[6],
    }


def get_latest_payment_request(cursor, office):
    cursor.execute(
        """
        SELECT id, office, amount, receipt, status, created_at
        FROM debt_payment_requests
        WHERE office=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (office,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "office": row[1],
        "amount": row[2] or 0,
        "receipt": row[3],
        "status": row[4],
        "created_at": row[5],
    }


def build_local_ai_order(message):
    text = normalize_digits(message).lower()
    counts = {}
    for name in MENU_BY_NAME:
        if name.lower() in text:
            qty = 1
            before_match = re.search(r"(\d+)\s+" + re.escape(name.lower()), text)
            after_match = re.search(re.escape(name.lower()) + r"\s*(\d+)", text)
            if before_match:
                qty = int(before_match.group(1))
            elif after_match:
                qty = int(after_match.group(1))
            counts[name] = counts.get(name, 0) + max(qty, 1)

    for alias, real_name in ITEM_ALIASES.items():
        if alias.lower() in text and real_name not in counts:
            counts[real_name] = 1

    items = []
    total = 0
    for name, qty in counts.items():
        if name not in MENU_BY_NAME:
            continue
        items.append({"name": name, "qty": qty, "price": MENU_BY_NAME[name]["price"]})
        total += MENU_BY_NAME[name]["price"] * qty

    if not items:
        return None

    summary_parts = [f"{item['name']} x{item['qty']}" for item in items]
    reply = "جهزت لك الطلب:\n" + "\n".join(f"- {part}" for part in summary_parts) + f"\nالمجموع: {total} شيكل"
    return {"reply": reply, "items": items, "total": total}


def parse_gemini_json(text):
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1] if raw.count("```") >= 2 else raw
        raw = raw.replace("json", "", 1).strip()
        if "```" in raw:
            raw = raw.split("```", 1)[0].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(raw[start : end + 1])
    except Exception:
        return None

    items = []
    total = 0
    for entry in payload.get("items", []):
        name = clean_office_name(entry.get("name"))
        qty = int(entry.get("qty", 1) or 1)
        if name not in MENU_BY_NAME:
            continue
        items.append({"name": name, "qty": max(qty, 1), "price": MENU_BY_NAME[name]["price"]})
        total += MENU_BY_NAME[name]["price"] * max(qty, 1)

    reply = payload.get("reply") or "تم تجهيز اقتراح الطلب."
    if items and not payload.get("total"):
        payload["total"] = total
    return {"reply": reply, "items": items, "total": payload.get("total", total)}


def init_db():
    conn = get_db()
    conn.autocommit = True
    c = conn.cursor()

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT DEFAULT 0,
            details TEXT,
            total_price INTEGER,
            location TEXT,
            timestamp TEXT,
            status TEXT DEFAULT 'انتظار',
            is_paid INTEGER DEFAULT 0,
            receipt TEXT,
            order_type TEXT DEFAULT 'داخل الكوفي كورنر',
            missing_note TEXT,
            rating INTEGER DEFAULT 0,
            review_text TEXT,
            is_reviewed INTEGER DEFAULT 0,
            approved_at TEXT
        )
        """
    )

    for col, definition in [
        ("receipt", "TEXT"),
        ("order_type", "TEXT DEFAULT 'داخل الكوفي كورنر'"),
        ("missing_note", "TEXT"),
        ("rating", "INTEGER DEFAULT 0"),
        ("review_text", "TEXT"),
        ("is_reviewed", "INTEGER DEFAULT 0"),
        ("approved_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE orders ADD COLUMN {col} {definition}")
        except Exception:
            pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS reminders (
            id SERIAL PRIMARY KEY,
            office TEXT,
            amount INTEGER DEFAULT 0,
            payment_info TEXT,
            is_active INTEGER DEFAULT 1,
            is_seen INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    for col, definition in [
        ("is_active", "INTEGER DEFAULT 1"),
        ("amount", "INTEGER DEFAULT 0"),
        ("payment_info", "TEXT"),
        ("is_seen", "INTEGER DEFAULT 0"),
        ("created_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE reminders ADD COLUMN {col} {definition}")
        except Exception:
            pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS debt_payment_requests (
            id SERIAL PRIMARY KEY,
            office TEXT,
            amount INTEGER,
            receipt TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            reminder_id INTEGER
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id SERIAL PRIMARY KEY,
            amount INTEGER,
            receipt TEXT,
            created_at TEXT
        )
        """
    )

    c.close()
    conn.close()


try:
    init_db()
except Exception as exc:
    print(f"DB init error: {exc}")


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
    return {"error": "logo.png not found"}


@app.get("/api/menu")
async def get_menu():
    return {"items": MENU_ITEMS}


@app.post("/api/chat")
async def chat_with_ai(request: Request):
    data = await request.json()
    user_message = data.get("message", "")
    history = data.get("history", [])

    local_order = build_local_ai_order(user_message)

    menu_text = "\n".join([f"- {item['name']}: {item['price']} شيكل" for item in MENU_ITEMS])
    system_prompt = f"""
أنت مساعد طلبات ذكي في LE Coffee.
اعتمد فقط على هذه القائمة:
{menu_text}

أجب دائماً بصيغة JSON فقط وبدون أي نص خارج JSON:
{{
  "reply": "رد لطيف وقصير بالعربية",
  "items": [{{"name": "اسم مطابق للقائمة", "qty": 1}}],
  "total": 0
}}

إذا لم يكن الطلب واضحاً، اجعل items فارغة واطلب التوضيح في reply.
""".strip()

    if not GEMINI_KEYS:
        if local_order:
            return {"reply": local_order["reply"], "parsed_order": local_order}
        return {"reply": "اكتب الطلب باسم الأصناف الموجودة في المنيو وسأرتبه لك مباشرة.", "parsed_order": None}

    try:
        genai.configure(api_key=GEMINI_KEYS[0])
        model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=system_prompt)
        chat_history = []
        for msg in history:
            role = "model" if msg.get("role") == "model" else "user"
            chat_history.append({"role": role, "parts": [msg.get("content", "")]})

        chat = model.start_chat(history=chat_history)
        response = chat.send_message(user_message)
        parsed = parse_gemini_json(getattr(response, "text", ""))
        if parsed and parsed.get("items"):
            return {"reply": parsed["reply"], "parsed_order": parsed}
        if local_order:
            return {"reply": local_order["reply"], "parsed_order": local_order}
        if parsed:
            return {"reply": parsed["reply"], "parsed_order": None}
    except Exception:
        if local_order:
            return {"reply": local_order["reply"], "parsed_order": local_order}

    return {"reply": "ما فهمت الطلب بالكامل. اكتب الأصناف كما هي في المنيو وسأرتبها لك.", "parsed_order": None}


@app.post("/api/order")
async def create_order(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    items = data.get("items", [])
    total_price = int(data.get("total_price", 0) or 0)
    order_type = clean_office_name(data.get("order_type") or "داخل الكوفي كورنر")
    receipt = data.get("receipt")

    if not office or not items:
        return {"status": "error", "message": "missing order data"}

    if any(item in SNACK_ITEM_NAMES for item in items):
        order_type = "داخل الكوفي كورنر"

    is_guest = is_guest_office(office)
    status = "مقبول" if is_guest else "انتظار"
    is_paid = 1 if is_guest else 0
    approved_at = get_pal_time() if is_guest else None

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO orders
            (user_id, details, total_price, location, timestamp, status, is_paid, receipt, order_type, approved_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (0, ", ".join(items), total_price, office, get_pal_time(), status, is_paid, receipt, order_type, approved_at),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.delete("/api/order/{order_id}")
async def cancel_order(order_id: int):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "UPDATE orders SET status='ملغي' WHERE id=%s AND status IN ('انتظار','صنف_ناقص')",
            (order_id,),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.put("/api/order/{order_id}")
async def update_order(order_id: int, request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    items = data.get("items", [])
    total_price = int(data.get("total_price", 0) or 0)
    order_type = clean_office_name(data.get("order_type") or "داخل الكوفي كورنر")

    if not office or not items:
        return {"status": "error", "message": "missing order data"}

    if any(item in SNACK_ITEM_NAMES for item in items):
        order_type = "داخل الكوفي كورنر"

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            UPDATE orders
            SET details=%s, total_price=%s, order_type=%s, missing_note=NULL, status='انتظار'
            WHERE id=%s AND location=%s AND status IN ('انتظار','صنف_ناقص')
            """,
            (", ".join(items), total_price, order_type, order_id, office),
        )
        updated = c.rowcount
        conn.commit()
        c.close()
        conn.close()
        if not updated:
            return {"status": "error", "message": "order not found or not editable"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/debt-payment")
async def submit_debt_payment(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    amount = int(data.get("amount", 0) or 0)
    receipt = data.get("receipt")

    if not office or not amount or not receipt:
        return {"status": "error", "message": "missing debt payment data"}

    try:
        conn = get_db()
        c = conn.cursor()
        reminder = get_active_reminder(c, office)
        if not reminder:
            c.close()
            conn.close()
            return {"status": "error", "message": "لا يوجد تذكير دفع فعال لهذا المكتب"}

        current_debt = fetch_current_debt(c, office)
        if current_debt <= 0:
            c.close()
            conn.close()
            return {"status": "error", "message": "لا يوجد دين حالي لهذا المكتب"}

        c.execute(
            """
            INSERT INTO debt_payment_requests (office, amount, receipt, status, created_at, reminder_id)
            VALUES (%s,%s,%s,'pending',%s,%s)
            """,
            (office, amount, receipt, get_pal_time(), reminder["id"]),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/reminder/{reminder_id}/seen")
async def mark_reminder_seen(reminder_id: int):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE reminders SET is_seen=1 WHERE id=%s", (reminder_id,))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/user/sync/{office}")
async def sync_user(office: str):
    office = clean_office_name(office)
    guest = is_guest_office(office)
    try:
        conn = get_db()
        c = conn.cursor()

        active_order = None
        if not guest:
            c.execute(
                """
                SELECT id, details, total_price, status, missing_note, order_type
                FROM orders
                WHERE location=%s AND status IN ('انتظار','صنف_ناقص')
                ORDER BY id DESC
                LIMIT 1
                """,
                (office,),
            )
            row = c.fetchone()
            if row:
                active_order = {
                    "id": row[0],
                    "details": row[1],
                    "total_price": row[2],
                    "status": row[3],
                    "missing_note": row[4],
                    "order_type": row[5],
                }

        c.execute(
            """
            SELECT id, details, total_price, timestamp, is_paid, status, receipt, order_type
            FROM orders
            WHERE location=%s AND status NOT IN ('انتظار','صنف_ناقص','ملغي')
            ORDER BY id DESC
            """,
            (office,),
        )
        rows = c.fetchall()
        orders = [
            {
                "id": row[0],
                "details": row[1],
                "total_price": row[2],
                "timestamp": row[3],
                "is_paid": row[4],
                "status": row[5],
                "receipt": row[6],
                "order_type": row[7],
            }
            for row in rows
        ]

        total_debt = fetch_current_debt(c, office) if not guest else 0
        reminder = get_active_reminder(c, office) if not guest else None
        latest_payment_request = get_latest_payment_request(c, office) if not guest else None

        review_due = None
        if not guest:
            c.execute(
                """
                SELECT id, details, total_price, approved_at
                FROM orders
                WHERE location=%s
                  AND status='مقبول'
                  AND is_reviewed=0
                  AND approved_at IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (office,),
            )
            rev_row = c.fetchone()
            if rev_row:
                approved_time = parse_time(rev_row[3])
                now = datetime.utcnow() + timedelta(hours=3)
                if approved_time and (now - approved_time).total_seconds() >= 600:
                    review_due = {
                        "order_id": rev_row[0],
                        "details": rev_row[1],
                        "total_price": rev_row[2],
                    }

        c.close()
        conn.close()
        return {
            "status": "success",
            "active_order": active_order,
            "orders": orders,
            "total_debt": total_debt,
            "can_pay_debt": bool(reminder),
            "active_reminder": reminder,
            "latest_payment_request": latest_payment_request,
            "review_due": review_due,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/review")
async def submit_review(request: Request):
    data = await request.json()
    order_id = data.get("order_id")
    rating = data.get("rating", 5)
    text = data.get("text", "")
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "UPDATE orders SET rating=%s, review_text=%s, is_reviewed=1 WHERE id=%s",
            (rating, text, order_id),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/dashboard")
async def admin_dashboard():
    try:
        conn = get_db()
        c = conn.cursor()

        c.execute(
            """
            SELECT id, details, total_price, location, status, order_type, missing_note
            FROM orders
            WHERE status IN ('انتظار','صنف_ناقص')
            ORDER BY id ASC
            """
        )
        active_rows = c.fetchall()
        active_orders = [
            {
                "id": row[0],
                "details": row[1],
                "total_price": row[2],
                "location": row[3],
                "status": row[4],
                "order_type": row[5],
                "missing_note": row[6],
                "kind": "order",
            }
            for row in active_rows
        ]

        c.execute(
            """
            SELECT id, office, amount, receipt, status, created_at
            FROM debt_payment_requests
            WHERE status='pending'
            ORDER BY id ASC
            """
        )
        payment_rows = c.fetchall()
        for row in payment_rows:
            active_orders.append(
                {
                    "id": row[0],
                    "details": "طلب تسديد دين",
                    "total_price": row[2],
                    "location": row[1],
                    "status": row[4],
                    "order_type": "سداد دين",
                    "missing_note": None,
                    "receipt": row[3],
                    "timestamp": row[5],
                    "kind": "debt_payment",
                }
            )

        c.execute(
            """
            SELECT location, SUM(total_price)
            FROM orders
            WHERE status='مقبول' AND is_paid=0 AND location NOT LIKE 'زائر%%'
            GROUP BY location
            ORDER BY location ASC
            """
        )
        debt_rows = c.fetchall()
        debts = []
        for office, amount in debt_rows:
            reminder = get_active_reminder(c, office)
            debts.append(
                {
                    "office": office,
                    "amount": amount or 0,
                    "status": "غير مدفوع",
                    "has_active_reminder": bool(reminder),
                    "reminder_id": reminder["id"] if reminder else None,
                    "reminder_amount": reminder["amount"] if reminder else 0,
                    "reminder_created_at": reminder["created_at"] if reminder else None,
                }
            )

        c.execute("SELECT COALESCE(SUM(total_price), 0) FROM orders WHERE status='مقبول'")
        total_sales = c.fetchone()[0] or 0

        c.execute("SELECT COUNT(*) FROM orders WHERE status='مقبول'")
        total_count = c.fetchone()[0] or 0

        c.execute("SELECT COALESCE(SUM(total_price), 0) FROM orders WHERE status='مقبول' AND is_paid=1")
        paid_invoices = c.fetchone()[0] or 0

        c.execute(
            """
            SELECT COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول' AND is_paid=0 AND location NOT LIKE 'زائر%%'
            """
        )
        total_debts = c.fetchone()[0] or 0

        c.execute("SELECT COALESCE(SUM(amount), 0) FROM expenses")
        total_expenses = c.fetchone()[0] or 0
        total_profit = total_sales - total_expenses

        c.execute(
            """
            SELECT location, rating, details, review_text, timestamp
            FROM orders
            WHERE is_reviewed=1 AND rating > 0
            ORDER BY id DESC
            LIMIT 50
            """
        )
        reviews = [
            {"office": row[0], "rating": row[1], "details": row[2], "text": row[3], "date": row[4]}
            for row in c.fetchall()
        ]

        c.execute(
            """
            SELECT id, details, total_price, location, timestamp, receipt
            FROM orders
            WHERE location LIKE 'زائر%%' AND status='مقبول'
            ORDER BY id DESC
            LIMIT 20
            """
        )
        guest_orders = [
            {
                "id": row[0],
                "details": row[1],
                "total_price": row[2],
                "location": row[3],
                "timestamp": row[4],
                "receipt": row[5],
            }
            for row in c.fetchall()
        ]

        c.execute("SELECT id, amount, receipt, created_at FROM expenses ORDER BY id DESC")
        expenses = [
            {"id": row[0], "amount": row[1], "receipt": row[2], "created_at": row[3]}
            for row in c.fetchall()
        ]

        c.close()
        conn.close()
        return {
            "stats": {
                "total_sales": total_sales,
                "total_count": total_count,
                "paid_invoices": paid_invoices,
                "total_debts": total_debts,
                "total_expenses": total_expenses,
                "total_profit": total_profit,
            },
            "active_orders": active_orders,
            "debts": debts,
            "reviews": reviews,
            "guest_orders": guest_orders,
            "expenses": expenses,
        }
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/admin/action")
async def admin_action(request: Request):
    data = await request.json()
    action = data.get("action")
    order_id = data.get("order_id")
    office = clean_office_name(data.get("office"))
    try:
        conn = get_db()
        c = conn.cursor()

        if action == "approve":
            c.execute("UPDATE orders SET status='مقبول', approved_at=%s WHERE id=%s", (get_pal_time(), order_id))
        elif action == "missing":
            c.execute("UPDATE orders SET status='صنف_ناقص', missing_note=%s WHERE id=%s", (data.get("note"), order_id))
        elif action == "remind":
            if not office:
                c.close()
                conn.close()
                return {"status": "error", "message": "office is required"}
            amount = fetch_current_debt(c, office)
            c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
            c.execute(
                """
                INSERT INTO reminders (office, amount, payment_info, is_active, is_seen, created_at)
                VALUES (%s,%s,%s,1,0,%s)
                """,
                (office, amount, DEBT_PAYMENT_INFO, get_pal_time()),
            )
        elif action == "mark_paid":
            c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status='مقبول'", (office,))
            c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (office,))
            c.execute("UPDATE debt_payment_requests SET status='paid' WHERE office=%s AND status='pending'", (office,))
        elif action == "confirm_debt_payment":
            c.execute(
                "SELECT office FROM debt_payment_requests WHERE id=%s AND status='pending'",
                (order_id,),
            )
            payment_row = c.fetchone()
            if not payment_row:
                c.close()
                conn.close()
                return {"status": "error", "message": "payment request not found"}
            pay_office = payment_row[0]
            c.execute("UPDATE debt_payment_requests SET status='paid' WHERE id=%s", (order_id,))
            c.execute("UPDATE orders SET is_paid=1 WHERE location=%s AND status='مقبول' AND is_paid=0", (pay_office,))
            c.execute("UPDATE reminders SET is_active=0 WHERE office=%s", (pay_office,))
        elif action == "add_expense":
            amount = int(data.get("amount", 0) or 0)
            receipt = data.get("receipt")
            if amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "invalid expense amount"}
            c.execute(
                "INSERT INTO expenses (amount, receipt, created_at) VALUES (%s,%s,%s)",
                (amount, receipt, get_pal_time()),
            )
        elif action == "delete_expense":
            c.execute("DELETE FROM expenses WHERE id=%s", (order_id,))

        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
