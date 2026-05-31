import hashlib
import json
import os
import random
import re
from difflib import SequenceMatcher
from datetime import datetime, timedelta

import google.generativeai as genai
import psycopg2
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

try:
    from pywebpush import WebPushException, webpush
except Exception:
    WebPushException = Exception
    webpush = None

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
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CLAIM_EMAIL = os.environ.get("VAPID_CLAIM_EMAIL", "admin@lecoffee.local")

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
    "مكسرات مشكل وزن 100جم": 11,
    "برنجلز أحمر صغير": 6,
    "برنجلز أحمر كبير": 11,
    "برنجلز أحمر كبير شطة": 11,
    "كيك فراولة": 7,
}

MENU_ITEMS = []

AR_NUMBERS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
ITEM_ALIASES = {
    "قهوة": "قهوة مزاج وسط",
    "مزاج": "قهوة مزاج وسط",
    "قهوة كبير": "قهوة مزاج كبير",
    "كوكا": "كوكاكولا 330ملم",
    "فانتا": "فانتا برتقال 330ملم",
    "راني": "عصير راني 250ملم",
    "بلو": "بلو أزرق 250ملم",
    "سندويش فيتا": "سندويش فينو فيتا",
    "سندويش مرتديلا": "سندويش فينو مرتديلا",
    "سندويش نوتيلا": "سندويش فينو نوتيلا",
    "برتقال": "لتر عصير برتقال",
    "مانجا": "لتر عصير مانجا",
    "شوكولاتة مراعي": "مراعي حليب شوكولاتة",
    "لفيفة": "لفيفا",
    "مكسرات": "مكسرات مشكل وزن 100جم",
}

CATEGORY_EMOJIS = {
    "hot": "☕",
    "cold": "🥤",
    "snack": "🥪",
    "candy": "🍬",
}
CATEGORY_ORDER = {"hot": 1, "cold": 2, "snack": 3, "candy": 4}
CANDY_TYPE_META = {
    "chocolate": {"label": "شوكولاتة", "emoji": "🍫", "order": 1},
    "biscuit": {"label": "بسكوت", "emoji": "🍪", "order": 2},
    "chips": {"label": "شبسي", "emoji": "🥔", "order": 3},
    "cake": {"label": "كيك", "emoji": "🍰", "order": 4},
    "nuts": {"label": "مكسرات", "emoji": "🥜", "order": 5},
    "sweet": {"label": "حلو", "emoji": "🍬", "order": 6},
}
VALID_MENU_CATEGORIES = set(CATEGORY_ORDER.keys())
VALID_CANDY_TYPES = set(CANDY_TYPE_META.keys())
REWARD_ORDER_TYPES = {'هدية مجانية'}
REWARD_EXCLUDED_ORDER_TYPES = {
    'تسوية دين يدوية',
    'إضافة يدوية',
    'حذف صنف من الدين',
    'رفض سداد الدين',
    'سداد دين',
    'سداد يدوي',
    'هدية مجانية',
}
ACCOUNTING_EXCLUDED_ORDER_TYPES = tuple(REWARD_EXCLUDED_ORDER_TYPES)
SALES_EXCLUDED_ORDER_TYPES = ('رفض سداد الدين', 'سداد دين', 'سداد يدوي', 'هدية مجانية')
REWARD_TIERS = [
    {'key': 'orders_3', 'kind': 'count', 'target': 10, 'prize_max': 2, 'title': 'أكملت 10 طلبات هذا الأسبوع'},
    {'key': 'amount_30', 'kind': 'amount', 'target': 60, 'prize_max': 2, 'title': 'مجموع طلباتك وصل 60 شيكل'},
    {'key': 'amount_60', 'kind': 'amount', 'target': 100, 'prize_max': 3, 'title': 'مجموع طلباتك وصل 100 شيكل'},
]


def infer_candy_type(item):
    name = str(item.get("name") or "")
    emoji = str(item.get("emoji") or "")
    if item.get("cat") != "candy":
        return ""
    if "🍫" in emoji or any(word in name for word in ["سنيكرز", "تويكس", "مارس", "لفيفا", "شوكولاتة"]):
        return "chocolate"
    if "🍪" in emoji or "بسك" in name or "قسماط" in name:
        return "biscuit"
    if "🥔" in emoji or "برنجلز" in name or "شيب" in name or "شبسي" in name:
        return "chips"
    if "🍰" in emoji or "🧁" in emoji or "كيك" in name or "مولتو" in name:
        return "cake"
    if "🥜" in emoji or "مكسرات" in name:
        return "nuts"
    return "sweet"


def get_menu_emoji(category, candy_type=""):
    if category == "candy":
        return CANDY_TYPE_META.get(candy_type, CANDY_TYPE_META["sweet"])["emoji"]
    return CATEGORY_EMOJIS.get(category, "☕")


def menu_sort_key(item):
    cat = item.get("cat") or ""
    candy_type = item.get("snack_type") or ""
    return (
        CATEGORY_ORDER.get(cat, 99),
        CANDY_TYPE_META.get(candy_type, {"order": 99})["order"] if cat == "candy" else 0,
        int(item.get("sort_order") or 0),
        str(item.get("name") or ""),
    )


def normalize_menu_row(row):
    item = {
        "db_id": row[0],
        "id": row[1],
        "name": row[2],
        "price": int(row[3] or 0),
        "cat": row[4],
        "emoji": get_menu_emoji(row[4], row[6] or ""),
        "snack_type": row[6] or "",
        "is_active": int(row[7] or 0),
        "is_deleted": int(row[8] or 0),
        "sort_order": int(row[9] or 0),
        "is_today_special": int(row[10] or 0),
    }
    if item["cat"] == "candy":
        item["snack_type_label"] = CANDY_TYPE_META.get(item["snack_type"], CANDY_TYPE_META["sweet"])["label"]
    return item


def fetch_menu_items(cursor, include_hidden=False):
    where = "COALESCE(is_deleted,0)=0"
    if not include_hidden:
        where += " AND COALESCE(is_active,1)=1"
    cursor.execute(
        f"""
        SELECT id, item_key, name, price, category, emoji, snack_type, is_active, is_deleted, sort_order, is_today_special
        FROM menu_items
        WHERE {where}
        ORDER BY category ASC, sort_order ASC, id ASC
        """
    )
    items = [normalize_menu_row(row) for row in cursor.fetchall()]
    return sorted(items, key=menu_sort_key)


def get_menu_by_name(cursor):
    return {item["name"]: item for item in fetch_menu_items(cursor, include_hidden=False)}


def build_order_snapshot(raw_items, menu_by_name):
    snapshot = []
    for raw_name in raw_items:
        name = clean_office_name(raw_name)
        if not name:
            continue
        item = menu_by_name.get(name)
        if not item:
            return None, name
        snapshot.append({"name": item["name"], "price": int(item["price"] or 0), "cat": item.get("cat") or ""})
    return snapshot, None


def parse_item_snapshot(value):
    if not value:
        return []
    try:
        data = json.loads(value)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict) and item.get("name")]
    except Exception:
        return []
    return []

def get_pal_time():
    return get_pal_datetime().strftime("%Y-%m-%d %H:%M:%S")


def get_reward_week_start(now=None):
    now = now or get_pal_datetime()
    week_start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    days_since_tuesday = (week_start.weekday() - 1) % 7
    week_start = week_start - timedelta(days=days_since_tuesday)
    if now < week_start:
        week_start = week_start - timedelta(days=7)
    return week_start


def get_reward_week_key(now=None):
    return get_reward_week_start(now).strftime("%Y-%m-%d %H:%M:%S")


def empty_reward_progress():
    week_key = get_reward_week_key()
    return {
        "week_start": week_key,
        "order_count": 0,
        "amount_total": 0,
        "has_ready_reward": False,
        "tiers": [
            {
                "key": tier["key"],
                "title": tier["title"],
                "kind": tier["kind"],
                "target": tier["target"],
                "progress": 0,
                "raw_progress": 0,
                "eligible": False,
                "status": "locked",
                "can_claim": False,
                "can_redeem": False,
                "reward": None,
            }
            for tier in REWARD_TIERS
        ],
    }


def fetch_reward_progress(cursor, office):
    week_start = get_reward_week_start()
    week_key = week_start.strftime("%Y-%m-%d %H:%M:%S")
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(total_price), 0)
        FROM orders
        WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
          AND status='مقبول'
          AND location NOT LIKE 'زائر%%'
          AND COALESCE(order_type, '') NOT IN %s
          AND COALESCE(approved_at, timestamp) >= %s
        """,
        (office_variants, office_number, tuple(REWARD_EXCLUDED_ORDER_TYPES), week_key),
    )
    order_count, amount_total = cursor.fetchone()
    order_count = int(order_count or 0)
    amount_total = int(amount_total or 0)
    cursor.execute(
        """
        SELECT id, reward_key, item_name, item_price, status, order_id
        FROM office_rewards
        WHERE office=%s AND week_start=%s
        """,
        (office, week_key),
    )
    reward_rows = {
        row[1]: {
            "id": row[0],
            "reward_key": row[1],
            "item_name": row[2],
            "item_price": int(row[3] or 0),
            "status": row[4],
            "order_id": row[5],
        }
        for row in cursor.fetchall()
    }
    tiers = []
    has_ready = False
    for tier in REWARD_TIERS:
        progress_value = order_count if tier["kind"] == "count" else amount_total
        eligible = progress_value >= tier["target"]
        reward = reward_rows.get(tier["key"])
        reward_status = reward["status"] if reward else "locked"
        can_claim = eligible and not reward
        can_redeem = bool(reward and reward_status == "claimed")
        has_ready = has_ready or can_claim or can_redeem
        tiers.append({
            "key": tier["key"],
            "title": tier["title"],
            "kind": tier["kind"],
            "target": tier["target"],
            "progress": min(progress_value, tier["target"]),
            "raw_progress": progress_value,
            "eligible": eligible,
            "status": reward_status,
            "can_claim": can_claim,
            "can_redeem": can_redeem,
            "reward": reward,
        })
    return {
        "week_start": week_key,
        "order_count": order_count,
        "amount_total": amount_total,
        "has_ready_reward": has_ready,
        "tiers": tiers,
    }


def select_reward_item(cursor, prize_max):
    cursor.execute(
        """
        SELECT name, price
        FROM menu_items
        WHERE COALESCE(is_deleted,0)=0
          AND COALESCE(is_active,1)=1
          AND price > 0
          AND price <= %s
        """,
        (prize_max,),
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    return random.choice(rows)


def get_pal_datetime():
    return datetime.utcnow() + timedelta(hours=3)


def is_store_open():
    now = get_pal_datetime()
    return (now.hour, now.minute) < (20, 30)


def store_closed_response():
    return {
        "status": "error",
        "message": "الطلبات مغلقة الآن. نستقبل الطلبات حتى الساعة 8:30 مساءً بتوقيت فلسطين.",
        "code": "store_closed",
    }


def is_valid_pin(pin):
    return bool(re.fullmatch(r"\d{4}", str(pin or "")))


def hash_pin(pin):
    return hashlib.sha256(str(pin).encode("utf-8")).hexdigest()


def generate_unique_pin(cursor):
    for _ in range(200):
        pin = f"{random.randint(0, 9999):04d}"
        cursor.execute("SELECT 1 FROM office_pins WHERE pin_hash=%s LIMIT 1", (hash_pin(pin),))
        if not cursor.fetchone():
            return pin
    return f"{random.randint(0, 9999):04d}"


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
    value = normalize_digits(office or "").strip()
    if re.fullmatch(r"\d{3}", value):
        return f"مكتب {value}"
    return value


def clean_push_endpoint(endpoint):
    return str(endpoint or "").strip()


def office_location_variants(office):
    value = clean_office_name(office)
    variants = [value]
    match = re.fullmatch(r"مكتب\s*(\d{3})", value)
    if match:
        number = match.group(1)
        variants.extend([number, f"مكتب{number}", f"مكتب {number}"])
    return tuple(dict.fromkeys(v for v in variants if v))


def office_number_value(office):
    value = clean_office_name(office)
    match = re.search(r"\d{3}", value)
    return match.group(0) if match else "__no_office_number__"


def is_guest_office(office):
    return clean_office_name(office).startswith("زائر")


def is_valid_office_number(office):
    office = clean_office_name(office)
    match = re.fullmatch(r"مكتب\s*(\d{3})", office)
    if not match:
        return False
    number = match.group(1)
    return number.startswith(("2", "4"))


def get_db():
    return psycopg2.connect(DATABASE_URL)


def push_is_configured():
    return bool(webpush and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)


def push_safe_tag(value):
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value or "")).strip("-")


def deactivate_push_subscriptions(cursor, office):
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        UPDATE push_subscriptions
        SET is_active=0, updated_at=%s
        WHERE office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s
        """,
        (get_pal_time(), office_variants, office_number),
    )


def send_push_notification(cursor, office, title, body, tag="", url="/"):
    if not push_is_configured() or not office:
        return
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT id, subscription
        FROM push_subscriptions
        WHERE is_active=1
          AND (office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s)
        """,
        (office_variants, office_number),
    )
    rows = cursor.fetchall()
    if not rows:
        return
    payload = json.dumps(
        {
            "title": title,
            "body": body,
            "tag": tag or push_safe_tag(f"{office}-{title}"),
            "url": url or "/",
        },
        ensure_ascii=False,
    )
    for sub_id, subscription in rows:
        try:
            subscription_info = json.loads(subscription) if isinstance(subscription, str) else subscription
            webpush(
                subscription_info=subscription_info,
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": f"mailto:{VAPID_CLAIM_EMAIL}"},
            )
            cursor.execute("UPDATE push_subscriptions SET last_used_at=%s WHERE id=%s", (get_pal_time(), sub_id))
        except WebPushException as exc:
            if getattr(exc, "response", None) is not None and getattr(exc.response, "status_code", 0) in (404, 410):
                cursor.execute("UPDATE push_subscriptions SET is_active=0, updated_at=%s WHERE id=%s", (get_pal_time(), sub_id))
        except Exception:
            pass


def send_reward_ready_notifications(cursor, office):
    try:
        progress = fetch_reward_progress(cursor, office)
        ready_tiers = [tier for tier in progress.get("tiers", []) if tier.get("can_claim") or tier.get("can_redeem")]
        for tier in ready_tiers:
            event_key = f"reward_ready:{progress.get('week_start')}:{office}:{tier.get('key')}"
            cursor.execute(
                "INSERT INTO push_notification_events (office, event_key, created_at) VALUES (%s,%s,%s) ON CONFLICT (office, event_key) DO NOTHING",
                (office, event_key, get_pal_time()),
            )
            if cursor.rowcount:
                send_push_notification(
                    cursor,
                    office,
                    "مبروك، لديك هدية",
                    "حصلها الآن من صفحة الهدايا.",
                    tag=push_safe_tag(event_key),
                    url="/",
                )
    except Exception:
        pass


def fetch_current_debt(cursor, office):
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT COALESCE(SUM(total_price), 0)
        FROM orders
        WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
          AND status='مقبول'
          AND is_paid=0
          AND location NOT LIKE 'زائر%%'
        """,
        (office_variants, office_number),
    )
    return cursor.fetchone()[0] or 0



def reminder_became_stale(cursor, office, reminder_created_at):
    if not reminder_created_at:
        return False
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT COALESCE(SUM(total_price), 0)
        FROM orders
        WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
          AND status='مقبول'
          AND is_paid=0
          AND COALESCE(approved_at, timestamp) <= %s
        """,
        (office_variants, office_number, reminder_created_at),
    )
    running_debt = int(cursor.fetchone()[0] or 0)
    cursor.execute(
        """
        SELECT total_price
        FROM orders
        WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
          AND status='مقبول'
          AND is_paid=0
          AND COALESCE(approved_at, timestamp) > %s
        ORDER BY COALESCE(approved_at, timestamp) ASC, id ASC
        """,
        (office_variants, office_number, reminder_created_at),
    )
    for row in cursor.fetchall():
        running_debt += int(row[0] or 0)
        if running_debt <= 0:
            return True
    return False


def deactivate_debt_collection_if_clear(cursor, office):
    if not office:
        return
    if int(fetch_current_debt(cursor, office) or 0) <= 0:
        office_variants = office_location_variants(office)
        office_number = office_number_value(office)
        cursor.execute(
            "UPDATE reminders SET is_active=0 WHERE office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s",
            (office_variants, office_number),
        )
        cursor.execute(
            "UPDATE debt_payment_requests SET status='cancelled' WHERE (office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s) AND status='pending'",
            (office_variants, office_number),
        )

def get_active_reminder(cursor, office):
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT id, office, amount, payment_info, is_active, is_seen, created_at
        FROM reminders
        WHERE (office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s) AND is_active=1
        ORDER BY id DESC
        LIMIT 1
        """,
        (office_variants, office_number),
    )
    row = cursor.fetchone()
    if not row:
        return None
    if reminder_became_stale(cursor, office, row[6]):
        cursor.execute("UPDATE reminders SET is_active=0 WHERE id=%s", (row[0],))
        cursor.execute("UPDATE debt_payment_requests SET status='cancelled' WHERE reminder_id=%s AND status='pending'", (row[0],))
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
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    cursor.execute(
        """
        SELECT id, office, amount, receipt, status, created_at
        FROM debt_payment_requests
        WHERE office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (office_variants, office_number),
    )
    row = cursor.fetchone()
    if not row:
        return None
    if reminder_became_stale(cursor, office, row[5]):
        cursor.execute("UPDATE reminders SET is_active=0 WHERE id=%s", (row[0],))
        cursor.execute("UPDATE debt_payment_requests SET status='cancelled' WHERE reminder_id=%s AND status='pending'", (row[0],))
        return None
    return {
        "id": row[0],
        "office": row[1],
        "amount": row[2] or 0,
        "receipt": row[3],
        "status": row[4],
        "created_at": row[5],
    }


AI_QTY_WORDS = {
    "واحد": 1, "واحدة": 1, "وحدة": 1, "حبة": 1, "حبه": 1,
    "اثنين": 2, "اتنين": 2, "ثنين": 2, "تنين": 2, "زوج": 2,
    "ثلاثة": 3, "ثلاث": 3, "تلاتة": 3, "تلات": 3,
    "اربعة": 4, "اربع": 4, "خمسة": 5, "خمس": 5,
    "ستة": 6, "ست": 6, "سبعة": 7, "سبع": 7,
    "ثمانية": 8, "ثمان": 8, "تمنية": 8, "تمن": 8,
    "تسعة": 9, "تسع": 9, "عشرة": 10, "عشر": 10,
}
AI_STOP_WORDS = {
    "بدي", "بدى", "اريد", "عايز", "عاوز", "ممكن", "لو", "سمحت", "هات", "جيب", "اعطيني", "اعطنى",
    "طلب", "واحد", "واحدة", "وحدة", "حبة", "حبه", "من", "مع", "على", "الى", "الي", "لوسمحت",
    "كبير", "وسط", "صغير", "بارد", "ساخن", "لتر", "مل", "ملم", "جم", "وزن",
}


def normalize_ai_text(value):
    text = normalize_digits(str(value or "")).lower()
    text = re.sub(r"[ًٌٍَُِّْـ]", "", text)
    replacements = {
        "أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي", "ئ": "ي", "ؤ": "و", "ة": "ه",
        "گ": "ك", "پ": "ب", "چ": "ج", "ڤ": "ف",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"[^\w\s\u0600-\u06ff]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def ai_quantity_near(text, start, end):
    before = text[max(0, start - 28):start].strip()
    after = text[end:end + 18].strip()
    patterns = [
        r"(?:^|\s)و?(\d{1,2})\s*$",
        r"(?:^|\s)و?(واحده?|وحده|حبه|اثنين|اتنين|ثنين|تنين|ثلاثه|ثلاث|تلاته|تلات|اربعه|اربع|خمسه|خمس|سته|ست|سبعه|سبع|ثمانيه|ثمان|تمنيه|تمن|تسعه|تسع|عشره|عشر)\s*$",
    ]
    for pattern in patterns:
        match = re.search(pattern, before)
        if match:
            value = match.group(1)
            return int(value) if value.isdigit() else AI_QTY_WORDS.get(value, 1)
    after_match = re.match(r"^\s*(\d{1,2}|واحده?|وحده|حبه|اثنين|اتنين|ثنين|تنين|ثلاثه|ثلاث|تلاته|تلات|اربعه|اربع|خمسه|خمس|سته|ست|سبعه|سبع|ثمانيه|ثمان|تمنيه|تمن|تسعه|تسع|عشره|عشر)(?:\s|$)", after)
    if after_match:
        value = after_match.group(1)
        return int(value) if value.isdigit() else AI_QTY_WORDS.get(value, 1)
    return 1


def ai_item_terms(item):
    name = item["name"]
    normalized = normalize_ai_text(name)
    terms = {normalized}
    without_units = re.sub(r"\b\d+\s*(ملم|مل|جم|g)\b", " ", normalized)
    without_units = re.sub(r"\s+", " ", without_units).strip()
    if without_units:
        terms.add(without_units)
    words = [w for w in without_units.split() if w not in AI_STOP_WORDS and not w.isdigit()]
    if len(words) >= 2:
        terms.add(" ".join(words[-2:]))
    if len(words) == 1 and len(words[0]) >= 4:
        terms.add(words[0])
    for alias, real_name in ITEM_ALIASES.items():
        if real_name == name:
            terms.add(normalize_ai_text(alias))
    return sorted([term for term in terms if term], key=len, reverse=True)


def build_local_ai_order(message, menu_items=None):
    menu_items = menu_items or []
    text = normalize_ai_text(message)
    if not text:
        return None

    matches = []
    occupied = []
    candidates = []
    for item in menu_items:
        for term in ai_item_terms(item):
            candidates.append((term, item))
    candidates.sort(key=lambda pair: len(pair[0]), reverse=True)

    for term, item in candidates:
        if len(term) < 3:
            continue
        for match in re.finditer(r"(?:^|\s|و)" + re.escape(term) + r"(?!\w)", text):
            start_pos, end_pos = match.span()
            if match.group(0).startswith((" ", "و")):
                start_pos += 1
            if any(not (end_pos <= a or start_pos >= b) for a, b in occupied):
                continue
            qty = max(ai_quantity_near(text, start_pos, end_pos), 1)
            matches.append((start_pos, item["name"], qty))
            occupied.append((start_pos, end_pos))
            break

    if not matches:
        words = [w for w in text.split() if w not in AI_STOP_WORDS and len(w) >= 3]
        for word in words:
            best_item = None
            best_score = 0
            for item in menu_items:
                for term in ai_item_terms(item):
                    score = SequenceMatcher(None, word, term).ratio()
                    if word in term:
                        score = max(score, 0.88)
                    if score > best_score:
                        best_score = score
                        best_item = item
            if best_item and best_score >= 0.86:
                matches.append((text.find(word), best_item["name"], 1))

    if not matches:
        return None

    counts = {}
    order_index = {}
    for idx, (pos, name, qty) in enumerate(sorted(matches, key=lambda row: row[0])):
        counts[name] = counts.get(name, 0) + qty
        order_index.setdefault(name, idx)

    menu_by_name = {item["name"]: item for item in menu_items}
    items = []
    total = 0
    for name in sorted(counts, key=lambda item_name: order_index[item_name]):
        item = menu_by_name.get(name)
        if not item:
            continue
        qty = min(max(int(counts[name] or 1), 1), 20)
        price = int(item.get("price", 0) or 0)
        items.append({"name": name, "qty": qty, "price": price})
        total += price * qty

    if not items:
        return None

    reply = "جهزت لك الطلب:\n" + "\n".join(f"- {item['name']} x{item['qty']}" for item in items) + f"\nالمجموع: {total} شيكل"
    return {"reply": reply, "items": items, "total": total}

def parse_gemini_json(text, menu_items=None):
    menu_items = menu_items or []
    menu_by_name = {item["name"]: item for item in menu_items}
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
        if name not in menu_by_name:
            continue
        price = int(menu_by_name[name].get("price", 0) or 0)
        items.append({"name": name, "qty": max(qty, 1), "price": price})
        total += price * max(qty, 1)

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
            approved_at TEXT,
            guest_phone TEXT,
            item_snapshot TEXT,
            payment_method TEXT,
            archive_hidden INTEGER DEFAULT 0
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
        ("guest_phone", "TEXT"),
        ("item_snapshot", "TEXT"),
        ("payment_method", "TEXT"),
        ("archive_hidden", "INTEGER DEFAULT 0"),
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
            reminder_id INTEGER,
            payment_method TEXT,
            archive_hidden INTEGER DEFAULT 0
        )
        """
    )
    c.execute("ALTER TABLE debt_payment_requests ADD COLUMN IF NOT EXISTS payment_method TEXT")
    c.execute("ALTER TABLE debt_payment_requests ADD COLUMN IF NOT EXISTS archive_hidden INTEGER DEFAULT 0")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id SERIAL PRIMARY KEY,
            amount INTEGER,
            receipt TEXT,
            created_at TEXT,
            description TEXT
        )
        """
    )

    c.execute("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS description TEXT")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS office_pins (
            id SERIAL PRIMARY KEY,
            office TEXT UNIQUE,
            pin_hash TEXT,
            updated_at TEXT
        )
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS office_pin_help (
            id SERIAL PRIMARY KEY,
            office TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            resolved_at TEXT
        )
        """
    )
    c.execute("ALTER TABLE office_pin_help ADD COLUMN IF NOT EXISTS new_pin TEXT")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id SERIAL PRIMARY KEY,
            office TEXT,
            endpoint TEXT UNIQUE,
            subscription TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT,
            updated_at TEXT,
            last_used_at TEXT
        )
        """
    )
    for col, definition in [
        ("office", "TEXT"),
        ("endpoint", "TEXT UNIQUE"),
        ("subscription", "TEXT"),
        ("is_active", "INTEGER DEFAULT 1"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
        ("last_used_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE push_subscriptions ADD COLUMN {col} {definition}")
        except Exception:
            pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS push_notification_events (
            id SERIAL PRIMARY KEY,
            office TEXT,
            event_key TEXT,
            created_at TEXT
        )
        """
    )
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS push_notification_events_unique ON push_notification_events (office, event_key)")
    except Exception:
        pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS office_rewards (
            id SERIAL PRIMARY KEY,
            office TEXT,
            week_start TEXT,
            reward_key TEXT,
            item_name TEXT,
            item_price INTEGER DEFAULT 0,
            status TEXT DEFAULT 'claimed',
            order_id INTEGER,
            created_at TEXT,
            claimed_at TEXT,
            ordered_at TEXT,
            approved_at TEXT
        )
        """
    )
    for col, definition in [
        ("office", "TEXT"),
        ("week_start", "TEXT"),
        ("reward_key", "TEXT"),
        ("item_name", "TEXT"),
        ("item_price", "INTEGER DEFAULT 0"),
        ("status", "TEXT DEFAULT 'claimed'"),
        ("order_id", "INTEGER"),
        ("created_at", "TEXT"),
        ("claimed_at", "TEXT"),
        ("ordered_at", "TEXT"),
        ("approved_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE office_rewards ADD COLUMN {col} {definition}")
        except Exception:
            pass
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS office_rewards_unique ON office_rewards (office, week_start, reward_key)")
    except Exception:
        pass

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS menu_items (
            id SERIAL PRIMARY KEY,
            item_key TEXT UNIQUE,
            name TEXT UNIQUE,
            price INTEGER NOT NULL DEFAULT 0,
            category TEXT NOT NULL,
            emoji TEXT,
            snack_type TEXT,
            is_active INTEGER DEFAULT 1,
            is_deleted INTEGER DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            is_today_special INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    for col, definition in [
        ("item_key", "TEXT UNIQUE"),
        ("name", "TEXT UNIQUE"),
        ("price", "INTEGER NOT NULL DEFAULT 0"),
        ("category", "TEXT NOT NULL DEFAULT 'hot'"),
        ("emoji", "TEXT"),
        ("snack_type", "TEXT"),
        ("is_active", "INTEGER DEFAULT 1"),
        ("is_deleted", "INTEGER DEFAULT 0"),
        ("sort_order", "INTEGER DEFAULT 0"),
        ("is_today_special", "INTEGER DEFAULT 0"),
        ("created_at", "TEXT"),
        ("updated_at", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE menu_items ADD COLUMN {col} {definition}")
        except Exception:
            pass

    c.execute("SELECT COUNT(*) FROM menu_items")
    if (c.fetchone()[0] or 0) == 0:
        now = get_pal_time()
        for idx, item in enumerate(MENU_ITEMS, start=1):
            category = item.get("cat") or "hot"
            snack_type = infer_candy_type(item)
            emoji = get_menu_emoji(category, snack_type)
            c.execute(
                """
                INSERT INTO menu_items (item_key, name, price, category, emoji, snack_type, is_active, is_deleted, sort_order, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,1,0,%s,%s,%s)
                ON CONFLICT (item_key) DO NOTHING
                """,
                (item.get("id"), item.get("name"), int(item.get("price", 0) or 0), category, emoji, snack_type, idx, now, now),
            )
    c.execute("ALTER TABLE office_pin_help ADD COLUMN IF NOT EXISTS pin_seen INTEGER DEFAULT 0")
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
        return FileResponse("index.html", media_type="text/html; charset=utf-8")
    return {"message": "LE Coffee API"}


@app.get("/admin")
async def serve_admin():
    if os.path.exists("admin.html"):
        return FileResponse("admin.html", media_type="text/html; charset=utf-8")
    return {"error": "admin.html not found"}


@app.get("/logo.png")
async def serve_logo():
    if os.path.exists("logo.png"):
        return FileResponse("logo.png")
    return {"error": "logo.png not found"}


@app.get("/manifest.webmanifest")
async def serve_manifest():
    if os.path.exists("manifest.webmanifest"):
        return FileResponse(
            "manifest.webmanifest",
            media_type="application/manifest+json; charset=utf-8",
            headers={"Cache-Control": "no-cache"},
        )
    return {"error": "manifest.webmanifest not found"}


@app.get("/manifest-admin.webmanifest")
async def serve_admin_manifest():
    if os.path.exists("manifest-admin.webmanifest"):
        return FileResponse(
            "manifest-admin.webmanifest",
            media_type="application/manifest+json; charset=utf-8",
            headers={"Cache-Control": "no-cache"},
        )
    return {"error": "manifest-admin.webmanifest not found"}


@app.get("/service-worker.js")
async def serve_service_worker():
    if os.path.exists("service-worker.js"):
        return FileResponse(
            "service-worker.js",
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Service-Worker-Allowed": "/",
            },
        )
    return {"error": "service-worker.js not found"}



@app.get("/api/store-status")
async def store_status():
    return {
        "is_open": is_store_open(),
        "message": "الطلبات متاحة حتى الساعة 8:30 مساءً بتوقيت فلسطين" if is_store_open() else "الطلبات مغلقة الآن. نستقبل الطلبات حتى الساعة 8:30 مساءً بتوقيت فلسطين.",
    }


@app.get("/api/push/config")
async def push_config():
    return {"enabled": push_is_configured(), "public_key": VAPID_PUBLIC_KEY if push_is_configured() else ""}


@app.get("/api/office-pin-status/{office}")
async def office_pin_status(office: str):
    office = clean_office_name(office)
    if not office or is_guest_office(office) or not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT 1 FROM office_pins WHERE office=%s LIMIT 1", (office,))
        exists = bool(c.fetchone())
        c.close()
        conn.close()
        return {"status": "success", "has_pin": exists, "is_open": is_store_open()}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/office-pin/setup")
async def setup_office_pin(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    pin = str(data.get("pin", "")).strip()
    if not office or is_guest_office(office) or not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}
    if not is_valid_pin(pin):
        return {"status": "error", "message": "الرقم السري يجب أن يكون 4 أرقام"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT 1 FROM office_pins WHERE office=%s LIMIT 1", (office,))
        if c.fetchone():
            c.close()
            conn.close()
            return {"status": "error", "message": "يوجد رقم سري لهذا المكتب"}
        c.execute(
            "INSERT INTO office_pins (office, pin_hash, updated_at) VALUES (%s,%s,%s)",
            (office, hash_pin(pin), get_pal_time()),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/office-pin/verify")
async def verify_office_pin(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    pin = str(data.get("pin", "")).strip()
    if not office or not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}
    if not is_valid_pin(pin):
        return {"status": "error", "message": "رقم سري غير صحيح"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT pin_hash FROM office_pins WHERE office=%s LIMIT 1", (office,))
        row = c.fetchone()
        c.close()
        conn.close()
        if row and row[0] == hash_pin(pin):
            return {"status": "success"}
        return {"status": "error", "message": "رقم سري غير صحيح"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/push/status")
async def push_status(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    endpoint = clean_push_endpoint(data.get("endpoint"))
    if not office or not endpoint:
        return {"status": "success", "active": False, "enabled": push_is_configured()}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT COALESCE(is_active, 0)
            FROM push_subscriptions
            WHERE office=%s AND endpoint=%s
            ORDER BY id DESC
            LIMIT 1
            """,
            (office, endpoint),
        )
        row = c.fetchone()
        c.close()
        conn.close()
        return {"status": "success", "active": bool(row and row[0]), "enabled": push_is_configured()}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "active": False, "enabled": push_is_configured()}


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    pin = str(data.get("pin", "")).strip()
    subscription = data.get("subscription") or {}
    endpoint = clean_push_endpoint(subscription.get("endpoint"))
    if not push_is_configured():
        return {"status": "error", "message": "الإشعارات غير مفعلة على السيرفر حالياً"}
    if not office or is_guest_office(office) or not is_valid_office_number(office) or not is_valid_pin(pin) or not endpoint:
        return {"status": "error", "message": "بيانات الإشعارات غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT pin_hash FROM office_pins WHERE office=%s LIMIT 1", (office,))
        row = c.fetchone()
        if not row or row[0] != hash_pin(pin):
            c.close()
            conn.close()
            return {"status": "error", "message": "الرقم السري للمكتب غير صحيح"}
        now = get_pal_time()
        c.execute(
            """
            INSERT INTO push_subscriptions (office, endpoint, subscription, is_active, created_at, updated_at, last_used_at)
            VALUES (%s,%s,%s,1,%s,%s,%s)
            ON CONFLICT (endpoint) DO UPDATE
            SET office=EXCLUDED.office,
                subscription=EXCLUDED.subscription,
                is_active=1,
                updated_at=EXCLUDED.updated_at
            """,
            (office, endpoint, json.dumps(subscription), now, now, now),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    pin = str(data.get("pin", "")).strip()
    endpoint = clean_push_endpoint(data.get("endpoint"))
    if not office or is_guest_office(office) or not is_valid_office_number(office) or not is_valid_pin(pin):
        return {"status": "error", "message": "بيانات الإشعارات غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT pin_hash FROM office_pins WHERE office=%s LIMIT 1", (office,))
        row = c.fetchone()
        if not row or row[0] != hash_pin(pin):
            c.close()
            conn.close()
            return {"status": "error", "message": "الرقم السري للمكتب غير صحيح"}
        if endpoint:
            c.execute("UPDATE push_subscriptions SET is_active=0, updated_at=%s WHERE office=%s AND endpoint=%s", (get_pal_time(), office, endpoint))
        else:
            deactivate_push_subscriptions(c, office)
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/admin/push/status")
async def admin_push_status(request: Request):
    data = await request.json()
    endpoint = clean_push_endpoint(data.get("endpoint"))
    if not endpoint:
        return {"status": "success", "active": False, "enabled": push_is_configured()}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "SELECT COALESCE(is_active, 0) FROM push_subscriptions WHERE office='__admin__' AND endpoint=%s ORDER BY id DESC LIMIT 1",
            (endpoint,),
        )
        row = c.fetchone()
        c.close()
        conn.close()
        return {"status": "success", "active": bool(row and row[0]), "enabled": push_is_configured()}
    except Exception as exc:
        return {"status": "error", "message": str(exc), "active": False, "enabled": push_is_configured()}


@app.post("/api/admin/push/subscribe")
async def admin_push_subscribe(request: Request):
    data = await request.json()
    subscription = data.get("subscription") or {}
    endpoint = clean_push_endpoint(subscription.get("endpoint"))
    if not push_is_configured():
        return {"status": "error", "message": "الإشعارات غير مفعلة على السيرفر حالياً"}
    if not endpoint:
        return {"status": "error", "message": "بيانات الإشعارات غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        now = get_pal_time()
        c.execute(
            """
            INSERT INTO push_subscriptions (office, endpoint, subscription, is_active, created_at, updated_at, last_used_at)
            VALUES ('__admin__',%s,%s,1,%s,%s,%s)
            ON CONFLICT (endpoint) DO UPDATE
            SET office='__admin__',
                subscription=EXCLUDED.subscription,
                is_active=1,
                updated_at=EXCLUDED.updated_at
            """,
            (endpoint, json.dumps(subscription), now, now, now),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/admin/push/unsubscribe")
async def admin_push_unsubscribe(request: Request):
    data = await request.json()
    endpoint = clean_push_endpoint(data.get("endpoint"))
    if not endpoint:
        return {"status": "error", "message": "بيانات الإشعارات غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE push_subscriptions SET is_active=0, updated_at=%s WHERE office='__admin__' AND endpoint=%s", (get_pal_time(), endpoint))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/office-pin/help")
async def request_office_pin_help(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    if not office or is_guest_office(office) or not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE office_pin_help SET status='resolved', resolved_at=%s WHERE office=%s AND status='pending'", (get_pal_time(), office))
        c.execute("INSERT INTO office_pin_help (office, status, created_at) VALUES (%s,'pending',%s)", (office, get_pal_time()))
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

@app.get("/api/menu")
async def get_menu():
    try:
        conn = get_db()
        c = conn.cursor()
        items = fetch_menu_items(c, include_hidden=False)
        c.close()
        conn.close()
        return {"items": items}
    except Exception as exc:
        return {"items": [], "status": "error", "message": str(exc)}


@app.post("/api/chat")
async def chat_with_ai(request: Request):
    data = await request.json()
    user_message = data.get("message", "")
    history = data.get("history", [])
    try:
        conn = get_db()
        c = conn.cursor()
        menu_items = fetch_menu_items(c, include_hidden=False)
        c.close()
        conn.close()
    except Exception:
        return {"reply": "تعذر تحميل المنيو حالياً. يرجى المحاولة لاحقاً.", "parsed_order": None}
    if not menu_items:
        return {"reply": "تعذر تحميل المنيو حالياً. يرجى المحاولة لاحقاً.", "parsed_order": None}

    local_order = build_local_ai_order(user_message, menu_items)

    menu_text = "\n".join([f"- {item['name']}: {item['price']} شيكل" for item in menu_items])
    system_prompt = f"""
أنت مساعد طلبات ذكي في LE Coffee. مهمتك فقط فهم طلب المستخدم من المنيو وتحويله إلى JSON.
لا تسأل عن رقم المكتب، ولا تتكلم عن الدفع، ولا تضف أصنافاً غير موجودة.
افهم العامية والأخطاء البسيطة والاختصارات، واجمع كل الأصناف والكميات من الرسالة الواحدة.
إذا ذكر المستخدم صنفاً قريباً من صنف في القائمة، اختر أقرب اسم مطابق من القائمة.
اعتمد فقط على هذه القائمة:
{menu_text}

أجب دائماً بصيغة JSON فقط وبدون أي نص خارج JSON:
{{
  "reply": "رد قصير يؤكد الأصناف والمجموع بالعربية",
  "items": [{{"name": "اسم مطابق تماماً للقائمة", "qty": 1}}],
  "total": 0
}}

إذا لم تفهم أي صنف، اجعل items فارغة واطلب توضيح اسم الصنف فقط.
""".strip()

    if not GEMINI_KEYS:
        if local_order:
            return {"reply": local_order["reply"], "parsed_order": local_order}
        return {"reply": "اكتب الطلب باسم الأصناف الموجودة في المنيو وسأرتبه لك مباشرة.", "parsed_order": None}

    try:
        genai.configure(api_key=GEMINI_KEYS[0])
        model = genai.GenerativeModel(GEMINI_MODEL, system_instruction=system_prompt)
        chat_history = []
        for msg in history[-4:]:
            role = "model" if msg.get("role") == "model" else "user"
            chat_history.append({"role": role, "parts": [msg.get("content", "")]})

        chat = model.start_chat(history=chat_history)
        response = chat.send_message(user_message)
        parsed = parse_gemini_json(getattr(response, "text", ""), menu_items)
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
    if not is_store_open():
        return store_closed_response()
    data = await request.json()
    office = clean_office_name(data.get("office"))
    items = data.get("items", [])
    total_price = int(data.get("total_price", 0) or 0)
    order_type = clean_office_name(data.get("order_type") or "داخل الكوفي كورنر")
    receipt = data.get("receipt")
    guest_phone = clean_office_name(data.get("guest_phone"))
    office_pin = str(data.get("office_pin", "")).strip()

    is_guest = is_guest_office(office)
    quick_guest_payment = is_guest and (bool(data.get("quick_payment")) or order_type == "دفع سريع للزائر")

    if not office or (not items and not quick_guest_payment):
        return {"status": "error", "message": "missing order data"}
    if quick_guest_payment and total_price <= 0:
        return {"status": "error", "message": "قيمة الدفع السريع مطلوبة"}

    if not is_guest and not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}
    if is_guest and not re.fullmatch(r"05\d{8}", guest_phone or ""):
        return {"status": "error", "message": "رقم الجوال يجب أن يبدأ بـ 05 ويتكون من 10 أرقام"}
    if is_guest and not receipt:
        return {"status": "error", "message": "فاتورة الدفع مطلوبة لطلبات الزوار"}

    if not is_guest:
        c = None
        conn = None
        try:
            conn = get_db()
            c = conn.cursor()
            c.execute("SELECT pin_hash FROM office_pins WHERE office=%s LIMIT 1", (office,))
            pin_row = c.fetchone()
            if not pin_row or not is_valid_pin(office_pin) or pin_row[0] != hash_pin(office_pin):
                c.close()
                conn.close()
                return {"status": "error", "message": "الرقم السري للمكتب غير صحيح"}
            c.close()
            conn.close()
        except Exception as exc:
            try:
                if c:
                    c.close()
                if conn:
                    conn.close()
            except Exception:
                pass
            return {"status": "error", "message": str(exc)}
    status = "بانتظار_دفع_زائر" if is_guest else "انتظار"
    is_paid = 0
    approved_at = None

    try:
        conn = get_db()
        c = conn.cursor()
        if quick_guest_payment:
            snapshot = []
            details_text = "دفع سريع للزائر"
            item_snapshot = json.dumps(snapshot, ensure_ascii=False)
            order_type = "دفع سريع للزائر"
            total_price = int(total_price or 0)
        else:
            menu_by_name = get_menu_by_name(c)
            snapshot, missing_item = build_order_snapshot(items, menu_by_name)
            if missing_item or not snapshot:
                c.close()
                conn.close()
                return {"status": "error", "message": f"الصنف غير متاح في المنيو: {missing_item or ''}"}
            total_price = sum(int(item.get("price", 0) or 0) for item in snapshot)
            details_text = ", ".join(item["name"] for item in snapshot)
            item_snapshot = json.dumps(snapshot, ensure_ascii=False)
            if any(item.get("cat") == "snack" for item in snapshot):
                order_type = "داخل الكوفي كورنر"
        if not is_guest:
            office_variants = office_location_variants(office)
            office_number = office_number_value(office)
            c.execute(
                """
                SELECT id
                FROM orders
                WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
                  AND status IN ('انتظار','صنف_ناقص')
                LIMIT 1
                """,
                (office_variants, office_number),
            )
            if c.fetchone():
                c.close()
                conn.close()
                return {"status": "error", "message": "لديك طلب قيد الانتظار"}
        if is_guest:
            c.execute(
                """
                SELECT timestamp
                FROM orders
                WHERE location LIKE 'زائر%%'
                  AND guest_phone=%s
                  AND details=%s
                  AND total_price=%s
                  AND status IN ('بانتظار_دفع_زائر', 'فاتورة_زائر_مرفوضة')
                ORDER BY id DESC
                LIMIT 1
                """,
                (guest_phone, details_text, total_price),
            )
            duplicate_row = c.fetchone()
            duplicate_time = parse_time(duplicate_row[0]) if duplicate_row else None
            if duplicate_time and get_pal_datetime() - duplicate_time <= timedelta(minutes=3):
                c.close()
                conn.close()
                return {"status": "error", "message": "تم إرسال هذا الطلب مسبقاً. انتظر مراجعة الكاشير."}
        c.execute(
            """
            INSERT INTO orders
            (user_id, details, total_price, location, timestamp, status, is_paid, receipt, order_type, approved_at, guest_phone, item_snapshot)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (0, details_text, total_price, office, get_pal_time(), status, is_paid, receipt, order_type, approved_at, guest_phone, item_snapshot),
        )
        new_order_id = c.fetchone()[0]
        if not is_guest:
            send_push_notification(
                c,
                "__admin__",
                "طلب جديد",
                f"{office} - {details_text}",
                tag=push_safe_tag(f"admin-order-{new_order_id}"),
                url="/admin",
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
    if not is_store_open():
        return store_closed_response()
    data = await request.json()
    office = clean_office_name(data.get("office"))
    items = data.get("items", [])
    total_price = int(data.get("total_price", 0) or 0)
    order_type = clean_office_name(data.get("order_type") or "داخل الكوفي كورنر")
    office_pin = str(data.get("office_pin", "")).strip()

    if not office or not items:
        return {"status": "error", "message": "missing order data"}
    if not is_valid_office_number(office):
        return {"status": "error", "message": "رقم المكتب يجب أن يكون 3 أرقام ويبدأ بـ 2 أو 4"}

    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT pin_hash FROM office_pins WHERE office=%s LIMIT 1", (office,))
        pin_row = c.fetchone()
        if not pin_row or not is_valid_pin(office_pin) or pin_row[0] != hash_pin(office_pin):
            c.close()
            conn.close()
            return {"status": "error", "message": "الرقم السري للمكتب غير صحيح"}
        menu_by_name = get_menu_by_name(c)
        snapshot, missing_item = build_order_snapshot(items, menu_by_name)
        if missing_item or not snapshot:
            c.close()
            conn.close()
            return {"status": "error", "message": f"الصنف غير متاح في المنيو: {missing_item or ''}"}
        total_price = sum(int(item.get("price", 0) or 0) for item in snapshot)
        details_text = ", ".join(item["name"] for item in snapshot)
        item_snapshot = json.dumps(snapshot, ensure_ascii=False)
        if any(item.get("cat") == "snack" for item in snapshot):
            order_type = "داخل الكوفي كورنر"
        c.execute(
            """
            UPDATE orders
            SET details=%s, total_price=%s, order_type=%s, item_snapshot=%s, missing_note=NULL, status='انتظار'
            WHERE id=%s AND location=%s AND status IN ('انتظار','صنف_ناقص')
            """,
            (details_text, total_price, order_type, item_snapshot, order_id, office),
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
            SELECT 1
            FROM debt_payment_requests
            WHERE (office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s)
              AND status='pending'
            LIMIT 1
            """,
            (office_location_variants(office), office_number_value(office)),
        )
        if c.fetchone():
            c.close()
            conn.close()
            return {"status": "error", "message": "يوجد طلب تسديد قيد المراجعة لهذا المكتب"}

        c.execute(
            """
            INSERT INTO debt_payment_requests (office, amount, receipt, status, created_at, reminder_id)
            VALUES (%s,%s,%s,'pending',%s,%s)
            RETURNING id
            """,
            (office, amount, receipt, get_pal_time(), reminder["id"]),
        )
        payment_request_id = c.fetchone()[0]
        send_push_notification(
            c,
            "__admin__",
            "طلب سداد دين جديد",
            f"{office} أرسل إثبات سداد بقيمة {amount} ₪",
            tag=push_safe_tag(f"admin-debt-payment-{payment_request_id}"),
            url="/admin",
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
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    try:
        conn = get_db()
        c = conn.cursor()

        active_order = None
        if not guest:
            c.execute(
                """
                SELECT id, details, total_price, status, missing_note, order_type
                FROM orders
                WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
                  AND status IN ('انتظار','صنف_ناقص')
                ORDER BY id DESC
                LIMIT 1
                """,
                (office_variants, office_number),
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

        history_since = (get_pal_datetime() - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT id, details, total_price, timestamp, is_paid, status, receipt, order_type
            FROM orders
            WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
                          AND status NOT IN ('انتظار','صنف_ناقص','ملغي')
                          AND COALESCE(details, '') <> 'تم حذف جميع الأصناف من هذا الطلب'
                          AND COALESCE(approved_at, timestamp) >= %s
            ORDER BY id DESC
            """,
            (office_variants, office_number, history_since),
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
        new_office_pin = None
        if not guest:
            c.execute(
                """
                SELECT id, new_pin
                FROM office_pin_help
                WHERE (office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s)
                  AND new_pin IS NOT NULL AND COALESCE(pin_seen, 0)=0
                ORDER BY id DESC
                LIMIT 1
                """,
                (office_variants, office_number),
            )
            pin_row = c.fetchone()
            if pin_row:
                new_office_pin = {"id": pin_row[0], "pin": pin_row[1]}

        review_due = None
        if not guest:
            c.execute(
                """
                SELECT id, details, total_price, approved_at
                FROM orders
                WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
                  AND status='مقبول'
                  AND is_reviewed=0
                  AND total_price > 16
                  AND COALESCE(order_type, 'داخل الكوفي كورنر') IN ('داخل الكوفي كورنر', 'توصيل للمكتب')
                  AND approved_at IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (office_variants, office_number),
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

        rewards = None
        if not guest:
            try:
                rewards = fetch_reward_progress(c, office)
            except Exception:
                rewards = empty_reward_progress()
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
            "new_office_pin": new_office_pin,
            "review_due": review_due,
            "rewards": rewards,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/office-pin/new-pin-seen")
async def mark_new_office_pin_seen(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    pin_help_id = data.get("id")
    if not office or not pin_help_id:
        return {"status": "error", "message": "missing data"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            "UPDATE office_pin_help SET pin_seen=1 WHERE id=%s AND office=%s",
            (pin_help_id, office),
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/rewards/claim")
async def claim_reward(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    reward_key = clean_office_name(data.get("reward_key"))
    if not office or is_guest_office(office) or not reward_key:
        return {"status": "error", "message": "بيانات الهدية غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        progress = fetch_reward_progress(c, office)
        tier = next((item for item in REWARD_TIERS if item["key"] == reward_key), None)
        tier_progress = next((item for item in progress["tiers"] if item["key"] == reward_key), None)
        if not tier or not tier_progress or not tier_progress["eligible"]:
            c.close()
            conn.close()
            return {"status": "error", "message": "هذه الهدية غير مستحقة حالياً"}
        if tier_progress.get("reward"):
            reward = tier_progress["reward"]
            c.close()
            conn.close()
            return {"status": "success", "reward": reward, "already_claimed": True}
        prize = select_reward_item(c, tier["prize_max"])
        if not prize:
            c.close()
            conn.close()
            return {"status": "error", "message": "لا توجد هدية متاحة حالياً"}
        item_name, item_price = prize
        now = get_pal_time()
        c.execute(
            """
            INSERT INTO office_rewards (office, week_start, reward_key, item_name, item_price, status, created_at, claimed_at)
            VALUES (%s,%s,%s,%s,%s,'claimed',%s,%s)
            RETURNING id
            """,
            (office, progress["week_start"], reward_key, item_name, int(item_price or 0), now, now),
        )
        reward_id = c.fetchone()[0]
        conn.commit()
        reward = {"id": reward_id, "reward_key": reward_key, "item_name": item_name, "item_price": int(item_price or 0), "status": "claimed", "order_id": None}
        c.close()
        conn.close()
        return {"status": "success", "reward": reward}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/rewards/redeem")
async def redeem_reward(request: Request):
    data = await request.json()
    office = clean_office_name(data.get("office"))
    reward_id = data.get("reward_id")
    if not office or is_guest_office(office) or not reward_id:
        return {"status": "error", "message": "بيانات الهدية غير مكتملة"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT id
            FROM orders
            WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
              AND status IN ('انتظار','صنف_ناقص')
            ORDER BY id DESC
            LIMIT 1
            """,
            (office_location_variants(office), office_number_value(office)),
        )
        if c.fetchone():
            c.close()
            conn.close()
            return {"status": "error", "message": "لديك طلب قيد الانتظار حالياً"}
        c.execute(
            """
            SELECT item_name, item_price, status, week_start
            FROM office_rewards
            WHERE id=%s AND office=%s
            """,
            (reward_id, office),
        )
        row = c.fetchone()
        if not row:
            c.close()
            conn.close()
            return {"status": "error", "message": "الهدية غير موجودة"}
        item_name, item_price, reward_status, reward_week_start = row
        if str(reward_week_start or "") != get_reward_week_key():
            c.close()
            conn.close()
            return {"status": "error", "message": "انتهت صلاحية هذه الهدية الأسبوعية"}
        if reward_status == "ordered":
            c.close()
            conn.close()
            return {"status": "success", "message": "تم إرسال الهدية للكاشير سابقاً"}
        if reward_status != "claimed":
            c.close()
            conn.close()
            return {"status": "error", "message": "لا يمكن استلام هذه الهدية حالياً"}
        now = get_pal_time()
        snapshot = json.dumps([{"name": item_name, "price": int(item_price or 0), "cat": "gift"}], ensure_ascii=False)
        c.execute(
            """
            INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, item_snapshot)
            VALUES (%s,%s,0,%s,%s,'انتظار',1,'هدية مجانية',%s)
            RETURNING id
            """,
            (0, f"هدية مجانية: {item_name}", office, now, snapshot),
        )
        order_id = c.fetchone()[0]
        c.execute(
            "UPDATE office_rewards SET status='ordered', order_id=%s, ordered_at=%s WHERE id=%s",
            (order_id, now, reward_id),
        )
        send_push_notification(
            c,
            "__admin__",
            "هدية جديدة للكاشير",
            f"{office} طلب استلام هدية: {item_name}",
            tag=push_safe_tag(f"admin-reward-{order_id}"),
            url="/admin",
        )
        conn.commit()
        c.close()
        conn.close()
        return {"status": "success", "order_id": order_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/review")
async def submit_review(request: Request):
    data = await request.json()
    order_id = data.get("order_id")
    skipped = bool(data.get("skipped"))
    rating = 0 if skipped else data.get("rating", 5)
    text = "" if skipped else data.get("text", "")
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


@app.get("/api/admin/receipt/{source}/{item_id}")
async def admin_receipt(source: str, item_id: int):
    table_map = {
        "order": "orders",
        "debt_payment": "debt_payment_requests",
        "expense": "expenses",
    }
    table = table_map.get(source)
    if not table:
        return {"status": "error", "message": "invalid receipt source"}
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(f"SELECT receipt FROM {table} WHERE id=%s", (item_id,))
        row = c.fetchone()
        c.close()
        conn.close()
        if not row or not row[0]:
            return {"status": "error", "message": "receipt not found"}
        return {"status": "success", "receipt": row[0]}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

@app.get("/api/admin/dashboard")
async def admin_dashboard():
    try:
        conn = get_db()
        c = conn.cursor()

        c.execute(
            """
            SELECT id, details, total_price, location, status, order_type, missing_note, timestamp
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
                "timestamp": row[7],
                "kind": "order",
            }
            for row in active_rows
        ]

        c.execute(
            """
            SELECT id, office, amount, (receipt IS NOT NULL AND receipt <> '') AS has_receipt, status, created_at, payment_method
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
                    "has_receipt": bool(row[3]),
                    "timestamp": row[5],
                    "kind": "debt_payment",
                    "payment_method": row[6],
                }
            )

        c.execute(
            """
            SELECT location, SUM(total_price), MAX(COALESCE(approved_at, timestamp)) AS last_debt_at
            FROM orders
            WHERE status='مقبول' AND is_paid=0 AND location NOT LIKE 'زائر%%'
            GROUP BY location
            ORDER BY last_debt_at DESC, location ASC
            """
        )
        debt_rows = c.fetchall()
        debts = []
        total_debts = 0
        for office, amount, last_debt_at in debt_rows:
            amount = amount or 0
            if amount <= 0:
                continue
            total_debts += amount
            reminder = get_active_reminder(c, office)
            debts.append(
                {
                    "office": office,
                    "amount": amount,
                    "status": "غير مدفوع",
                    "has_active_reminder": bool(reminder),
                    "reminder_id": reminder["id"] if reminder else None,
                    "reminder_amount": reminder["amount"] if reminder else 0,
                    "reminder_created_at": reminder["created_at"] if reminder else None,
                    "last_debt_at": last_debt_at,
                }
            )

        c.execute(
            "SELECT COUNT(*) FROM orders WHERE status='مقبول' AND COALESCE(order_type, '') NOT IN %s",
            (ACCOUNTING_EXCLUDED_ORDER_TYPES,),
        )
        total_count = c.fetchone()[0] or 0

        c.execute(
            "SELECT COALESCE(SUM(total_price), 0) FROM orders WHERE status='مقبول' AND COALESCE(order_type, '') NOT IN %s",
            (SALES_EXCLUDED_ORDER_TYPES,),
        )
        total_sales = c.fetchone()[0] or 0
        paid_invoices = total_sales - total_debts
        c.execute("SELECT COALESCE(SUM(amount), 0) FROM expenses")
        total_expenses = c.fetchone()[0] or 0
        total_profit = total_sales - total_expenses
        last_7_days = (get_pal_datetime() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

        c.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول'
              AND COALESCE(order_type, '') NOT IN %s
              AND COALESCE(approved_at, timestamp) >= %s
            """,
            (ACCOUNTING_EXCLUDED_ORDER_TYPES, last_7_days),
        )
        last_7_count, _ = c.fetchone()
        last_7_count = int(last_7_count or 0)

        c.execute(
            """
            SELECT COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول'
              AND COALESCE(order_type, '') NOT IN %s
              AND COALESCE(approved_at, timestamp) >= %s
            """,
            (SALES_EXCLUDED_ORDER_TYPES, last_7_days),
        )
        last_7_sales = int(c.fetchone()[0] or 0)

        c.execute(
            """
            SELECT COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول'
              AND is_paid=0
              AND location NOT LIKE 'زائر%%'
              AND COALESCE(approved_at, timestamp) >= %s
            """,
            (last_7_days,),
        )
        last_7_debts = int(c.fetchone()[0] or 0)
        last_7_paid = last_7_sales - last_7_debts

        c.execute("SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE created_at >= %s", (last_7_days,))
        last_7_expenses = int(c.fetchone()[0] or 0)
        last_7_profit = last_7_sales - last_7_expenses

        c.execute(
            """
            SELECT timestamp, approved_at
            FROM orders
            WHERE timestamp IS NOT NULL
              AND approved_at IS NOT NULL
              AND status IN ('مقبول','صنف_ناقص','فاتورة_زائر_مرفوضة')
              AND COALESCE(order_type, '') NOT IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'رفض سداد الدين', 'سداد دين', 'سداد يدوي', 'هدية مجانية')
            ORDER BY id DESC
            LIMIT 100
            """
        )
        response_minutes = []
        for started_at, handled_at in c.fetchall():
            started = parse_time(started_at)
            handled = parse_time(handled_at)
            if started and handled and handled >= started:
                response_minutes.append((handled - started).total_seconds() / 60)
        avg_response_minutes = round(sum(response_minutes) / len(response_minutes), 1) if response_minutes else 0
        week_key = get_reward_week_key()
        c.execute(
            """
            SELECT timestamp, approved_at
            FROM orders
            WHERE timestamp IS NOT NULL
              AND approved_at IS NOT NULL
              AND status IN ('مقبول','صنف_ناقص','فاتورة_زائر_مرفوضة')
              AND COALESCE(order_type, '') NOT IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'رفض سداد الدين', 'سداد دين', 'سداد يدوي', 'هدية مجانية')
              AND COALESCE(approved_at, timestamp) >= %s
            """,
            (week_key,),
        )
        week_response_minutes = []
        for started_at, handled_at in c.fetchall():
            started = parse_time(started_at)
            handled = parse_time(handled_at)
            if started and handled and handled >= started:
                week_response_minutes.append((handled - started).total_seconds() / 60)
        week_avg_response_minutes = round(sum(week_response_minutes) / len(week_response_minutes), 1) if week_response_minutes else 0
        response_week_delta_percent = 0
        if avg_response_minutes and week_avg_response_minutes:
            response_week_delta_percent = round(((week_avg_response_minutes - avg_response_minutes) / avg_response_minutes) * 100, 1)
        if not response_minutes:
            response_level = "none"
        elif avg_response_minutes < 5:
            response_level = "good"
        elif avg_response_minutes < 10:
            response_level = "warn"
        else:
            response_level = "bad"

        c.execute("SELECT COALESCE(AVG(rating), 0), COUNT(*) FROM orders WHERE is_reviewed=1 AND rating > 0")
        rating_avg, rating_count = c.fetchone()
        rating_avg = round(float(rating_avg or 0), 1)
        rating_count = int(rating_count or 0)

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
            SELECT id, details, total_price, location, timestamp, (receipt IS NOT NULL AND receipt <> '') AS has_receipt, guest_phone, status, is_paid, missing_note, order_type, payment_method
            FROM orders
            WHERE location LIKE 'زائر%%'
              AND status NOT IN ('ملغي','مقبول','فاتورة_زائر_مرفوضة')
            ORDER BY id DESC
            LIMIT 50
            """
        )
        guest_orders = [
            {
                "id": row[0],
                "details": row[1],
                "total_price": row[2],
                "location": row[3],
                "timestamp": row[4],
                "has_receipt": bool(row[5]),
                "guest_phone": row[6],
                "status": row[7],
                "is_paid": row[8],
                "rejection_note": row[9],
                "order_type": row[10],
                "payment_method": row[11],
            }
            for row in c.fetchall()
        ]
        c.execute(
            """
            SELECT 'debt_payment' AS source, id, office AS payer, amount, status, created_at,
                   (receipt IS NOT NULL AND receipt <> '') AS has_receipt, payment_method, NULL::TEXT AS note
            FROM debt_payment_requests
            WHERE status <> 'pending' AND COALESCE(archive_hidden, 0)=0
            UNION ALL
            SELECT 'guest_order' AS source, id, location AS payer, total_price AS amount, status, COALESCE(approved_at, timestamp) AS created_at,
                   (receipt IS NOT NULL AND receipt <> '') AS has_receipt, payment_method, missing_note AS note
            FROM orders
            WHERE location LIKE 'زائر%%' AND status IN ('مقبول','فاتورة_زائر_مرفوضة') AND COALESCE(archive_hidden, 0)=0
            UNION ALL
            SELECT 'manual_debt_payment' AS source, id, location AS payer, ABS(total_price) AS amount, status, COALESCE(approved_at, timestamp) AS created_at,
                   FALSE AS has_receipt, payment_method, details AS note
            FROM orders
            WHERE location NOT LIKE 'زائر%%'
              AND status='مقبول'
              AND order_type='سداد دين'
              AND details LIKE 'سداد دين:%%'
              AND COALESCE(archive_hidden, 0)=0
            ORDER BY created_at DESC NULLS LAST
            """
        )
        payment_archive = [
            {
                "source": row[0],
                "id": row[1],
                "payer": row[2],
                "amount": row[3],
                "status": row[4],
                "created_at": row[5],
                "has_receipt": bool(row[6]),
                "payment_method": row[7],
                "note": row[8],
            }
            for row in c.fetchall()
        ]
        c.execute(
            """
            SELECT payment_method, COALESCE(SUM(amount), 0)
            FROM (
                SELECT payment_method, amount
                FROM debt_payment_requests
                WHERE status='paid' AND COALESCE(archive_hidden, 0)=0
                UNION ALL
                SELECT payment_method, total_price AS amount
                FROM orders
                WHERE location LIKE 'زائر%%' AND status='مقبول' AND COALESCE(archive_hidden, 0)=0
                UNION ALL
                SELECT payment_method, ABS(total_price) AS amount
                FROM orders
                WHERE location NOT LIKE 'زائر%%'
                  AND status='مقبول'
                  AND order_type='سداد دين'
                  AND details LIKE 'سداد دين:%%'
                  AND COALESCE(archive_hidden, 0)=0
            ) reviewed_payments
            WHERE payment_method IN ('wallet','bank')
            GROUP BY payment_method
            """
        )
        archive_totals = {row[0]: int(row[1] or 0) for row in c.fetchall()}
        payment_archive_stats = {
            "wallet_total": archive_totals.get("wallet", 0),
            "bank_total": archive_totals.get("bank", 0),
        }
        c.execute("SELECT id, amount, (receipt IS NOT NULL AND receipt <> '') AS has_receipt, created_at, description FROM expenses ORDER BY id DESC")
        expenses = [
            {"id": row[0], "amount": row[1], "has_receipt": bool(row[2]), "created_at": row[3], "description": row[4]}
            for row in c.fetchall()
        ]

        c.execute(
            """
            SELECT office, MAX(updated_at)
            FROM office_pins
            GROUP BY office
            ORDER BY office ASC
            """
        )
        offices = [
            {"office": row[0], "has_pin": True, "updated_at": row[1], "help_requested": False, "help_created_at": None, "accepted_orders_count": 0, "total_purchases": 0, "weekly_orders_count": 0, "weekly_purchases": 0}
            for row in c.fetchall()
        ]
        office_map = {item["office"]: item for item in offices}

        c.execute(
            """
            SELECT DISTINCT location
            FROM orders
            WHERE location NOT LIKE 'زائر%%'
            ORDER BY location ASC
            """
        )
        for row in c.fetchall():
            if row[0] not in office_map:
                office_map[row[0]] = {"office": row[0], "has_pin": False, "updated_at": None, "help_requested": False, "help_created_at": None, "accepted_orders_count": 0, "total_purchases": 0, "weekly_orders_count": 0, "weekly_purchases": 0}

        c.execute("SELECT office, created_at FROM office_pin_help WHERE status='pending' ORDER BY id DESC")
        for office_name, created_at in c.fetchall():
            if office_name not in office_map:
                office_map[office_name] = {"office": office_name, "has_pin": False, "updated_at": None, "help_requested": True, "help_created_at": created_at, "accepted_orders_count": 0, "total_purchases": 0, "weekly_orders_count": 0, "weekly_purchases": 0}
            else:
                office_map[office_name]["help_requested"] = True
                office_map[office_name]["help_created_at"] = created_at

        c.execute(
            """
            SELECT location, COUNT(*), COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول'
              AND location NOT LIKE 'زائر%%'
              AND COALESCE(order_type, '') NOT IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'رفض سداد الدين', 'سداد دين', 'سداد يدوي', 'هدية مجانية')
            GROUP BY location
            """
        )
        for office_name, order_count, total_purchase in c.fetchall():
            if office_name not in office_map:
                office_map[office_name] = {"office": office_name, "has_pin": False, "updated_at": None, "help_requested": False, "help_created_at": None, "accepted_orders_count": 0, "total_purchases": 0, "weekly_orders_count": 0, "weekly_purchases": 0}
            office_map[office_name]["accepted_orders_count"] = int(order_count or 0)
            office_map[office_name]["total_purchases"] = int(total_purchase or 0)

        c.execute(
            """
            SELECT location, COUNT(*), COALESCE(SUM(total_price), 0)
            FROM orders
            WHERE status='مقبول'
              AND location NOT LIKE 'زائر%%'
              AND COALESCE(order_type, '') NOT IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'رفض سداد الدين', 'سداد دين', 'سداد يدوي', 'هدية مجانية')
              AND COALESCE(approved_at, timestamp) >= %s
            GROUP BY location
            """,
            (week_key,),
        )
        for office_name, weekly_order_count, weekly_purchase in c.fetchall():
            if office_name not in office_map:
                office_map[office_name] = {"office": office_name, "has_pin": False, "updated_at": None, "help_requested": False, "help_created_at": None, "accepted_orders_count": 0, "total_purchases": 0, "weekly_orders_count": 0, "weekly_purchases": 0}
            office_map[office_name]["weekly_orders_count"] = int(weekly_order_count or 0)
            office_map[office_name]["weekly_purchases"] = int(weekly_purchase or 0)
        offices = sorted(office_map.values(), key=lambda item: item["office"])

        menu_items = fetch_menu_items(c, include_hidden=True)
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
                "avg_response_minutes": avg_response_minutes,
                "week_avg_response_minutes": week_avg_response_minutes,
                "response_week_delta_percent": response_week_delta_percent,
                "response_count": len(response_minutes),
                "week_response_count": len(week_response_minutes),
                "response_level": response_level,
                "rating_average": rating_avg,
                "rating_count": rating_count,
                "last_7_days": {
                    "total_sales": last_7_sales,
                    "total_count": last_7_count,
                    "paid_invoices": last_7_paid,
                    "total_debts": last_7_debts,
                    "total_profit": last_7_profit,
                },
            },
            "active_orders": active_orders,
            "debts": debts,
            "reviews": reviews,
            "guest_orders": guest_orders,
            "payment_archive": payment_archive,
            "payment_archive_stats": payment_archive_stats,
            "expenses": expenses,
            "offices": offices,
        "menu_items": menu_items,
        }
    except Exception as exc:
        return {"error": str(exc)}



@app.get("/api/admin/debt-details/{office}")
async def admin_debt_details(office: str):
    office = clean_office_name(office)
    office_variants = office_location_variants(office)
    office_number = office_number_value(office)
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(
            """
            SELECT id, details, total_price, timestamp, order_type, item_snapshot
            FROM orders
            WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
                          AND status='مقبول' AND is_paid=0
                          AND COALESCE(order_type, '') NOT IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'سداد دين', 'سداد يدوي')
            ORDER BY id DESC
            """,
            (office_variants, office_number),
        )
        rows = c.fetchall()
        orders = []
        for row in rows:
            snapshot = parse_item_snapshot(row[5] if len(row) > 5 else None)
            if snapshot:
                item_details = [{"name": item.get("name"), "price": int(item.get("price", 0) or 0)} for item in snapshot]
                items = [item["name"] for item in item_details if item.get("name")]
            else:
                items = [] if (row[1] or "").strip() == "تم حذف جميع الأصناف من هذا الطلب" else [item.strip() for item in (row[1] or "").split(",") if item.strip()]
                item_details = [{"name": item, "price": int(PRICES.get(item, 0) or 0)} for item in items]
            orders.append({"id": row[0], "details": row[1], "items": items, "item_details": item_details, "total_price": row[2], "timestamp": row[3], "order_type": row[4]})
        c.execute(
            """
            SELECT id, details, total_price, timestamp, order_type
            FROM orders
            WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
              AND status='مقبول' AND is_paid=0
              AND COALESCE(order_type, '') IN ('تسوية دين يدوية', 'إضافة يدوية', 'حذف صنف من الدين', 'سداد دين', 'سداد يدوي')
            ORDER BY id DESC
            """,
            (office_variants, office_number),
        )
        adjustment_rows = c.fetchall()
        adjustments = []
        for row in adjustment_rows:
            amount = int(row[2] or 0)
            raw_details = row[1] or ""
            note = raw_details.replace("تسوية دين:", "", 1).strip() or raw_details
            adjustments.append({
                "id": row[0],
                "details": raw_details,
                "note": note,
                "amount": amount,
                "timestamp": row[3],
                "order_type": row[4],
                "kind": "discount" if amount < 0 else "addition",
            })
        current_debt_ids = {item["id"] for item in orders + adjustments}
        c.execute(
            """
            SELECT id, details, total_price, timestamp, status, order_type, is_paid
            FROM orders
            WHERE (location IN %s OR regexp_replace(COALESCE(location, ''), '[^0-9]', '', 'g')=%s)
              AND status NOT IN ('انتظار','صنف_ناقص','ملغي')
              AND COALESCE(details, '') <> 'تم حذف جميع الأصناف من هذا الطلب'
            ORDER BY id DESC
            """,
            (office_variants, office_number),
        )
        archive = [
            {
                "id": row[0],
                "details": row[1],
                "total_price": row[2],
                "timestamp": row[3],
                "status": row[4],
                "order_type": row[5],
                "is_paid": row[6],
            }
            for row in c.fetchall()
            if row[0] not in current_debt_ids
        ]
        total_debt = fetch_current_debt(c, office)
        c.close()
        conn.close()
        return {"status": "success", "office": office, "orders": orders, "adjustments": adjustments, "archive": archive, "total_debt": total_debt}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.post("/api/admin/action")
async def admin_action(request: Request):
    data = await request.json()
    action = data.get("action")
    new_pin = None
    order_id = data.get("order_id")
    office = clean_office_name(data.get("office"))
    payment_method = data.get("payment_method")
    if payment_method not in ("wallet", "bank"):
        payment_method = None
    try:
        conn = get_db()
        c = conn.cursor()

        if action == "approve":
            now = get_pal_time()
            c.execute("SELECT location, details, order_type FROM orders WHERE id=%s", (order_id,))
            approve_row = c.fetchone()
            c.execute("UPDATE orders SET status='مقبول', approved_at=%s WHERE id=%s", (now, order_id))
            if approve_row and (approve_row[2] or '') == 'هدية مجانية':
                gift_office = approve_row[0]
                c.execute("SELECT id, item_name, item_price FROM office_rewards WHERE order_id=%s", (order_id,))
                gift_row = c.fetchone()
                if gift_row:
                    reward_id, item_name, item_price = gift_row
                    item_price = int(item_price or 0)
                    c.execute("UPDATE office_rewards SET status='approved', approved_at=%s WHERE id=%s", (now, reward_id))
                    if item_price > 0:
                        c.execute(
                            "INSERT INTO expenses (amount, receipt, created_at, description) VALUES (%s,%s,%s,%s)",
                            (item_price, None, now, f"هدية مجانية للمكتب {gift_office}: {item_name}"),
                        )
            elif approve_row and approve_row[0] and not is_guest_office(approve_row[0]):
                send_reward_ready_notifications(c, approve_row[0])
        elif action == "missing":
            c.execute("SELECT location FROM orders WHERE id=%s", (order_id,))
            missing_row = c.fetchone()
            c.execute("UPDATE orders SET status='صنف_ناقص', missing_note=%s, approved_at=%s WHERE id=%s", (data.get("note"), get_pal_time(), order_id))
            if missing_row and missing_row[0] and not is_guest_office(missing_row[0]):
                send_push_notification(
                    c,
                    missing_row[0],
                    "يوجد صنف ناقص في طلبك",
                    "راجع الطلب الآن من الموقع.",
                    tag=push_safe_tag(f"missing-{order_id}"),
                    url="/",
                )
        elif action == "confirm_visitor_payment":
            if not payment_method:
                c.close()
                conn.close()
                return {"status": "error", "message": "اختر طريقة حفظ التحويل"}
            c.execute("UPDATE orders SET status='مقبول', is_paid=1, approved_at=%s, missing_note=NULL, payment_method=%s WHERE id=%s AND location LIKE 'زائر%%'", (get_pal_time(), payment_method, order_id))
        elif action == "reject_visitor_payment":
            note = clean_office_name(data.get("note"))
            if not note:
                c.close()
                conn.close()
                return {"status": "error", "message": "سبب الرفض مطلوب"}
            c.execute("UPDATE orders SET status='فاتورة_زائر_مرفوضة', is_paid=0, missing_note=%s, approved_at=%s WHERE id=%s AND location LIKE 'زائر%%'", (note, get_pal_time(), order_id))
        elif action == "remind":
            if not office:
                c.close()
                conn.close()
                return {"status": "error", "message": "office is required"}
            active_reminder = get_active_reminder(c, office)
            if active_reminder:
                c.close()
                conn.close()
                return {"status": "error", "message": "يوجد تذكير دفع فعال لهذا المكتب"}
            amount = fetch_current_debt(c, office)
            if amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "لا يوجد دين حالي لهذا المكتب"}
            c.execute(
                """
                INSERT INTO reminders (office, amount, payment_info, is_active, is_seen, created_at)
                VALUES (%s,%s,%s,1,0,%s)
                """,
                (office, amount, DEBT_PAYMENT_INFO, get_pal_time()),
            )
            send_push_notification(
                c,
                office,
                "لديك طلب سداد دين",
                "راجع صفحة ديوني لإرسال إثبات السداد.",
                tag=push_safe_tag(f"debt-reminder-{office}-{get_pal_time()}"),
                url="/",
            )
        elif action == "mark_paid":
            c.close()
            conn.close()
            return {"status": "error", "message": "تم إيقاف السداد اليدوي. استخدم طلب سداد الدين أو تعديل الدين فقط."}
        elif action == "confirm_debt_payment":
            if not payment_method:
                c.close()
                conn.close()
                return {"status": "error", "message": "اختر طريقة حفظ التحويل"}
            c.execute(
                "SELECT office, amount FROM debt_payment_requests WHERE id=%s AND status='pending'",
                (order_id,),
            )
            payment_row = c.fetchone()
            if not payment_row:
                c.close()
                conn.close()
                return {"status": "error", "message": "payment request not found"}
            pay_office, pay_amount = payment_row
            pay_amount = int(pay_amount or 0)
            if pay_amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "invalid payment amount"}
            c.execute("UPDATE debt_payment_requests SET status='paid', payment_method=%s WHERE id=%s", (payment_method, order_id))
            c.execute(
                """
                INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, approved_at)
                VALUES (%s,%s,%s,%s,%s,'مقبول',0,'سداد دين',%s)
                """,
                (0, "سداد دين بناء على تذكير الكاشير", -pay_amount, pay_office, get_pal_time(), get_pal_time()),
            )
            c.execute(
                "UPDATE reminders SET is_active=0 WHERE office IN %s OR regexp_replace(COALESCE(office, ''), '[^0-9]', '', 'g')=%s",
                (office_location_variants(pay_office), office_number_value(pay_office)),
            )
        elif action == "update_payment_method":
            source = data.get("source")
            if not payment_method:
                c.close()
                conn.close()
                return {"status": "error", "message": "اختر طريقة حفظ التحويل"}
            if source == "debt_payment":
                c.execute("UPDATE debt_payment_requests SET payment_method=%s WHERE id=%s AND status='paid'", (payment_method, order_id))
            elif source == "guest_order":
                c.execute("UPDATE orders SET payment_method=%s WHERE id=%s AND location LIKE 'زائر%%' AND status='مقبول'", (payment_method, order_id))
            elif source == "manual_debt_payment":
                c.execute("UPDATE orders SET payment_method=%s WHERE id=%s AND location NOT LIKE 'زائر%%' AND status='مقبول' AND order_type='سداد دين' AND details LIKE 'سداد دين:%%'", (payment_method, order_id))
            else:
                c.close()
                conn.close()
                return {"status": "error", "message": "مصدر التحويل غير معروف"}
        elif action == "clear_payment_archive":
            c.execute(
                """
                UPDATE debt_payment_requests
                SET receipt=NULL, payment_method=NULL
                WHERE status <> 'pending'
                """
            )
            c.execute(
                """
                UPDATE orders
                SET receipt=NULL, payment_method=NULL
                WHERE (
                    (location LIKE 'زائر%%' AND status IN ('مقبول','فاتورة_زائر_مرفوضة'))
                    OR (
                      location NOT LIKE 'زائر%%'
                      AND status='مقبول'
                      AND order_type='سداد دين'
                      AND details LIKE 'سداد دين:%%'
                    )
                  )
                """
            )
        elif action == "reject_debt_payment":
            note = clean_office_name(data.get("note"))
            if not note:
                c.close()
                conn.close()
                return {"status": "error", "message": "سبب الرفض مطلوب"}
            c.execute(
                "SELECT office, amount FROM debt_payment_requests WHERE id=%s AND status='pending'",
                (order_id,),
            )
            payment_row = c.fetchone()
            if not payment_row:
                c.close()
                conn.close()
                return {"status": "error", "message": "payment request not found"}
            pay_office, pay_amount = payment_row
            c.execute("UPDATE debt_payment_requests SET status='rejected' WHERE id=%s", (order_id,))
            c.execute(
                """
                INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, approved_at)
                VALUES (%s,%s,0,%s,%s,'مقبول',1,'رفض سداد الدين',%s)
                """,
                (0, f"رفض سداد الدين: {note}", pay_office, get_pal_time(), get_pal_time()),
            )
        elif action == "resolve_office_pin_help":
            if not office:
                c.close()
                conn.close()
                return {"status": "error", "message": "office is required"}
            c.execute(
                "UPDATE office_pin_help SET status='resolved', resolved_at=%s WHERE office=%s AND status='pending'",
                (get_pal_time(), office),
            )
        elif action == "reset_office_pin":
            if not office:
                c.close()
                conn.close()
                return {"status": "error", "message": "office is required"}
            deactivate_push_subscriptions(c, office)
            new_pin = generate_unique_pin(c)
            c.execute(
                """
                INSERT INTO office_pins (office, pin_hash, updated_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (office) DO UPDATE SET pin_hash=EXCLUDED.pin_hash, updated_at=EXCLUDED.updated_at
                """,
                (office, hash_pin(new_pin), get_pal_time()),
            )
            c.execute(
                """
                UPDATE office_pin_help
                SET status='resolved', resolved_at=%s, new_pin=%s, pin_seen=0
                WHERE office=%s AND status='pending'
                """,
                (get_pal_time(), new_pin, office),
            )
            if c.rowcount == 0:
                c.execute(
                    "INSERT INTO office_pin_help (office, status, created_at, resolved_at, new_pin, pin_seen) VALUES (%s,'resolved',%s,%s,%s,0)",
                    (office, get_pal_time(), get_pal_time(), new_pin),
                )
        elif action == "set_total_debt":
            c.close()
            conn.close()
            return {"status": "error", "message": "تم إيقاف تعديل الدين النهائي. استخدم إضافة دين أو تسجيل سداد."}
        elif action == "add_debt_charge":
            amount = int(data.get("amount", 0) or 0)
            note = clean_office_name(data.get("note")) or "إضافة دين"
            if not office or amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "office and amount are required"}
            c.execute(
                """
                INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, approved_at)
                VALUES (%s,%s,%s,%s,%s,'مقبول',0,'تسوية دين يدوية',%s)
                """,
                (0, f"إضافة دين: {note}", amount, office, get_pal_time(), get_pal_time()),
            )
        elif action == "add_debt_payment":
            amount = int(data.get("amount", 0) or 0)
            note = clean_office_name(data.get("note")) or "سداد خارجي"
            if not payment_method:
                c.close()
                conn.close()
                return {"status": "error", "message": "اختر طريقة حفظ التحويل"}
            if not office or amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "office and amount are required"}
            current_amount = int(fetch_current_debt(c, office) or 0)
            if current_amount <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "لا يوجد دين حالي لهذا المكتب"}
            if amount > current_amount:
                c.close()
                conn.close()
                return {"status": "error", "message": "قيمة السداد أكبر من الدين الحالي"}
            c.execute(
                """
                INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, approved_at, payment_method)
                VALUES (%s,%s,%s,%s,%s,'مقبول',0,'سداد دين',%s,%s)
                """,
                (0, f"سداد دين: {note}", -amount, office, get_pal_time(), get_pal_time(), payment_method),
            )
            deactivate_debt_collection_if_clear(c, office)
        elif action == "add_manual_debt":
            c.close()
            conn.close()
            return {"status": "error", "message": "تم إيقاف إضافة الدين اليدوية القديمة. استخدم إضافة دين من نافذة تعديل الدين."}
        elif action == "remove_debt_item":
            item_name = clean_office_name(data.get("item_name"))
            if not order_id or not item_name:
                c.close()
                conn.close()
                return {"status": "error", "message": "order and item are required"}
            c.execute("SELECT details, total_price, location, timestamp, item_snapshot FROM orders WHERE id=%s AND status='مقبول' AND is_paid=0", (order_id,))
            order_row = c.fetchone()
            if not order_row:
                c.close()
                conn.close()
                return {"status": "error", "message": "order not found"}
            snapshot = parse_item_snapshot(order_row[4])
            removed_from_snapshot = False
            item_price = 0
            if snapshot:
                for idx, item in enumerate(snapshot):
                    if item.get("name") == item_name:
                        item_price = int(item.get("price", 0) or 0)
                        snapshot.pop(idx)
                        removed_from_snapshot = True
                        break
                if not removed_from_snapshot:
                    c.close()
                    conn.close()
                    return {"status": "error", "message": "item not found"}
                items = [item.get("name") for item in snapshot if item.get("name")]
            else:
                items = [item.strip() for item in (order_row[0] or "").split(",") if item.strip()]
                if item_name not in items:
                    c.close()
                    conn.close()
                    return {"status": "error", "message": "item not found"}
                item_price = int(PRICES.get(item_name, 0) or 0)
                items.remove(item_name)
            if item_price <= 0:
                c.close()
                conn.close()
                return {"status": "error", "message": "item price not found"}
            new_details = ", ".join(items) if items else "تم حذف جميع الأصناف من هذا الطلب"
            new_snapshot = json.dumps(snapshot, ensure_ascii=False) if removed_from_snapshot else order_row[4]
            c.execute("UPDATE orders SET details=%s, item_snapshot=%s WHERE id=%s", (new_details, new_snapshot, order_id))
            c.execute(
                """
                INSERT INTO orders (user_id, details, total_price, location, timestamp, status, is_paid, order_type, approved_at)
                VALUES (%s,%s,%s,%s,%s,'مقبول',0,'حذف صنف من الدين',%s)
                """,
                (0, f"تسوية دين: تم حذف الصنف {item_name} من الدين من قبل الإدارة", -item_price, order_row[2], get_pal_time(), get_pal_time()),
            )
            deactivate_debt_collection_if_clear(c, order_row[2])
        elif action in ("add_menu_item", "update_menu_item"):
            item_id = data.get("item_id")
            name = clean_office_name(data.get("name"))
            category = clean_office_name(data.get("category"))
            snack_type = clean_office_name(data.get("snack_type"))
            price = int(data.get("price", 0) or 0)
            is_today_special = 1 if int(data.get("is_today_special", 0) or 0) else 0
            if category not in VALID_MENU_CATEGORIES:
                return {"status": "error", "message": "تصنيف المنيو غير صحيح"}
            if category == "candy":
                snack_type = snack_type if snack_type in VALID_CANDY_TYPES else "sweet"
            else:
                snack_type = ""
            if not name or price < 0:
                return {"status": "error", "message": "اسم الصنف والسعر مطلوبان"}
            emoji = get_menu_emoji(category, snack_type)
            now = get_pal_time()
            if action == "add_menu_item":
                c.execute("SELECT COALESCE(MAX(sort_order), 0) + 1 FROM menu_items WHERE category=%s", (category,))
                sort_order = c.fetchone()[0] or 1
                item_key = f"db{int(datetime.utcnow().timestamp() * 1000)}{random.randint(100,999)}"
                c.execute(
                    """
                    INSERT INTO menu_items (item_key, name, price, category, emoji, snack_type, is_active, is_deleted, sort_order, is_today_special, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,1,0,%s,%s,%s,%s)
                    """,
                    (item_key, name, price, category, emoji, snack_type, sort_order, is_today_special, now, now),
                )
            else:
                if not item_id:
                    return {"status": "error", "message": "item_id is required"}
                c.execute(
                    """
                    UPDATE menu_items
                    SET name=%s, price=%s, category=%s, emoji=%s, snack_type=%s, is_today_special=%s, updated_at=%s
                    WHERE id=%s AND COALESCE(is_deleted,0)=0
                    """,
                    (name, price, category, emoji, snack_type, is_today_special, now, item_id),
                )
                if c.rowcount == 0:
                    return {"status": "error", "message": "الصنف غير موجود"}
        elif action == "toggle_menu_item":
            item_id = data.get("item_id")
            is_active = 1 if int(data.get("is_active", 0) or 0) else 0
            if not item_id:
                return {"status": "error", "message": "item_id is required"}
            c.execute("UPDATE menu_items SET is_active=%s, updated_at=%s WHERE id=%s AND COALESCE(is_deleted,0)=0", (is_active, get_pal_time(), item_id))
        elif action == "toggle_menu_today_special":
            item_id = data.get("item_id")
            is_today_special = 1 if int(data.get("is_today_special", 0) or 0) else 0
            if not item_id:
                return {"status": "error", "message": "item_id is required"}
            c.execute("UPDATE menu_items SET is_today_special=%s, updated_at=%s WHERE id=%s AND COALESCE(is_deleted,0)=0", (is_today_special, get_pal_time(), item_id))
        elif action == "delete_menu_item":
            item_id = data.get("item_id")
            if not item_id:
                return {"status": "error", "message": "item_id is required"}
            c.execute("UPDATE menu_items SET is_deleted=1, is_active=0, updated_at=%s WHERE id=%s", (get_pal_time(), item_id))
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
        if action == "reset_office_pin":
            return {"status": "success"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))











