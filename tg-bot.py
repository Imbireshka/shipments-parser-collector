"""
Generic Telegram Bot for Time-Based Data Subscriptions

Features:
- Whitelist by chat_id
- Pagination over location list
- Hourly time-slot subscriptions (6:00–22:00)
- Automatic report delivery from PostgreSQL
- Clean UI with inline buttons

"""

import logging
import os
import psycopg2
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from datetime import datetime, timedelta
import asyncio
import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# === CONFIGURATION ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN must be set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "data_db"),
    "user": os.getenv("DB_USER", "bot_user"),
    "password": os.getenv("DB_PASSWORD"),
}

# Timezone and UI settings
LOCAL_TZ = pytz.timezone(os.getenv("TIMEZONE", "Europe/Moscow"))
PVS_PER_PAGE = int(os.getenv("ITEMS_PER_PAGE", "8"))

# List of locations (replace with your own)
LOCATIONS = os.getenv("LOCATION_LIST", "").split(",") if os.getenv("LOCATION_LIST") else [
    "location_a", "location_b", "location_c"
]

# Hourly slots from 6:00 to 22:00
TIME_SLOTS = [f"{h}:00-{h+1}:00" for h in range(6, 22)]

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# === DATABASE UTILITIES ===
def db_fetch(query, params=None):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def db_execute(query, params=None):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            conn.commit()

def is_whitelisted(chat_id: int) -> bool:
    rows = db_fetch("SELECT 1 FROM user_whitelist WHERE chat_id = %s", (chat_id,))
    return len(rows) > 0


# === UI HELPERS ===
def get_locations_keyboard(page: int):
    start = page * PVS_PER_PAGE
    end = start + PVS_PER_PAGE
    current_items = LOCATIONS[start:end]
    buttons = [[InlineKeyboardButton(loc, callback_data=f"loc:{loc}")] for loc in current_items]
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Back", callback_data=f"page_choose:{page-1}"))
    if end < len(LOCATIONS):
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_choose:{page+1}"))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(buttons)

def get_user_subscriptions_keyboard(chat_id: int, page: int):
    subs = db_fetch(
        "SELECT location_name, time_slot FROM user_subscriptions WHERE chat_id = %s ORDER BY location_name, time_slot",
        (chat_id,)
    )
    if not subs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("No subscriptions", callback_data="no_subs")]])
    
    start = page * PVS_PER_PAGE
    end = start + PVS_PER_PAGE
    current_subs = subs[start:end]
    buttons = [
        [InlineKeyboardButton(f"{loc} — {ts}", callback_data=f"rm_single:{loc}:{ts}")]
        for loc, ts in current_subs
    ]
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Back", callback_data=f"page_remove:{page-1}"))
    if end < len(subs):
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_remove:{page+1}"))
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(buttons)

def format_subscriptions(chat_id: int) -> str:
    subs = db_fetch(
        "SELECT location_name, time_slot FROM user_subscriptions WHERE chat_id = %s ORDER BY location_name, time_slot",
        (chat_id,)
    )
    if not subs:
        return "You have no subscriptions."
    lines = ["📌 Your subscriptions:"]
    for loc, time_slot in subs:
        lines.append(f"• {loc} — {time_slot}")
    return "\n".join(lines)


# === COMMAND HANDLERS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_whitelisted(chat_id):
        await update.message.reply_text("Access denied. Contact admin to get whitelisted.")
        return
    await update.message.reply_text(
        "Welcome! Subscribe to locations and time slots to receive automated reports.\n\n"
        "Commands:\n"
        "/choose_location — subscribe to a location\n"
        "/remove_location — unsubscribe\n"
        "/subscribe — view all subscriptions\n"
        "/help — show this message"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_whitelisted(chat_id):
        return
    help_text = (
        "<b>Available commands:</b>\n"
        "<b>/start</b> — welcome message\n"
        "<b>/help</b> — this help\n"
        "<b>/choose_location</b> — subscribe to location + time slot\n"
        "<b>/remove_location</b> — unsubscribe\n"
        "<b>/subscribe</b> — list your subscriptions"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")

async def choose_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_whitelisted(chat_id):
        return
    await update.message.reply_text("Select a location (page 1):", reply_markup=get_locations_keyboard(0))

async def remove_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_whitelisted(chat_id):
        return
    subs = db_fetch("SELECT 1 FROM user_subscriptions WHERE chat_id = %s", (chat_id,))
    if not subs:
        await update.message.reply_text("You have no subscriptions to remove.")
        return
    await update.message.reply_text("Select subscription to remove (page 1):", reply_markup=get_user_subscriptions_keyboard(chat_id, 0))

async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_whitelisted(chat_id):
        return
    msg = format_subscriptions(chat_id)
    await update.message.reply_text(msg)


# === CALLBACK HANDLER ===
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    data = query.data

    if not is_whitelisted(chat_id):
        return

    if data == "cancel":
        await query.edit_message_text("Cancelled.")
        return
    if data == "no_subs":
        await query.edit_message_text("No subscriptions.")
        return

    if data.startswith("page_choose:"):
        page = int(data.split(":")[1])
        await query.edit_message_text(f"Select location (page {page + 1}):", reply_markup=get_locations_keyboard(page))
        return

    if data.startswith("page_remove:"):
        page = int(data.split(":")[1])
        await query.edit_message_text(f"Select subscription to remove (page {page + 1}):", reply_markup=get_user_subscriptions_keyboard(chat_id, page))
        return

    if data.startswith("loc:"):
        loc = data.split(":", 1)[1]
        if loc not in LOCATIONS:
            await query.edit_message_text("Invalid location.")
            return
        context.user_data["selected_loc"] = loc
        buttons = [
            [InlineKeyboardButton(slot, callback_data=f"time:{slot}") for slot in TIME_SLOTS[i:i+2]]
            for i in range(0, len(TIME_SLOTS), 2)
        ]
        buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
        await query.edit_message_text(f"Selected: {loc}\nChoose time slot:", reply_markup=InlineKeyboardMarkup(buttons))
        return

    if data.startswith("time:"):
        time_slot = data.split(":", 1)[1]
        loc = context.user_data.get("selected_loc")
        if not loc or time_slot not in TIME_SLOTS:
            await query.edit_message_text("❌ Selection error.")
            return
        try:
            db_execute(
                "INSERT INTO user_subscriptions (chat_id, location_name, time_slot) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (chat_id, loc, time_slot)
            )
            await query.edit_message_text(
                f"✅ Subscribed to {loc} at {time_slot}.\n{format_subscriptions(chat_id)}\n\nUse /choose_location to add more."
            )
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            await query.edit_message_text("❌ Failed to save subscription.")
        return

    if data.startswith("rm_single:"):
        parts = data.split(":", 3)
        if len(parts) != 4:
            await query.edit_message_text("❌ Invalid data.")
            return
        _, loc, time_slot = parts[0], parts[1], parts[2] + ":" + parts[3]
        try:
            db_execute(
                "DELETE FROM user_subscriptions WHERE chat_id = %s AND location_name = %s AND time_slot = %s",
                (chat_id, loc, time_slot)
            )
            await query.edit_message_text(
                f"❌ Unsubscribed from {loc} at {time_slot}.\n{format_subscriptions(chat_id)}\n\nUse /remove_location to remove more."
            )
        except Exception as e:
            logger.error(f"Unsubscribe error: {e}")
            await query.edit_message_text("❌ Failed to remove subscription.")
        return


# === REPORTING ENGINE ===
def format_timedelta_seconds(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"

async def monitor_hourly_slot(context, hour: int):
    time_slot = f"{hour}:00-{hour+1}:00"
    end_time = datetime.now(LOCAL_TZ).replace(minute=59, second=59, microsecond=0)
    logger.info(f"🔍 Monitoring slot {time_slot} until {end_time.strftime('%H:%M:%S')}")

    while datetime.now(LOCAL_TZ) <= end_time:
        subs = db_fetch(
            "SELECT DISTINCT chat_id, location_name FROM user_subscriptions WHERE time_slot = %s",
            (time_slot,)
        )
        if not subs:
            break

        user_loc_map = {}
        for chat_id, loc in subs:
            user_loc_map.setdefault(chat_id, []).append(loc)

        any_sent = False
        for chat_id, loc_list in user_loc_map.items():
            for loc in loc_list:
                rows = db_fetch("""
                    SELECT location_name, delivery_date, created_at, unload_started_at, closed_at,
                           status, sent, received, excess, boxes_count, unload_duration_seconds
                    FROM data_reports
                    WHERE location_name = %s
                      AND delivery_date = CURRENT_DATE
                      AND EXTRACT(HOUR FROM inserted_at) = %s
                    ORDER BY inserted_at
                """, (loc, hour))

                if not rows:
                    continue

                # Build one message per location
                lines = [f"📍 Location: {loc}"]
                for row in rows:
                    (_, delivery_date, created_at, unload_started_at, closed_at,
                     status, sent, received, excess, boxes_count, unload_duration_seconds) = row

                    def fmt_dt(dt):
                        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "-"

                    display_status = status.capitalize()
                    duration_str = format_timedelta_seconds(unload_duration_seconds)
                    close_line = f"Closed at: {fmt_dt(closed_at)}"

                    lines.append(
                        f"Date: {delivery_date}\n"
                        f"Unload started: {fmt_dt(unload_started_at)}\n"
                        f"{close_line}\n"
                        f"❗Unload duration: {duration_str}\n"
                        f"Status: {display_status}\n"
                        f"Boxes: {boxes_count}\n"
                        f"Sent: {sent}\n"
                        f"Received: {received}\n"
                        f"Excess: {excess}"
                    )

                message = "\n".join(lines)
                try:
                    await context.bot.send_message(chat_id=chat_id, text=message)
                    logger.info(f"📤 Report for {loc} sent to {chat_id}")
                    any_sent = True
                except Exception as e:
                    logger.error(f"❌ Send error to {chat_id}: {e}")

        if any_sent:
            break

        await asyncio.sleep(30)

async def trigger_hourly_check(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(LOCAL_TZ)
    current_hour = now.hour
    if 6 <= current_hour < 22:
        logger.info(f"⏰ Triggering report check for hour {current_hour}")
        asyncio.create_task(monitor_hourly_slot(context, current_hour))


# === MAIN ===
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("choose_location", choose_location))
    app.add_handler(CommandHandler("remove_location", remove_location))
    app.add_handler(CommandHandler("subscribe", show_subscriptions))
    app.add_handler(CallbackQueryHandler(handle_callback))

    app.job_queue.run_repeating(trigger_hourly_check, interval=3600, first=10)
    logger.info("🚀 Bot started. Reporting active from 6:00 to 22:00.")

    app.run_polling()

if __name__ == "__main__":
    main()
