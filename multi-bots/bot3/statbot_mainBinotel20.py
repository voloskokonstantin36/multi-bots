# === СТАНДАРТНЫЕ БИБЛИОТЕКИ ===
import os
import json
import logging
import asyncio
import html
import time
import re
import shutil
import tempfile
import csv
import aiohttp
import requests
import pytz

from pathlib import Path
from datetime import datetime, date, timedelta

# === СТОРОННИЕ БИБЛИОТЕКИ ===
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
    ChatMemberUpdated,
    ChatMember,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ChatMemberHandler,
    CallbackContext,
)

from telegram.helpers import escape_markdown

# === НАСТРОЙКА ЛОГИРОВАНИЯ ===
logging.basicConfig(level=logging.INFO)
logging.getLogger("apscheduler").setLevel(logging.ERROR)
logging.getLogger("telegram").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Корень проекта
ROOT_DIR = Path(__file__).resolve().parent.parent  # или адаптируй путь к твоему .env
load_dotenv(dotenv_path=ROOT_DIR / ".env")

BOT_TOKEN = os.getenv("BOT3_TOKEN") 

# Локальная папка бота
BASE_DIR = Path(__file__).resolve().parent

# Читаем переменные окружения
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN")  # https://yourdomain.com
WEBHOOK_PATH = "/webhook/bot3"
WEBHOOK_URL = f"{WEBHOOK_DOMAIN}{WEBHOOK_PATH}"
ERROR_CHANNEL_ID = int(os.getenv("ERROR_CHANNEL_ID", "-1"))  # если не задан, будет -1
SESSION_ID = os.getenv("SESSION_ID")

# Функция загрузки настроек из JSON (settings.json) из той же папки
def load_settings(filename: str = "settings.json") -> dict:
    try:
        settings_path = BASE_DIR / filename
        with settings_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Ошибка загрузки {filename}: {e}")
        return {}

settings = load_settings()
ADMIN_LIST = settings.get("ADMIN_LIST", [])
REPORT_CHANNEL_ID = settings.get("REPORT_CHANNEL_ID")

# --- Сообщение об падениях бота ---
async def notify_admins(context, message: str):
    try:
        await message_queue.send(
            context.bot,
            chat_id=ERROR_CHANNEL_ID,
            text=f"🚨 {message}"
        )
    except Exception as e:
        logging.error(f"❌ Не удалось отправить ошибку в канал: {e}")

# --- Работа с JSON ---
def load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Файл {path} не найден.")
        return {}
    except Exception as e:
        logging.error(f"Ошибка чтения {path}: {e}")
        return {}

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Ошибка сохранения {path}: {e}")

def save_settings(data, path="settings.json"):
    save_json(path, data)

def get_admin_ids() -> list[int]:
    try:
        settings = load_settings()
        if "ADMIN_LIST" in settings:
            return [int(admin["user_id"]) for admin in settings["ADMIN_LIST"]]
        return [int(uid) for uid in settings.get("ADMIN_IDS", [])]
    except Exception as e:
        print(f"Ошибка чтения админов: {e}")
        return []

def is_admin(user_id: int) -> bool:
    return int(user_id) in get_admin_ids()

FOLDER_NEW = BASE_DIR / "new_data"
FOLDER_OLD = BASE_DIR / "old_data"
CURRENT_NEW_FILE = FOLDER_NEW / "new_data.json"

def find_latest_old_json():
    yesterday = datetime.now().date() - timedelta(days=1)

    old_files = sorted(
        FOLDER_OLD.glob("*.json"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )

    for file in old_files:
        try:
            modified_time = datetime.fromtimestamp(file.stat().st_mtime).date()
            if modified_time == yesterday:
                continue
            return file
        except Exception:
            continue
    return None

def escape_markdown_tag(tag: str) -> str:
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    for ch in escape_chars:
        tag = tag.replace(ch, '\\' + ch)
    return tag

def update_report_channel(new_channel_id: str):
    global REPORT_CHANNEL_ID, settings
    try:
        channel_id_int = int(new_channel_id)
    except ValueError:
        logging.error(f"Неверный ID канала: {new_channel_id}")
        return False

    settings["REPORT_CHANNEL_ID"] = channel_id_int
    REPORT_CHANNEL_ID = channel_id_int
    save_settings(settings)
    logging.info(f"Канал отчёта обновлён на {new_channel_id}")
    return True

# Загружаем звонки

def fetch_via_playwright() -> Path | None:
    import os, json, csv, time, tempfile, shutil, requests, pytz
    from datetime import datetime, timedelta, date
    from pathlib import Path
    from dotenv import load_dotenv

    def should_replace_file(file_path: Path, interval_end: datetime) -> bool:
        if not file_path.exists():
            return True
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=pytz.UTC)
        return file_mtime < interval_end.astimezone(pytz.UTC)

    # Ключи и пути из .env
    BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
    BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")
    BINOTEL_FOLDER = BASE_DIR / os.getenv("BINOTEL_FOLDER", "binotel")
    CSV_OUTPUT_FOLDER = BASE_DIR / os.getenv("BINOTEL_CSV_FOLDER", "new_data")

    if not BINOTEL_API_KEY or not BINOTEL_API_SECRET:
        print("❌ Не заданы ключи BINOTEL_API_KEY или BINOTEL_API_SECRET")
        return None

    KYIV_TZ = pytz.timezone("Europe/Kyiv")
    now_kyiv = datetime.now(KYIV_TZ)
    date_str = now_kyiv.strftime("%Y-%m-%d")

    day_folder = BINOTEL_FOLDER / date_str
    day_folder.mkdir(parents=True, exist_ok=True)

    # Удаление старых папок
    for subdir in BINOTEL_FOLDER.iterdir():
        if subdir.is_dir() and subdir.name != date_str:
            shutil.rmtree(subdir)

    start_of_interval = now_kyiv.replace(hour=7, minute=30, second=0, microsecond=0)
    planned_end = now_kyiv.replace(hour=22, minute=0, second=0, microsecond=0)
    if now_kyiv < start_of_interval:
        print("⚠️ Ещё не наступило время для запросов (до 07:30)")
        return None

    minute = (now_kyiv.minute // 30) * 30
    last_interval_end = now_kyiv.replace(minute=minute, second=0, microsecond=0) + timedelta(minutes=30)
    end_of_interval = min(planned_end, last_interval_end)
    if end_of_interval <= start_of_interval:
        print("⚠️ Нет доступных интервалов для запроса")
        return None

    interval = timedelta(minutes=30)
    current_start = start_of_interval

    while current_start < end_of_interval:
        current_end = current_start + interval
        filename = f"{current_start.strftime('%H_%M')}_{current_end.strftime('%H_%M')}.json"
        filepath = day_folder / filename

        if not should_replace_file(filepath, current_end):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("status") == "success" and data.get("callDetails"):
                    print(f"🔁 Уже скачано: {filename}")
                    current_start = current_end
                    continue
            except Exception as e:
                print(f"⚠️ Повреждённый файл {filename}, перезапрашиваем...")

        payload = {
            "startTime": int(current_start.timestamp()),
            "stopTime": int(current_end.timestamp()),
            "key": BINOTEL_API_KEY,
            "secret": BINOTEL_API_SECRET
        }

        try:
            response = requests.post(
                "https://api.binotel.com/api/4.0/stats/outgoing-calls-for-period.json",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=30
            )

            print(f"📡 {current_start.strftime('%H:%M')}–{current_end.strftime('%H:%M')} — Статус: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                print(f"❌ Ошибка HTTP {response.status_code}")
        except Exception as e:
            print(f"❌ Ошибка запроса: {e}")

        current_start = current_end
        time.sleep(1)

    # Обработка и сохранение CSV
    transformed_calls = []
    for file in sorted(day_folder.glob("*.json")):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("status") != "success":
                continue

            call_details = data.get("callDetails", {})
            calls = list(call_details.values()) if isinstance(call_details, dict) else call_details

            for call in calls:
                employee_data = call.get("employeeData", {})
                employee_name = employee_data.get("name", "") if isinstance(employee_data, dict) else ""
                transformed_calls.append({
                    "general call id": call.get("generalCallID", ""),
                    "date": datetime.fromtimestamp(int(call.get("startTime", 0)), KYIV_TZ).strftime("%H:%M %d-%m-%Y"),
                    "pbx number": call.get("pbxNumberData", {}).get("number", ""),
                    "pbx number name": "",
                    "customer number": call.get("externalNumber", ""),
                    "customer name": "",
                    "link to crm": "",
                    "list of labels in customer": "",
                    "employee number": call.get("internalNumber", ""),
                    "employee name": employee_name,
                    "waitsec": call.get("waitsec", ""),
                    "billsec": call.get("billsec", ""),
                    "disposition": call.get("disposition", ""),
                    "trunkNumber": "",
                    "isNewCall": call.get("isNewCall", ""),
                    "recording status": call.get("recordingStatus", ""),
                    "who hung up": call.get("whoHungUp", ""),
                    "comment": "",
                    "tags": ""
                })

        except Exception as e:
            print(f"⚠️ Ошибка чтения {file.name}: {e}")

    if not transformed_calls:
        print("⚠️ Нет данных для CSV")
        return None

    desired_columns = list(transformed_calls[0].keys())
    tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmpfile, fieldnames=desired_columns, delimiter=';', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(transformed_calls)
    tmpfile.close()

    final_path = CSV_OUTPUT_FOLDER / f"binotel_calls_{date_str}.csv"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(tmpfile.name, final_path)
    print(f"✅ CSV сохранён: {final_path}")
    return final_path

def get_today_file(folder: Path):
    today = date.today()
    for file in folder.iterdir():
        if file.is_file():
            mdate = datetime.fromtimestamp(file.stat().st_mtime).date()
            if mdate == today:
                return file
    return None

import uuid
from playwright.async_api import async_playwright

load_dotenv()  # загружаем .env при импорте

async def fetch_json_data() -> Path | None:
    save_path = Path("new_data") / "data.json"
    save_path.parent.mkdir(parents=True, exist_ok=True)

    session_id = os.getenv("SESSION_ID")
    if not session_id:
        print("❌ SESSION_ID не найден в .env")
        return None

    url = "https://flash-team.com.ua/control_panel/statistics/download"

    headers = {
        "Cookie": f"session_id={session_id}",
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    print(f"❌ HTTP ошибка: {resp.status}")
                    return None

                try:
                    json_data = await resp.json()
                except Exception as e:
                    print(f"❌ Ошибка при разборе JSON: {e}")
                    return None

                with open(save_path, "w", encoding="utf-8") as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)

                return save_path

    except Exception as e:
        print(f"❌ Ошибка при получении JSON: {e}")
        return None
        
def move_file(src: Path, dst_folder: Path):
    dst_folder.mkdir(parents=True, exist_ok=True)
    dst_file = dst_folder / src.name
    if dst_file.exists():
        dst_file.unlink()
    shutil.move(str(src), str(dst_file))
    return dst_file

USERS_FILE = BASE_DIR / "users.json"
NORMS_FILE = BASE_DIR / "norms.json"
CURRENT_OLD_FILE = BASE_DIR / "current" / "old_data.json"
CURRENT_NEW_FILE = BASE_DIR / "current" / "new_data.json"

def load_users():
    data = load_json(USERS_FILE)
    return data if isinstance(data, list) else []

def save_users(users):
    save_json(USERS_FILE, users)

def adapt_new_format(json_data):
    if not isinstance(json_data, dict):
        return {}

    if "user_stats" in json_data:
        result = {}

        for entry in json_data.get("user_stats", []):
            initials = entry.get("user_data", {}).get("identifier", "").upper()
            if not initials:
                continue

            general = entry.get("general_stats", {})
            resale_percent = general.get("orders_with_resale_percent", 0.0)

            # преобразуем проекты в список
            project_list = []
            for project_name, stats in (entry.get("projects") or {}).items():
                project_list.append({
                    "name": project_name,
                    "upsell_percent": stats.get("orders_with_resale_percent", 0.0),
                    "orders": stats.get("orders_total", 0),  # 🔧 исправлено здесь
                    "avg_check": stats.get("avg_check", 0.0)
                })

            result[initials] = {
                "upsell_percent": resale_percent,
                "avg_check": general.get("avg_check", 0.0),
                "speed": entry.get("orders_per_hour", 0.0),
                "orders_total": general.get("orders_total", 0),
                "projects": project_list
            }

        return result

    return json_data  # старый формат

def load_norms(path=NORMS_FILE):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}

def save_norms(norms):
    with open(NORMS_FILE, "w", encoding="utf-8") as f:
        json.dump(norms, f, ensure_ascii=False, indent=2)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return

    user_id = update.effective_user.id

    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав администратора для доступа к меню.")
        return

    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="broadcast_menu")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="manage_users")],
        [InlineKeyboardButton("📏 Настройка норм", callback_data="norms")],
        [InlineKeyboardButton("🧹 Очистить недоступные группы", callback_data="clean_invalid")]
    ]

    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("⚙️ Админ. управление", callback_data="admin_manage")])
        keyboard.append([InlineKeyboardButton("📊 Статистика: процент по операторам и CRM", callback_data="debug_command")])

    keyboard.append([InlineKeyboardButton("🚪 Выход", callback_data="exit")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Меню:", reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    try:
        await query.answer()
    except Exception as e:
        logging.warning(f"❗ query.answer() error: {e}")

    data = query.data
    user_id = query.from_user.id

    if not is_admin(user_id):
        await query.edit_message_text("❌ У вас нет прав для управления этим ботом.", parse_mode=None)
        return

    if data == "add_admin":
        await query.edit_message_text("Введите ID нового администратора (число):", parse_mode=None)
        context.user_data['adding_admin'] = {'step': 'id'}
        return

    elif data == "show_broadcast_menu":
        await show_broadcast_menu_callback(query, context)

    elif data == "broadcast_by_initials":
        await show_back_button(query, "Введите инициалы для рассылки через пробел или 'всем' для всех:")
        context.user_data["awaiting_initials_broadcast"] = True

    elif data == "broadcast_by_template":
        await show_back_button(query, "Введите текст шаблона с {tag} для рассылки:")
        context.user_data["awaiting_template_text"] = True

    elif data == "set_report_channel":
        await show_back_button(
            query,
            "Отправьте сюда ID канала (например, -1001234567890), куда хотите отправлять отчёты.\n"
            "Или отправьте /cancel для отмены."
        )
        context.user_data["setting_report_channel"] = True

    elif data == "back_to_main":
        await show_main_menu_callback(query, context)

    elif data == "broadcast_menu":
        await show_broadcast_menu_callback(query, context)

    elif data == "manage_users":
        await show_users_menu_callback(query, context)

    elif data == "norms":
        await show_norms_menu_callback(query, context)

    elif data == "clean_invalid":
        await clean_invalid_groups(query, context)

    elif data == "admin_manage":
        await show_admins_menu_callback(query, context)

    elif data == "exit":
        await query.edit_message_text("Выход из меню.", parse_mode=None)

    elif data.startswith("user_"):
        await handle_user_button(query, context, data)

    elif data.startswith("edit_user_tag_"):
        initials = data[len("edit_user_tag_"):]
        context.user_data["editing_user_tag"] = initials
        context.user_data["awaiting_user_tag"] = True
        await query.edit_message_text(f"Введите новый тег для пользователя {initials}:", parse_mode=None)

    elif data.startswith("delete_user_"):
        initials = data[len("delete_user_"):]
        users = load_users()
        users = [u for u in users if u.get("initials") != initials]
        save_users(users)
        await query.edit_message_text(f"Пользователь {initials} удалён.", parse_mode=None)

    elif data.startswith("admin_"):
        await handle_admin_button(query, context, data)

    elif data.startswith("edit_admin_name_"):
        uid = int(data[len("edit_admin_name_"):])
        context.user_data["editing_admin_name"] = uid
        context.user_data["awaiting_admin_name"] = True
        await query.edit_message_text(f"Введите новое имя для администратора с ID {uid}:", parse_mode=None)

    elif data.startswith("delete_admin_"):
        uid = int(data[len("delete_admin_"):])
        settings = load_settings()

        admin_ids = settings.get("ADMIN_IDS", [])
        if uid in admin_ids:
            admin_ids.remove(uid)
        settings["ADMIN_IDS"] = admin_ids

        admin_list = settings.get("ADMIN_LIST", [])
        admin_list = [a for a in admin_list if a.get("user_id") != uid]
        settings["ADMIN_LIST"] = admin_list

        save_settings(settings)

        global ADMIN_IDS, ADMIN_LIST
        ADMIN_IDS = admin_ids
        ADMIN_LIST = admin_list

        await query.edit_message_text(f"Администратор с ID {uid} удалён.", parse_mode=None)

    elif data.startswith("norm_"):
        await show_norm_detail_callback(query, context, data)

    elif data.startswith("editnorm_"):
        await start_edit_norm_callback(query, context, data)

    elif data.startswith("group_"):
        await handle_group_button(query, context, data)

    elif data.startswith("edit_group_tag_"):
        initials = data[len("edit_group_tag_"):]
        context.user_data["editing_group_tag"] = initials
        context.user_data["awaiting_group_tag"] = True
        await query.edit_message_text(f"Введите новый тег для группы {initials}:", parse_mode=None)

    elif data.startswith("delete_group_"):
        initials = data[len("delete_group_"):]
        groups = load_users()
        groups = [g for g in groups if g.get("initials") != initials]
        save_users(groups)
        await query.edit_message_text(f"Группа {initials} удалена.", parse_mode=None)

    elif data == "debug_command":
        await send_stats_report(query.message.reply_text, query.from_user.id)

    else:
        await query.answer("Неизвестная команда.", show_alert=True)

async def show_broadcast_menu_callback(query, context):
    keyboard = [
        [InlineKeyboardButton("📊Рассылка по инициалам", callback_data="broadcast_by_initials")],
        [InlineKeyboardButton("📝Рассылка по шаблону", callback_data="broadcast_by_template")],
        [InlineKeyboardButton("🛠Настроить канал отчёта", callback_data="set_report_channel")],
        [InlineKeyboardButton("⬅️Назад", callback_data="back_to_main")]
    ]
    await query.edit_message_text("Выберите тип рассылки:", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_back_button(query, text):
    keyboard = [[InlineKeyboardButton("Назад", callback_data="show_broadcast_menu")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_main_menu_callback(query, context):
    user_id = query.from_user.id

    keyboard = [
        [InlineKeyboardButton("📤 Начать рассылку", callback_data="broadcast_menu")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="manage_users")],
        [InlineKeyboardButton("📏 Настройка норм", callback_data="norms")],
        [InlineKeyboardButton("🧹 Очистить недоступные группы", callback_data="clean_invalid")]
    ]

    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("⚙️ Админ. управление", callback_data="admin_manage")])

    keyboard.append([InlineKeyboardButton("🚪 Выход", callback_data="exit")])

    await query.edit_message_text("Меню:", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_users_menu_callback(query, context):
    users = load_users()
    if not users:
        await query.edit_message_text(
            "Пользователей пока нет.\n\nНажмите 'Назад' для возврата.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="back_to_main")]])
        )
        return
    keyboard = [
        [InlineKeyboardButton(f"{u.get('initials', '??')} ({u.get('tag', '')})", callback_data=f"user_{u.get('initials')}")]
        for u in users
    ]
    keyboard.append([InlineKeyboardButton("⬅️Назад", callback_data="back_to_main")])
    await query.edit_message_text("Список пользователей:", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_user_button(query, context, data):
    initials = data[len("user_"):]
    keyboard = [
        [InlineKeyboardButton("✏️Изменить тег", callback_data=f"edit_user_tag_{initials}")],
        [InlineKeyboardButton("❌Удалить пользователя", callback_data=f"delete_user_{initials}")],
        [InlineKeyboardButton("⬅️Назад", callback_data="manage_users")]
    ]
    await query.edit_message_text(f"Пользователь: {initials}", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_admins_menu_callback(query, context):
    if not ADMIN_LIST:
        keyboard = [
            [InlineKeyboardButton("➕ Добавить администратора", callback_data="add_admin")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
        ]
        await query.edit_message_text(
            "Администраторов нет.\n\nНажмите 'Добавить администратора' для создания.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    keyboard = []
    for adm in ADMIN_LIST:
        name = adm.get("name", "Без имени")
        uid = adm.get("user_id")
        keyboard.append([InlineKeyboardButton(f"{name} (ID: {uid})", callback_data=f"admin_{uid}")])
    keyboard.append([InlineKeyboardButton("➕ Добавить администратора", callback_data="add_admin")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])

    await query.edit_message_text("📋 Список администраторов:", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_admin_button(query, context, data):
    uid = int(data[len("admin_"):])
    admin = next((a for a in ADMIN_LIST if a["user_id"] == uid), None)
    if not admin:
        await query.edit_message_text("Администратор не найден.")
        return

    name = html.escape(admin.get("name", ""))
    tag = html.escape(admin.get("tag", ""))

    keyboard = [
        [InlineKeyboardButton("✏️Изменить имя", callback_data=f"edit_admin_name_{uid}")],
        [InlineKeyboardButton("❌Удалить администратора", callback_data=f"delete_admin_{uid}")],
        [InlineKeyboardButton("⬅️Назад", callback_data="admin_manage")]
    ]

    await query.edit_message_text(
        f"👤 <b>{name}</b> {tag}\n🆔 ID: <code>{uid}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_norms_menu_callback(query, context):
    global norms
    await query.answer()
    if not norms:
        await query.edit_message_text(
            "❌Нормы не заданы.\n\nНажмите '⬅️ Назад' для возврата.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
        )
        return

    keyboard = [
        [InlineKeyboardButton(norm_key, callback_data=f"norm_{norm_key}")]
        for norm_key in norms.keys()
    ]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])

    await query.edit_message_text(
        "👉Выберите норму для просмотра/редактирования:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_norm_detail_callback(query, context, data):
    global norms
    norm_name = data[len("norm_"):]
    norms = load_norms()
    norm_values = norms.get(norm_name, {})

    color_emoji = {
        "червона": "🔴",
        "жовта": "🟡",
        "зелена": "🟢"
    }

    text = f"Норма: *{norm_name}*\n"
    for zone in ["червона", "жовта", "зелена"]:
        val = norm_values.get(zone, "не задано")
        emoji = color_emoji.get(zone, "")
        text += f"{emoji} {zone}: {val}\n"

    keyboard = [
        [InlineKeyboardButton(f"Изменить {color_emoji[zone]} {zone}", callback_data=f"editnorm_{norm_name}_{zone}")]
        for zone in ["червона", "жовта", "зелена"]
    ]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="norms")])

    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

async def start_edit_norm_callback(query, context, data):
    parts = data.split("_")
    zone = parts[-1]
    norm_name = "_".join(parts[1:-1])
    context.user_data["editing_norm"] = (norm_name, zone)
    context.user_data["awaiting_norm_value"] = True
    await query.edit_message_text(f"Введите новое значение для {norm_name} ({zone}):")

async def show_groups_menu_callback(query, context):
    users = load_users()
    if not users:
        await query.edit_message_text(
            "❌Группы не найдены.\n\nНажмите 'Назад' для возврата.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="back_to_main")]])
        )
        return

    keyboard = [
        [InlineKeyboardButton(f"{u.get('initials', '??')} ({u.get('tag', '')})", callback_data=f"group_{u.get('initials')}")]
        for u in users
    ]
    keyboard.append([InlineKeyboardButton("⬅️Назад", callback_data="back_to_main")])

    await query.edit_message_text("📋Список групп:", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_group_button(query, context, data):
    initials_raw = data[len("group_"):]
    initials = html.escape(initials_raw)

    keyboard = [
        [InlineKeyboardButton("✏️Изменить тег", callback_data=f"edit_group_tag_{initials_raw}")],
        [InlineKeyboardButton("❌Удалить группу", callback_data=f"delete_group_{initials_raw}")],
        [InlineKeyboardButton("⬅️Назад", callback_data="manage_groups")]
    ]

    await query.edit_message_text(
        f"Група: <b>{initials}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def clean_invalid_groups(query, context):
    users = load_users()
    valid_users = []
    removed_groups = []

    for u in users:
        try:
            await context.bot.get_chat(u.get("user_id"))
            valid_users.append(u)
        except Exception:
            removed_groups.append(u.get("initials", "??"))

    if removed_groups:
        save_users(valid_users)
        await query.edit_message_text(f"✅Удалены недоступные группы:\n" + "\n".join(removed_groups))
    else:
        await query.edit_message_text("✅Недоступных групп не найдено.")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if text == "⬅️ Назад в главное меню":
        await update.message.reply_text("🔙 Возврат в главное меню.", reply_markup=ReplyKeyboardRemove())
        dummy_query = update  # нет query — обходим через сообщение
        await show_main_menu_callback(dummy_query, context)
        return

    if context.user_data.get("setting_report_channel"):
        if text == "/cancel":
            await update.message.reply_text("❌ Отмена установки канала отчёта.")
            context.user_data["setting_report_channel"] = False
            return

        if not re.match(r"^-?\d+$", text):
            await update.message.reply_text("❌ Неверный формат ID канала. Попробуйте ещё раз.")
            return

        update_report_channel(text)
        context.user_data["setting_report_channel"] = False
        await update.message.reply_text(f"✅ Канал отчёта обновлён на {text}")
        return

    if not is_admin(user_id):
        await update.message.reply_text("❌ У вас нет прав для работы с ботом.")
        return
    if context.user_data.get("adding_admin"):
        data = context.user_data["adding_admin"]
        step = data.get("step")

        if step == "id":
            if not text.isdigit():
                await update.message.reply_text("❌ ID должен быть числом. Введите ID нового администратора:")
                return
            data["user_id"] = int(text)
            data["step"] = "name"
            await update.message.reply_text("Введите имя администратора:")
            return

        elif step == "name":
            data["name"] = text
            data["step"] = "tag"
            await update.message.reply_text("Введите тег администратора (например, @username) или оставьте пустым:")
            return

        elif step == "tag":
            data["tag"] = text
            new_admin = {
                "user_id": data["user_id"],
                "name": data["name"],
                "tag": data["tag"]
            }
            ADMIN_LIST.append(new_admin)
            settings["ADMIN_LIST"] = ADMIN_LIST
            save_settings(settings)

            await update.message.reply_text(f"✅ Администратор {data['name']} (ID: {data['user_id']}) добавлен.")
            context.user_data.pop("adding_admin")
            return
    if context.user_data.get("awaiting_user_tag"):
        initials = context.user_data.get("editing_user_tag")
        users = load_users()
        for u in users:
            if u.get("initials") == initials:
                u["tag"] = text
                break
        save_users(users)
        await update.message.reply_text(f"✅ Тег пользователя {initials} обновлён на '{text}'.")
        context.user_data.pop("awaiting_user_tag", None)
        context.user_data.pop("editing_user_tag", None)
        return

    if context.user_data.get("awaiting_group_tag"):
        initials = context.user_data.get("editing_group_tag")
        groups = load_users()  # используем тех же users, если групп нет отдельно
        for g in groups:
            if g.get("initials") == initials:
                g["tag"] = text
                break
        save_users(groups)
        await update.message.reply_text(f"✅ Тег группы {initials} обновлён на '{text}'.")
        context.user_data.pop("awaiting_group_tag", None)
        context.user_data.pop("editing_group_tag", None)
        return

    if context.user_data.get("awaiting_admin_name"):
        uid = context.user_data.get("editing_admin_name")
        found = False
        for adm in ADMIN_LIST:
            if adm.get("user_id") == uid:
                adm["name"] = text
                found = True
                break
        if found:
            save_settings(settings)
            await update.message.reply_text(f"✅ Имя администратора с ID {uid} обновлено на '{text}'.")
        else:
            await update.message.reply_text("❌ Администратор не найден.")
        context.user_data.pop("awaiting_admin_name", None)
        context.user_data.pop("editing_admin_name", None)
        return
    if context.user_data.get("awaiting_initials_broadcast"):
        initials_input = text.upper()
        context.user_data.pop("awaiting_initials_broadcast", None)
        await broadcast_with_file_management(update, context, initials_input)
        return

    if context.user_data.get("awaiting_template_text"):
        template_text = text
        context.user_data.pop("awaiting_template_text", None)
        context.user_data["awaiting_initials_for_template"] = True
        context.user_data["template_text"] = template_text
        await update.message.reply_text("Введите инициалы для рассылки шаблона через пробел или 'всем' для всех:")
        return

    if context.user_data.get("awaiting_initials_for_template"):
        initials_input = text.upper()
        template_text = context.user_data.get("template_text", "")
        context.user_data.pop("awaiting_initials_for_template", None)
        context.user_data.pop("template_text", None)
        await perform_broadcast_by_template(update, context, template_text, initials_input)
        return

    if context.user_data.get("awaiting_norm_value"):
        norm_name, zone = context.user_data.get("editing_norm", (None, None))
        if norm_name is None or zone is None:
            await update.message.reply_text("❌ Ошибка данных норм.")
            context.user_data.pop("awaiting_norm_value", None)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text("Введите числовое значение.")
            return
        if norm_name not in norms:
            await update.message.reply_text(f"Норма '{norm_name}' не найдена.")
            context.user_data.pop("awaiting_norm_value", None)
            return
        norms[norm_name][zone] = value
        save_norms(norms)
        await update.message.reply_text(f"Норма '{norm_name}' для зоны '{zone}' обновлена на {value}.")
        context.user_data.pop("awaiting_norm_value", None)
        context.user_data.pop("editing_norm", None)
        return

    await update.message.reply_text("❌ Неизвестная команда или действие.")

async def reload_norms_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global norms
    norms = load_norms("norms.json")
    await update.message.reply_text("Норми успішно оновлено!")

async def perform_broadcast_by_template(update: Update, context: ContextTypes.DEFAULT_TYPE, template_text: str, initials_input: str):
    await update.message.reply_text(f"✅Запущена розсилка по шаблону, ініціали: {initials_input}")
    users = load_users()

    initials_list = [i.strip().upper() for i in initials_input.split()]

    if "ВСЕМ" in initials_list or "ВСІМ" in initials_list:
        target_users = users
    else:
        target_users = [u for u in users if u.get("initials", "").upper() in initials_list]

    if not target_users:
        await update.message.reply_text("❌Користувачі з такими ініціалами не знайдені.")
        return

    for user in target_users:
        tag_raw = user.get("tag", "")
        escaped_tag = escape_markdown_tag(tag_raw)
        text_to_send = template_text.replace("{tag}", escaped_tag)

        try:
            await message_queue.send(
                bot=context.bot,
                chat_id=user.get("user_id"),
                text=text_to_send,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logging.error(f"❌ Помилка при відправці шаблону користувачу {user.get('initials')}: {e}")

def get_zone_and_emoji(key, value, norms):
    if not key or not norms:
        return "—", "❔"

    key_map = {
        "upsell": "відсоток",
        "avg_check": "середній чек",
        "speed": "швидкість"
    }

    norm_key = key_map.get(key)
    if norm_key is None or norm_key not in norms:
        return "—", "❔"

    norm = norms[norm_key]

    if value < norm["червона"]:
        return "червона", "🔴"
    elif value < norm["жовта"]:
        return "жовта", "🟡"
    else:
        return "зелена", "🟢"

def generate_operator_message(user, old, new, warnings, old_file_exists=True, norms=None):
    tag_raw = user.get("tag") or ""
    tag = escape_markdown(tag_raw, version=2)
    initials = escape_markdown(user.get("initials") or "", version=2)
    orders_total = user.get("orders_total") or new.get("orders_total", 0)

    old_vals = {
        "upsell": old.get("upsell_percent") or 0.0,
        "avg_check": old.get("avg_check") or 0.0,
        "speed": old.get("speed") or 0.0
    }
    new_vals = {
        "upsell": new.get("upsell_percent") or 0.0,
        "avg_check": new.get("avg_check") or 0.0,
        "speed": new.get("speed") or 0.0
    }

    upsell_zone, upsell_emoji = get_zone_and_emoji("upsell", new_vals["upsell"], norms)
    avg_check_zone, avg_check_emoji = get_zone_and_emoji("avg_check", new_vals["avg_check"], norms)
    speed_zone, speed_emoji = get_zone_and_emoji("speed", new_vals["speed"], norms)

    lines_decline = []
    lines_growth = []

    def add_line(name, ov, nv):
        if not old_file_exists:
            return  # 💡 не сравниваем, если нет старого файла
        if not isinstance(ov, (int, float)) or not isinstance(nv, (int, float)):
            return  # 🔒 пропускаем, если что-то не число
        if abs(nv - ov) < 0.01:  # ✅ игнорируем почти равные значения
            return
        if nv < ov:
            lines_decline.append(f"- {escape_markdown(name, version=2)}: *{ov:.1f}* → *{nv:.1f}* (падає)😱")
        elif nv > ov:
            lines_growth.append(f"- {escape_markdown(name, version=2)}: *{ov:.1f}* → *{nv:.1f}* (росте)🚀")

    add_line("допродажі", old_vals["upsell"], new_vals["upsell"])
    add_line("середній чек", old_vals["avg_check"], new_vals["avg_check"])
    add_line("швидкість", old_vals["speed"], new_vals["speed"])

    metrics_block = (
        f"📈 Допродажі: *{new_vals['upsell']:.1f}%* — {upsell_emoji} {escape_markdown(upsell_zone, version=2)}\n"
        f"💰 Середній чек: *{new_vals['avg_check']:.2f} грн* — {avg_check_emoji} {escape_markdown(avg_check_zone, version=2)}\n"
        f"🕓 Замовлень/год: *{new_vals['speed']:.1f}* — {speed_emoji} {escape_markdown(speed_zone, version=2)}\n"
    )

    msg = (
        f"{tag}\n\n"
        f"🔠 Ініціали: {initials}\n"
        f"📦 Загалом замовлень: *{orders_total}*\n\n"
        f"{metrics_block}"
    )

    if old_file_exists and (lines_decline or lines_growth):
        if lines_decline:
            msg += "\n*🔻 Виявлено погіршення:*\n" + "\n".join(lines_decline) + "\n"
        if lines_growth:
            msg += "\n*🔺 Показники ростуть:*\n" + "\n".join(lines_growth) + "\n"

    if warnings:
        msg += "\n*⚠️⚠️ УВАГА! Рекомендації: ⚠️⚠️*\n"
        msg += "❗️ Потрібно покращити загальні показники‼️\n"
        msg += "\n❗️ Також у проєктах є проблеми:\n"

        for w in sorted(warnings, key=lambda x: x.get("percent", 0.0)):
            proj = escape_markdown(w.get("project", "Без назви"), version=2)
            zone = escape_markdown(w.get("zone", ""), version=2)
            percent = w.get("percent", 0.0)
            orders = w.get("orders", 0)
            project_avg_check = w.get("project_avg_check", 0.0)
            emoji = "🔴" if zone == "червона" else "🟡"
            msg += (
                f"❗️  *{proj}* {emoji} *{orders} зам.* —  *{percent:.1f}%*, серед. чек *{project_avg_check:.0f} грн* 😱 — зверни увагу‼️\n"
            )

        msg += "🚨🚨🚨🚨🚨🚨\n"

    zone_values = {upsell_zone, avg_check_zone, speed_zone}
    if any("червона" in z for z in zone_values):
        msg += "\n🔄 Ми віримо в тебе! Виправишся і піднімеш показники! 💪✨\n"
    elif any("жовта" in z for z in zone_values):
        msg += "\n⚠️ Не зупиняйся! Трохи зусиль — і буде зелена зона! 🌱🔥\n"
    else:
        msg += "\n🌟 Молодець! Тримаєш позитивну динаміку, так тримати! 💪🔥\n"

    return msg, ""

def build_warning_line_for_user(initials: str, user_stats: dict) -> str:
    initials_esc = escape_markdown(initials, version=2)
    percent = user_stats.get("orders_with_resale_percent")
    avg_check = user_stats.get("avg_check")
    orders_count = user_stats.get("orders_total")

    percent_str = f"{percent:.1f}%" if percent is not None else "—"
    avg_check_str = f"{avg_check:.2f} грн" if avg_check is not None else "—"
    orders_count_str = str(orders_count) if orders_count is not None else "—"

    return (
        f"🔠 Ініціали: {initials_esc}\n"
        f"📦 Загалом замовлень: {orders_count_str}\n\n"
        f"📈 Допродажі: {percent_str}\n"
        f"💰 Середній чек: {avg_check_str}"
    )

def build_warnings_by_projects(projects: list[dict], norms: dict) -> list[dict]:
    """
    Возвращает список словарей с предупреждениями по проектам,
    где процент допродаж в жёлтой или красной зоне.
    """
    warnings = []

    for proj in projects:
        name = proj.get("name", "Без назви")
        percent = proj.get("upsell_percent", 0.0)
        orders = proj.get("orders", 0)  # ✅ фикс
        project_avg_check = proj.get("avg_check", 0.0)
        zone, _ = get_zone_and_emoji("upsell", percent, norms)

        if zone in ("жовта", "червона"):
            warnings.append({
                "project": name,
                "orders": orders,
                "zone": zone,
                "percent": percent,
                "project_avg_check": project_avg_check
            })

    return warnings

async def scheduled_broadcast(context: ContextTypes.DEFAULT_TYPE):
    global last_broadcast_time

    now = datetime.now()
    if last_broadcast_time and now - last_broadcast_time < timedelta(minutes=55):
        logging.info("⏱ Авторассылка недавно уже выполнялась, пропускаем.")
        return

    try:
        class DummyMessage:
            async def reply_text(self, *args, **kwargs):
                return None

        class DummyUpdate:
            def __init__(self):
                self.message = DummyMessage()

        dummy_update = DummyUpdate()

        initials_input = "ВСІМ"
        await broadcast_with_file_management(dummy_update, context, initials_input)
        last_broadcast_time = now
        logging.info("✅ Авторассылка успешно выполнена")

    except Exception as e:
        error_msg = f"[🕓 scheduled_broadcast] Ошибка при авторассылке: {e}"
        logging.error(error_msg)
        try:
            await context.bot.send_message(ERROR_CHANNEL_ID, f"❗ Помилка авторассилки:\n{e}")
        except Exception:
            pass

def get_active_initials_from_calls(csv_path: Path, active_minutes_threshold=80) -> set[str]:
    now = datetime.now()
    active_initials = set()

    wait_time = 0
    while csv_path.exists() and csv_path.stat().st_size == 0 and wait_time < 180:
        # logging.info(f"⏳ Очікуємо завершення завантаження CSV... {wait_time} сек")
        time.sleep(1)
        wait_time += 1

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        logging.error("❌ CSV не був завантажений або порожній.")
        return active_initials

    try:
        time.sleep(1)

        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = list(reader)

            if rows:
                for row in rows:
                    raw_name = row.get("employee name", "").strip()
                    if not raw_name:
                        continue

                    initials = raw_name[:2].upper()

                    try:
                        call_time = datetime.strptime(row["date"], "%H:%M %d-%m-%Y")
                        if call_time.date() == now.date():
                            minutes_diff = (now - call_time).total_seconds() / 60
                            if 0 <= minutes_diff <= active_minutes_threshold:
                                active_initials.add(initials)
                    except Exception as e:
                        # logging.warning(f"⚠️ Не вдалося обробити дату: {row.get('date')} — {e}")
                        continue
    except Exception as e:
        logging.error(f"❌ Помилка читання CSV: {e}")

    return active_initials

def inject_speed_from_calls(new_data: dict, csv_path: Path) -> None:
    from collections import defaultdict

    call_times_by_initials = defaultdict(list)
    now = datetime.now()

    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                raw_name = row.get("employee name", "").strip()
                if not raw_name or len(raw_name) < 2:
                    continue

                initials = raw_name[:2].upper()

                try:
                    call_time = datetime.strptime(row["date"], "%H:%M %d-%m-%Y")
                    if call_time.date() == now.date():
                        call_times_by_initials[initials].append(call_time)
                except:
                    continue
    except Exception as e:
        logging.error(f"❌ Помилка при аналізі CSV для швидкості: {e}")
        return

    # ⏱ Для каждого сотрудника считаем часы
    for initials, calls in call_times_by_initials.items():
        calls.sort()
        work_minutes = 0
        block_start = calls[0]

        for i in range(1, len(calls)):
            diff = (calls[i] - calls[i - 1]).total_seconds() / 60
            if diff <= 80:
                continue
            else:
                block_end = calls[i - 1]
                block_duration = (block_end - block_start).total_seconds() / 60
                work_minutes += block_duration
                block_start = calls[i]

        # Последний блок
        block_end = calls[-1]
        last_block_duration = (block_end - block_start).total_seconds() / 60
        work_minutes += last_block_duration

        hours = work_minutes / 60
        if hours > 0 and initials in new_data:
            orders = new_data[initials].get("orders_total", 0)
            speed = orders / hours if hours else 0.0
            new_data[initials]["speed"] = round(speed, 2)

def inject_old_speed_from_calls_by_json_time(old_data: dict, csv_path: Path, old_json_file: Path) -> None:
    from collections import defaultdict
    import os

    try:
        cutoff_time = datetime.fromtimestamp(old_json_file.stat().st_mtime)
    except Exception as e:
        logging.error(f"❌ Не вдалося отримати час створення JSON: {e}")
        return

    call_times_by_initials = defaultdict(list)

    try:
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                raw_name = row.get("employee name", "").strip()
                if not raw_name or len(raw_name) < 2:
                    continue

                initials = raw_name[:2].upper()
                try:
                    call_time = datetime.strptime(row["date"], "%H:%M %d-%m-%Y")
                    if call_time < cutoff_time and call_time.date() == cutoff_time.date():
                        call_times_by_initials[initials].append(call_time)
                except Exception:
                    continue
    except Exception as e:
        logging.error(f"❌ Помилка читання CSV: {e}")
        return

    for initials, calls in call_times_by_initials.items():
        calls.sort()
        work_minutes = 0
        block_start = calls[0]

        for i in range(1, len(calls)):
            diff = (calls[i] - calls[i - 1]).total_seconds() / 60
            if diff <= 80:
                continue
            else:
                block_end = calls[i - 1]
                work_minutes += (block_end - block_start).total_seconds() / 60
                block_start = calls[i]

        work_minutes += (calls[-1] - block_start).total_seconds() / 60
        hours = work_minutes / 60

        if hours > 0 and initials in old_data:
            orders = old_data[initials].get("orders_total", 0)
            speed = orders / hours
            old_data[initials]["speed"] = round(speed, 2)

async def broadcast_with_file_management(update: Update, context: ContextTypes.DEFAULT_TYPE, initials_input: str):
    old_file = get_today_file(FOLDER_OLD)

    if old_file:
        raw_old_data = load_json(old_file)
        old_data = adapt_new_format(raw_old_data)
    else:
        old_data = {}

    norms = load_norms()
    users = load_users()
    initials_list = [i.strip().upper() for i in initials_input.split()]

    try:
        await update.message.reply_text("📊 Завантаження JSON статистики...")
        json_path = await fetch_json_data()
        if not json_path:
            await update.message.reply_text("❌ Не вдалося завантажити JSON статистику.")
            return

        raw_new_data = load_json(json_path)
        new_data = adapt_new_format(raw_new_data)

        await update.message.reply_text("🕐 Завантаження дзвінків з Binotel... (1–2 хвилини)")
        start_time = time.time()
        csv_path = fetch_via_playwright()
        duration = time.time() - start_time

        if not csv_path:
            await update.message.reply_text("❌ Не вдалося завантажити файл дзвінків з Binotel.")
            return

        inject_speed_from_calls(new_data, csv_path)

        if old_file and old_data:
            inject_old_speed_from_calls_by_json_time(old_data, csv_path, old_file)

        await update.message.reply_text(f"✅ Файл дзвінків отримано за {duration:.1f} сек.")

        active_initials = get_active_initials_from_calls(csv_path, active_minutes_threshold=70)
        csv_path.unlink(missing_ok=True)

        await update.message.reply_text(f"🎧 Активні ініціали: {', '.join(active_initials) or 'немає'}")

        if not active_initials:
            await update.message.reply_text("🚫 Не знайдено жодного активного співробітника — розсилка відмінена.")
            return

        if "ВСЕМ" in initials_list or "ВСІМ" in initials_list:
            filtered_users = [u for u in users if u.get("initials", "").upper() in active_initials]
        else:
            filtered_users = [
                u for u in users
                if u.get("initials", "").upper() in initials_list and u.get("initials", "").upper() in active_initials
            ]

        target_users = [
            u for u in filtered_users if u.get("user_id") not in [REPORT_CHANNEL_ID, ERROR_CHANNEL_ID]
        ]

        if not target_users:
            await update.message.reply_text("🚫 Немає користувачів з дзвінками сьогодні.")
            return

    except Exception as e:
        error_text = f"❌ Помилка при перевірці дзвінків Binotel або статистики: {e}"
        logging.error(error_text)
        await notify_admins(context, error_text)
        await update.message.reply_text("⚠️ Сталася помилка при перевірці даних. Розсилка відмінена.")
        return

    def is_zone_bad(zone):
        return zone in ("червона", "жовта")

    # Отправка индивидуальных сообщений операторам
    for user in target_users:
        initials = user.get("initials", "").upper()
        old_metrics = old_data.get(initials, {})
        new_metrics = new_data.get(initials, {})

        if not new_metrics or new_metrics.get("orders_total", 0) == 0:
            logging.info(f"Пропущено: {initials} — немає статистики або 0 замовлень")
            continue

        if old_metrics:
            keys_to_compare = ["orders_total", "upsell_percent", "avg_bill"]
            has_changes = any(
                round(old_metrics.get(k, 0), 1) != round(new_metrics.get(k, 0), 1)
                for k in keys_to_compare
            )
            if not has_changes:
                logging.info(f"Пропущено: {initials} — показники не змінилися")
                continue

        projects = new_metrics.get("projects", [])
        warnings = build_warnings_by_projects(projects, norms)

        msg_text, _ = generate_operator_message(
            user, old_metrics, new_metrics, warnings,
            old_file_exists=bool(old_data), norms=norms
        )

        try:
            await message_queue.send(
                context.bot,
                chat_id=user.get("user_id"),
                text=msg_text,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logging.error(f"Ошибка отправки пользователю {initials}: {e}")

    # Формируем отчёт для канала в новом формате
    report_data = []
    total_users_count = 0

    for user in target_users:
        initials = user.get("initials", "").upper()
        old_metrics = old_data.get(initials, {})
        new_metrics = new_data.get(initials, {})

        if not new_metrics or new_metrics.get("orders_total", 0) == 0:
            continue

        projects = new_metrics.get("projects", [])
        changed_projects = []

        for proj in projects:
            proj_name = proj.get("name", "Без проекта")
            upsell_new = proj.get("upsell_percent", 0.0)

            # Ищем старое значение upsell
            upsell_old = None
            if old_metrics:
                for old_proj in old_metrics.get("projects", []):
                    if old_proj.get("name") == proj_name:
                        upsell_old = old_proj.get("upsell_percent", None)
                        break

            change = None
            if upsell_old is not None:
                if abs(upsell_new - upsell_old) >= 0.01:
                    if upsell_new > upsell_old and upsell_new < 99.0:
                        change = "up"
                    elif upsell_new < upsell_old:
                        change = "down"
            else:
                if upsell_new < 75:
                    change = "bad"

            if change:
                changed_projects.append({
                    "name": proj_name,
                    "upsell": upsell_new,
                    "change": change
                })

        if changed_projects:
            total_users_count += 1
            report_data.append({
                "initials": initials,
                "projects": changed_projects
            })

    if REPORT_CHANNEL_ID and report_data:
        header = f"*🎯Загалом користувачів із падінням: {total_users_count}*\n\n"
        lines = [header, "*Зміни у показниках:*"]

        for user_block in report_data:
            initials = user_block["initials"]
            lines.append(f"*{initials}* —")  # инициалы жирным
            for proj in user_block["projects"]:
                upsell = proj["upsell"]
                if proj["change"] == "up":
                    symbol = "✅ росте 🚀"
                elif proj["change"] == "down":
                    symbol = "‼️ падає🔻"
                elif proj["change"] == "bad":
                    symbol = "⚠️ низький показник"
                else:
                    symbol = ""
                lines.append(f"    {proj['name']} — {upsell:.1f}% {symbol}".rstrip())
            lines.append("")  # ⏎ пустая строка между блоками операторов

        report_text = "\n".join(lines)

        try:
            await message_queue.send(
                context.bot,
                chat_id=REPORT_CHANNEL_ID,
                text=report_text,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logging.error(f"❌Ошибка отправки отчёта в канал: {e}")

    try:
        if old_file:
            old_file.unlink()
        move_file(json_path, FOLDER_OLD)
    except Exception as e:
        error_text = f"❌ Ошибка обновления файлов після розсилки: {e}"
        logging.error(error_text)
        await notify_admins(context, error_text)

    await update.message.reply_text("✅ Розсилка виконана і файли оновлено.")

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat

    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    # Исключаем каналы отчёта и ошибок
    if chat_id in (REPORT_CHANNEL_ID, ERROR_CHANNEL_ID):
        return

    title = chat.title or "Без названия"

    initials_match = re.search(r"\(([A-Za-zА-Яа-я]{2})\)", title)
    initials = initials_match.group(1).upper() if initials_match else str(chat_id)

    users = load_users()
    exists = any(g.get("user_id") == chat_id for g in users)

    if not exists:
        users.append({
            "initials": initials,
            "tag": "",
            "user_id": chat_id
        })
        save_users(users)

async def my_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_member_update = update.my_chat_member
    chat = chat_member_update.chat
    new_status = chat_member_update.new_chat_member.status

    if new_status in ("kicked", "left"):
        users = load_users()
        users = [u for u in users if u.get("user_id") != chat.id]
        save_users(users)
        logging.info(f"Группа {chat.title} ({chat.id}) удалена из списка, т.к. бот был выгнан или вышел.")
        
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("❌ Произошла ошибка:", exc_info=context.error)
    try:
        await context.bot.send_message(
            chat_id=ERROR_CHANNEL_ID,
            text=f"🚨 Произошла ошибка:\n<pre>{html.escape(str(context.error))}</pre>",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка при отправке сообщения об ошибке: {e}")

async def test_auto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("🔁 Тестовий запуск авторассилки...")
    await scheduled_broadcast(context)

# rate_limiter.py или внизу main файла
import time
import logging
from asyncio import Queue, create_task, sleep
from collections import defaultdict

class MessageQueue:
    def __init__(self, max_per_sec=10, parallel_limit=10):
        self.queue = Queue()
        self.max_per_sec = max_per_sec
        self.parallel_limit = parallel_limit
        self.last_sent = defaultdict(lambda: 0)
        self.active_tasks = set()

    async def start(self):
        while True:
            if len(self.active_tasks) < self.parallel_limit:
                task = await self.queue.get()
                t = create_task(self._safe_send(task))
                self.active_tasks.add(t)
                t.add_done_callback(self.active_tasks.discard)
            await sleep(0.05)  # проверка каждые 50мс

    async def _safe_send(self, task_data):
        bot, chat_id, text, parse_mode = task_data
        now = time.time()
        delay = max(1.0 - (now - self.last_sent[chat_id]), 0.0)
        await sleep(delay)
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        except Exception as e:
            logging.error(f"❌ Ошибка при отправке сообщения в {chat_id}: {e}")
        self.last_sent[chat_id] = time.time()

    async def send(self, bot, chat_id, text, parse_mode=None):
        await self.queue.put((bot, chat_id, text, parse_mode))

# Глобальная очередь сообщений
message_queue = MessageQueue(max_per_sec=15, parallel_limit=15)

# 💡 Глобальная переменная приложения
app_instance = None

def make_context(app):
    return CallbackContext(application=app)

async def send_stats_report(reply_func, user_id):
    if not is_admin(user_id):
        await reply_func("❌ Доступ только для администраторов.")
        return

    try:
        await reply_func("🔄 Загрузка актуальной статистики...")

        json_path = await fetch_json_data()
        if not json_path:
            await reply_func("❌ Не удалось загрузить статистику.")
            return
        raw_stat = load_json(json_path)
        stat_data = adapt_new_format(raw_stat)

        # 📦 Общая стата по проектам
        general_stats = {}
        for p in raw_stat.get("general_projects_stats", []):
            name = p.get("name")
            stats = p.get("stats", {})
            general_stats[name] = {
                "orders_total": stats.get("total_orders", 0),
                "avg_percent": stats.get("orders_with_resale_percent", 0)
            }

        # 📁 Старая стата
        old_stat_data = {}
        old_json_path = find_latest_old_json()
        if old_json_path:
            old_raw = load_json(old_json_path)
            old_stat_data = adapt_new_format(old_raw)

        await reply_func("📞 Загрузка звонков Binotel...")
        start_time = time.time()
        csv_path = fetch_via_playwright()
        if not csv_path:
            await reply_func("❌ Не удалось загрузить звонки с Binotel.")
            return
        duration = time.time() - start_time
        inject_speed_from_calls(stat_data, csv_path)
        active_initials = get_active_initials_from_calls(csv_path)
        Path(csv_path).unlink(missing_ok=True)

        await reply_func(
            f"✅ Звонки загружены за {duration:.1f} сек.\nАктивные: {', '.join(active_initials) or 'нет'}"
        )

        if not active_initials:
            await reply_func("🚫 Нет активных операторов, звіт не сформовано.")
            return

        filtered_stat = {i: d for i, d in stat_data.items() if i in active_initials}
        projects_info = {}

        # 📊 Сравнение с прошлыми значениями
        for initials, metrics in filtered_stat.items():
            old_projects_by_name = {
                p.get("name", "Без проекта"): p
                for p in old_stat_data.get(initials, {}).get("projects", [])
            }

            for proj in metrics.get("projects", []):
                name = proj.get("name", "Без проекта")
                upsell = proj.get("upsell_percent", 0.0)
                orders = proj.get("orders", 0)
                #print(f"DEBUG: {initials=} {name=} {upsell=} {orders=}")  # вот здесь

                old_proj = old_projects_by_name.get(name)
                old_upsell = old_proj.get("upsell_percent") if old_proj else None

                # 💡 Учитываем точность до десятых при сравнении upsell
                if old_upsell is not None and abs(upsell - old_upsell) < 0.01:
                    continue  # нет изменений

                if name not in projects_info:
                    projects_info[name] = {
                        "managers": {}
                    }

                # 💡 Сохраняем точные значения без агрегации
                projects_info[name]["managers"][initials] = {
                    "upsell": upsell,
                    "orders": orders,
                    "old_upsell": old_upsell
                }

        # 🧹 Удаляем проекты без изменений
        projects_info = {name: data for name, data in projects_info.items() if data["managers"]}

        if not projects_info:
            await reply_func("📭 Немає змін у показниках активних операторів.")
            return

        # 📥 Добавляем общую статику
        for name, data in projects_info.items():
            data["orders_total"] = general_stats.get(name, {}).get("orders_total", 0)
            data["avg_percent"] = general_stats.get(name, {}).get("avg_percent", 0.0)

        # 📋 Сортировка проектов по среднему upsell
        sorted_projects = sorted(
            projects_info.items(),
            key=lambda x: sum(m["upsell"] for m in x[1]["managers"].values()) / len(x[1]["managers"])
        )

        chunks = [sorted_projects[i:i + 5] for i in range(0, len(sorted_projects), 5)]

        for chunk in chunks:
            lines = []
            for proj_name, info in chunk:
                proj_escaped = escape_markdown(proj_name, version=2)

                managers = info["managers"]
                total_orders = info.get("orders_total", 0)
                avg_percent = info.get("avg_percent", 0.0)
                avg_warn = " ‼️⚠️" if avg_percent < 80 else ""

                lines.append(f"👉 *{proj_escaped}* {avg_percent:.1f}% {total_orders} зам.{avg_warn}")

                sorted_mgrs = sorted(managers.items(), key=lambda x: x[1]["upsell"])
                mgr_lines = []
                for init, data in sorted_mgrs:
                    upsell = data["upsell"]
                    orders = data["orders"]
                    old_upsell = data["old_upsell"]
                    falling = old_upsell is not None and upsell < old_upsell
                    warn = " ‼️портит🔻" if falling else ""
                    mark = "" if upsell >= 75 else "❗️"

                    init_escaped = escape_markdown(init, version=2)
                    line = f"{init_escaped} - {upsell:.1f}%{mark} {orders}з{warn}"
                    mgr_lines.append(line)

                for i in range(0, len(mgr_lines), 2):
                    lines.append("   ".join(mgr_lines[i:i + 2]))

                lines.append("")  # ← добавлена пустая строка между проектами

            text = "\n".join(lines).strip()
            await reply_func(text, parse_mode="Markdown")

        Path(json_path).unlink(missing_ok=True)

    except Exception as e:
        logging.error(f"❌ Ошибка в отправке отчёта: {e}")
        await reply_func(f"❌ Ошибка при выполнении: {e}")
        
async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_stats_report(update.message, context)

# Планировщик авторассылки (экспортируемая функция)
def setup_scheduler(app):
    from apscheduler.schedulers.background import BackgroundScheduler
    import asyncio

    global last_broadcast_time
    last_broadcast_time = None  # Инициализация глобальной переменной

    scheduler = BackgroundScheduler(timezone="Europe/Kyiv")
    loop = asyncio.get_event_loop()

    def job_func():
        try:
            ctx = make_context(app)
            asyncio.run_coroutine_threadsafe(scheduled_broadcast(ctx), loop)
        except Exception as e:
            logging.error(f"[🕓 scheduled_broadcast] Ошибка при авторассылке: {e}")

    for hour in range(9, 21):
        scheduler.add_job(
            job_func,
            trigger='cron',
            hour=hour,
            minute=2,
            id=f'broadcast_{hour:02d}'
        )

    scheduler.start()

# bot3/statbot_mainBinotel20.py

import os
import html
import logging
import asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, filters
)
from fastapi import Request


application = ApplicationBuilder().token(BOT_TOKEN).build()

application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("reload_norms", reload_norms_command))
application.add_handler(CommandHandler("test_auto", test_auto_command))
application.add_handler(CommandHandler("debug", debug_command))
application.add_handler(CallbackQueryHandler(button_handler))
application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, text_handler))
application.add_handler(MessageHandler(filters.ChatType.GROUPS, handle_group_message))
application.add_handler(ChatMemberHandler(my_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))
application.add_error_handler(error_handler)

# === Экспортируемые функции для общего FastAPI-приложения ===

async def handle_startup():
    # Загрузим нормы и подменим глобальную переменную
    norms_loaded = load_norms(NORMS_FILE)
    import bot3.statbot_mainBinotel20
    bot3.statbot_mainBinotel20.norms = norms_loaded

    await application.initialize()
    await application.start()

    # ✅ Устанавливаем Webhook
    await application.bot.set_webhook(WEBHOOK_URL)

    asyncio.create_task(message_queue.start())
    setup_scheduler(application)

    if ERROR_CHANNEL_ID:
        await application.bot.send_message(ERROR_CHANNEL_ID, "✅ bot3 запущен")

async def handle_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

async def handle_shutdown():
    await application.stop()
    await application.shutdown()
