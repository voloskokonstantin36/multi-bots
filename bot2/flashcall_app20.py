import os, json, csv, time, re
from dotenv import load_dotenv
import html
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path 
from datetime import time

import shutil
import traceback
import pytz
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

# Корень проекта
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=ROOT_DIR / ".env")

BOT_TOKEN = os.getenv("BOT2_TOKEN")
ERROR_CHANNEL = int(os.getenv("ERROR_CHANNEL_ID"))

# Пути к файлам и папкам
BASE_DIR = Path(__file__).resolve().parent  # Папка, где находится текущий файл
CONFIG = BASE_DIR / "config.json"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

from telegram.constants import ParseMode

async def safe_send(bot, chat_id, text):
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"Ошибка отправки сообщения: {e}")

def load_config():
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)

def save_config(data):
    with CONFIG.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def escape_markdown(text: str) -> str:
    escape_chars = r"\_*[]()~`>#+-=|{}.!<>"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", str(text))

def escape_user_tag(text: str) -> str:
    """Экранирует Markdown-символы только в имени/теге пользователя, не затрагивая остальной текст."""
    escape_chars = r"\_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", str(text))
    
from telegram.constants import ChatType

def is_allowed_chat(chat, context):
    allowed_ids = set(int(k) for k in context.bot_data.get("projects", {}).keys())
    report_chs = {
        context.bot_data.get("report_channel"),
        context.bot_data.get("manager_report_channel"),
        context.bot_data.get("leader_report_channel"),
    }
    # Уберём None из report_chs, если есть
    report_chs = {ch for ch in report_chs if ch is not None}

    return (
        chat.type == ChatType.PRIVATE or
        chat.id in allowed_ids or
        chat.id in report_chs
    )

def is_allowed_menu_chat(chat, context):
    return chat.type == ChatType.PRIVATE or chat.id in {
        context.bot_data.get("report_channel"),
        context.bot_data.get("leader_report_channel"),
    }

def load_df(day: date):
    f = DATA_DIR / f"{day.isoformat()}.csv"
    if not f.exists():
        return pd.DataFrame(columns=["timestamp", "chat_id", "user_id", "message"])
    df = pd.read_csv(f)
    return df

def load_multiple_days_df(days_count=3):
    frames = []
    ttn_pattern = re.compile(r"[12456]\d{9,}")  # паттерн ТТН

    for i in range(days_count):
        day = date.today() - timedelta(days=i)
        try:
            df = load_df(day)
            if not df.empty:
                df = df[df["message"].astype(str).str.contains(ttn_pattern, na=False)]
                if not df.empty:
                    frames.append(df)
        except Exception as e:
            print(f"⚠️ Ошибка при загрузке данных за {day}: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def get_today_file():
    today_file = DATA_DIR / f"{date.today().isoformat()}.csv"
    if today_file.exists():
        # Файл на сегодня уже есть — используем его
        return today_file

    # Файла с сегодняшней датой нет — ищем старый файл без даты (например, messages_current.csv)
    # или просто любой другой CSV в папке, кроме сегодняшнего
    files = list(DATA_DIR.glob("*.csv"))
    old_file = None
    for f in files:
        if f != today_file:
            old_file = f
            break

    if old_file:
        # Копируем содержимое старого файла в новый с сегодняшней датой
        shutil.copy2(old_file, today_file)
        # Удаляем старый файл
        old_file.unlink()
        print(f"Старый файл {old_file} перенесён в {today_file}")
    else:
        # Старого файла нет — создаём новый (пустой)
        with open(today_file, "w", encoding="utf-8", newline="") as f:
            pass
        print(f"Создан новый файл {today_file}")

    return today_file

def save_message_to_file(message):
    if not message.text:
        return
    if not re.search(r"[0-9]\d{11,13}", message.text):
        return

    try:
        f = get_today_file()

        row = {
            "timestamp": message.date.strftime("%Y-%m-%d %H:%M:%S"),
            "chat_id": message.chat.id,
            "user_id": str(message.from_user.id) if message.from_user else "unknown",
            "message": html.escape(message.text),
        }

        file_exists = f.exists() and f.stat().st_size > 0
        with open(f, "a", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print(f"✅ Сообщение сохранено в {f}")

    except Exception as e:
        print(f"❌ Ошибка при сохранении сообщения: {e}")

async def resolve_user_name(bot, user_id: str):
    try:
        user = await bot.get_chat(int(user_id))
        if user.username:
            return f"@{user.username}"
        full = (user.first_name or "") + (" " + user.last_name if user.last_name else "")
        return escape_user_tag(full.strip() or f"User{user_id}")
    except:
        return escape_user_tag(f"@{user.username}")

async def format_project_report(df, bot_data, bot=None):
    today = date.today()
    today_str = today.strftime("%d.%m")
    out = [f"<b>Знижки на {today_str}</b>\n"]
    total = 0
    unknowns = []

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df_today = df[df["timestamp"].dt.date == today]

    projects = bot_data.get("projects", {})
    norms = bot_data.get("norms", {})
    users = bot_data.get("users", {})

    for cid, proj in projects.items():
        g = df_today[df_today["chat_id"] == int(cid)]
        g = g[g["message"].astype(str).str.contains(r"[12456]\d{9,}", na=False)]

        if g.empty:
            out.append(f"👉 <b>{proj}: 0 ‼️</b>")
            out.append(f"🎯норма -- {norms.get(proj, 0)}")
            out.append("🚩по операторам: нет данных\n")
            continue

        ini_map = g["user_id"].astype(str).map(lambda u: users.get(u, None))
        vc = ini_map.value_counts()
        count = vc.sum()
        norm = norms.get(proj, 0)
        flag = "‼️" if count < norm else ""
        out.append(f"👉 <b>{proj}: {count} {flag}</b>")
        out.append(f"🎯норма -- {norm}")
        ops = ", ".join(f"{cnt}{ini}" for ini, cnt in vc.items() if ini)
        out.append(f"🚩по операторам: {ops or 'нет данных'}\n")
        total += count

        unknown_ids = set(g["user_id"].astype(str)) - set(users.keys())
        for uid in unknown_ids:
            unknowns.append((uid, proj))

    out.append(f"ИТОГО по всем проектам: {total}")

    # 🔻 Блок "Без инициалов"
    if unknowns and bot:
        out.append("\n❓ Без инициалов:")
        for uid, proj in unknowns:
            try:
                user = await bot.get_chat(int(uid))
                if user.username:
                    name = f"@{user.username}"
                else:
                    name = html.escape(user.full_name)
            except Exception:
                name = "неизвестно"
            out.append(f"🟥 {uid} {name} ({proj})")

    return "\n".join(out), unknowns

    
def format_operator_report(df, bot_data):
    today = date.today()
    today_str = today.strftime("%d.%m")
    first_day = today.replace(day=1)
    users = bot_data.get("users", {})

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df_month = df[(df["timestamp"].dt.date >= first_day) & (df["timestamp"].dt.date <= today)]
    df_today = df[df["timestamp"].dt.date == today]

    month_counts = df_month["user_id"].astype(str).value_counts()
    today_counts = df_today["user_id"].astype(str).value_counts()

    stats = []
    for uid, ini in users.items():
        if not ini or len(ini) != 2:
            continue
        month_total = month_counts.get(uid, 0)
        today_total = today_counts.get(uid, 0)
        bonus = "💰💵" if month_total >= 100 else ""
        stats.append((today_total, f"🎯 <b>{ini}</b> — {today_total} / {month_total} {bonus}"))

    stats.sort(reverse=True, key=lambda x: x[0])
    lines = [f"<b>Знижки на {today_str}</b>\n"]
    lines.extend([line for _, line in stats])
    return "\n".join(lines) if len(lines) > 1 else "Нет данных по операторам."

async def format_leader_report(df, bot_data, comment=None):
    text, unknowns = await format_project_report(df, bot_data)  # Добавлено await
    if comment and comment.strip() and comment.strip() != "-":
        comment_escaped = html.escape(comment.strip()) # Можно добавить escape HTML, если нужно
        text += f"\n\n💬 Комментарий:\n{comment_escaped}"
    return text

async def send_report(bot, bot_data, chat_id: int, report_type: str = None, comment=None, send_all=False):
    print(f"send_report: bot type = {type(bot)}, bot_data type = {type(bot_data)}")
    df = load_multiple_days_df(3)
    if df.empty:
        await safe_send(bot, chat_id, "Нет данных для отчёта за последние дни.")
        return

    try:
        if send_all:
            text_main, _ = await format_project_report(df, bot_data, bot)
            text_manager = format_operator_report(df, bot_data)
            text_leader = await format_leader_report(df, bot_data, comment)

            await safe_send(bot, chat_id, "*Основной отчёт:*\n" + text_main)
            await safe_send(bot, chat_id, "*Отчёт менеджеров:*\n" + text_manager)
            await safe_send(bot, chat_id, "*Отчёт руководителю:*\n" + text_leader)
        else:
            if report_type == "main":
                text, _ = await format_project_report(df, bot_data, bot)
            elif report_type == "manager":
                text = format_operator_report(df, bot_data)
            elif report_type == "leader":
                text = await format_leader_report(df, bot_data, comment)
            else:
                text = "Неверный тип отчёта."
            await safe_send(bot, chat_id, text)
    except Exception as e:
        print(f"Ошибка при отправке отчёта: {e}")
        await notify_admin(bot, bot_data, f"Ошибка send_report: {e}")

async def notify_admin(bot, bot_data, text: str):
    error_channel = bot_data.get("error_channel")
    if error_channel:
        try:
            await safe_send(bot, error_channel, f"⚠️ Ошибка:\n{text}")
        except:
            pass

async def scheduled_report(bot, bot_data):
    try:
        print(f"[{datetime.now()}] ⏰ scheduled_report")
        await send_report(bot, bot_data, bot_data["report_channel"], send_all=True)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[{datetime.now()}] ❌ Ошибка автоотчёта: {tb}")
        await safe_send(bot, bot_data["error_channel"], f"Ошибка автоотчёта:\n{tb}")

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Меню отчётов", callback_data="report_menu")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="users_menu")],
        [InlineKeyboardButton("🏷️ Нормы проектов", callback_data="norms_menu")],
        [InlineKeyboardButton("⏰ Изменить время отчёта", callback_data="set_time")],
        [InlineKeyboardButton("🕵 Проверить пропущенные сообщения", callback_data="check_missed")],
        [InlineKeyboardButton("⚙️ Настройки каналов", callback_data="channels_menu")],
        [InlineKeyboardButton("🚪 Выход", callback_data="exit")]
    ])

def report_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Отправить отчёт в основной канал", callback_data="send_report_main")],
        [InlineKeyboardButton("📤 Отчёт по операторам", callback_data="send_report_manager")],
        [InlineKeyboardButton("📤 Отчёт руководителю (с комментарием)", callback_data="send_report_leader")],
        [InlineKeyboardButton("📤 Отправить все отчёты (с задержкой)", callback_data="send_report_all")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")]
    ])

async def users_menu_keyboard(bot_data, bot):
    keyboard = []
    USERS = bot_data.get("users", {})
    df_multi = load_multiple_days_df(3)
    known_user_ids = set(USERS.keys())
    unknown_user_ids = sorted(set(df_multi["user_id"].astype(str).unique()) - known_user_ids)

    for uid, ini in USERS.items():
        keyboard.append([InlineKeyboardButton(f"{html.escape(ini)} ({uid})", callback_data=f"edit_user:{uid}")])

    for uid in unknown_user_ids:
        try:
            user = await bot.get_chat(int(uid))
            name = f"@{user.username}" if user.username else html.escape(user.full_name)
        except:
            name = "неизвестно"
        keyboard.append([InlineKeyboardButton(f"🟥 {uid} {name}", callback_data=f"add_ini:{uid}")])

    keyboard.append([InlineKeyboardButton("➕ Добавить пользователя", callback_data="add_user")])
    keyboard.append([InlineKeyboardButton("🗑 Удалить пользователя", callback_data="del_user_menu")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)


def del_user_menu_keyboard(bot_data):
    keyboard = []
    for uid, ini in bot_data.get("users", {}).items():
        keyboard.append([InlineKeyboardButton(f"{ini} ({uid})", callback_data=f"del_user:{uid}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="users_menu")])
    return InlineKeyboardMarkup(keyboard)

def norms_menu_keyboard(bot_data):
    keyboard = []
    for proj in set(bot_data.get("projects", {}).values()):
        norm = bot_data.get("norms", {}).get(proj, 0)
        keyboard.append([InlineKeyboardButton(f"{proj}: {norm}", callback_data=f"edit_norm:{proj}")])
    keyboard.append([InlineKeyboardButton("➕ Добавить проект", callback_data="add_project")])
    keyboard.append([InlineKeyboardButton("🗑 Удалить проект", callback_data="del_project_menu")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(keyboard)

def del_project_menu_keyboard(bot_data):
    keyboard = []
    for cid, proj in bot_data.get("projects", {}).items():
        keyboard.append([InlineKeyboardButton(f"{proj} ({cid})", callback_data=f"del_project:{cid}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="norms_menu")])
    return InlineKeyboardMarkup(keyboard)

def channels_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Канал основного отчёта", callback_data="set_channel_report")],
        [InlineKeyboardButton("Канал менеджеров", callback_data="set_channel_manager")],
        [InlineKeyboardButton("Канал руководителя", callback_data="set_channel_leader")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="main_menu")]
    ])

async def check_missed_messages(app):
    from_date = date.today().replace(day=1)
    df = load_df(from_date)

    if df.empty:
        print("Нет сообщений за текущий месяц.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    recent = df[df["timestamp"].dt.date >= from_date]

    if recent.empty:
        print("Нет новых сообщений.")
        return

    print("🔎 Сохраняем все найденные сообщения с ТТН с начала месяца:")
    for _, row in recent.iterrows():
        # Фейковое сообщение, чтобы использовать `save_message_to_file`
        class FakeMessage:
            def __init__(self, row):
                self.text = row["message"]
                self.date = row["timestamp"]
                self.chat = type("chat", (), {"id": row["chat_id"]})()
                self.from_user = type("user", (), {"id": int(row["user_id"])})()

        msg = FakeMessage(row)
        save_message_to_file(msg)

    print("✅ Все сообщения с ТТН добавлены в файл.")


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat = query.message.chat
    bot_data = context.bot_data
    chat_data = context.chat_data
    bot = context.bot
    await query.answer()

    if not is_allowed_menu_chat(chat, context):
        await query.answer("Меню доступно только в ЛС и разрешённых каналах.", show_alert=True)
        return

    data = query.data

    if data == "main_menu":
        await query.edit_message_text("Выберите действие:", reply_markup=main_menu_keyboard())

    elif data == "report_menu":
        await query.edit_message_text("Меню отчётов:", reply_markup=report_menu_keyboard())

    elif data == "send_report_main":
        await send_report(bot, bot_data, bot_data["report_channel"], "main")
        try:
            await query.edit_message_text("✅ Отчёт отправлен.", reply_markup=report_menu_keyboard())
        except:
            await query.answer("Отчёт отправлен.", show_alert=True)

    elif data == "send_report_manager":
        await send_report(bot, bot_data, bot_data["manager_report_channel"], "manager")
        try:
            await query.edit_message_text("✅ Отчёт менеджерам отправлен.", reply_markup=report_menu_keyboard())
        except:
            await query.answer("Отчёт отправлен.", show_alert=True)

    elif data == "send_report_leader":
        chat_data["state"] = "wait_leader_comment"
        await query.edit_message_text("Введите комментарий или '-' для отчёта без комментария:")

    elif data == "send_report_all":
        await send_report(bot, bot_data, bot_data["report_channel"], report_type="main")
        await send_report(bot, bot_data, bot_data["report_channel"], report_type="manager")
        await send_report(bot, bot_data, bot_data["report_channel"], report_type="leader", comment="-")
        try:
            await query.edit_message_text("✅ Все отчёты отправлены в report канал.", reply_markup=report_menu_keyboard())
        except:
            await query.answer("Отчёты отправлены.", show_alert=True)

    elif data == "users_menu":
        keyboard = await users_menu_keyboard(bot_data, bot)
        await query.edit_message_text("Меню пользователей:", reply_markup=keyboard)

    elif data.startswith("edit_user:"):
        uid = data.split(":")[1]
        chat_data["state"] = "edit_user"
        chat_data["edit_uid"] = uid
        ini = bot_data.get("users", {}).get(uid, uid)
        await query.edit_message_text(f"Введите новые инициалы для пользователя {ini}:")

    elif data.startswith("add_ini:"):
        uid = data.split(":")[1]
        chat_data["state"] = "add_user_ask_ini"
        chat_data["add_user_id"] = uid
        name = await resolve_user_name(bot, uid)
        await query.edit_message_text(f"Добавление инициалов для пользователя {uid} ({name}). Введите инициалы:")

    elif data == "add_user":
        chat_data["state"] = "add_user_ask_id"
        await query.edit_message_text("Введите user_id для нового пользователя:")

    elif data == "del_user_menu":
        keyboard = del_user_menu_keyboard(bot_data)
        await query.edit_message_text("Выберите пользователя для удаления:", reply_markup=keyboard)

    elif data.startswith("del_user:"):
        uid = data.split(":")[1]
        if uid in bot_data.get("users", {}):
            bot_data["users"].pop(uid)
            save_config(bot_data)
            keyboard = await users_menu_keyboard(bot_data, bot)
            await query.edit_message_text(f"✅ Пользователь {uid} удалён.", reply_markup=keyboard)
        else:
            keyboard = await users_menu_keyboard(bot_data, bot)
            await query.edit_message_text("Пользователь не найден.", reply_markup=keyboard)

    elif data == "norms_menu":
        keyboard = norms_menu_keyboard(bot_data)
        await query.edit_message_text("Меню норм:", reply_markup=keyboard)

    elif data.startswith("edit_norm:"):
        proj = data.split(":", 1)[1]
        chat_data["state"] = "edit_norm"
        chat_data["edit_proj"] = proj
        await query.edit_message_text(f"Введите новую норму для проекта {proj}:")

    elif data == "add_project":
        chat_data["state"] = "add_project_ask_chat"
        await query.edit_message_text("Введите chat_id проекта:")

    elif data == "del_project_menu":
        keyboard = del_project_menu_keyboard(bot_data)
        await query.edit_message_text("Выберите проект для удаления:", reply_markup=keyboard)

    elif data.startswith("del_project:"):
        cid = int(data.split(":")[1])
        projects = bot_data.get("projects", {})
        norms = bot_data.get("norms", {})
        if cid in projects:
            proj_name = projects.pop(cid)
            norms.pop(proj_name, None)
            save_config(bot_data)
            keyboard = norms_menu_keyboard(bot_data)
            await query.edit_message_text(f"✅ Проект {proj_name} удалён.", reply_markup=keyboard)
        else:
            keyboard = norms_menu_keyboard(bot_data)
            await query.edit_message_text("Проект не найден.", reply_markup=keyboard)

    elif data == "check_missed":
        await check_missed_messages(context.application)
        await query.edit_message_text("✅ Проверка завершена.", reply_markup=main_menu_keyboard())

    elif data == "channels_menu":
        keyboard = channels_menu_keyboard()
        await query.edit_message_text("Меню каналов:", reply_markup=keyboard)

    elif data == "set_channel_report":
        chat_data["state"] = "set_channel_report"
        await query.edit_message_text(f"Введите новый chat_id основного канала (текущий {bot_data.get('report_channel')}):")

    elif data == "set_channel_manager":
        chat_data["state"] = "set_channel_manager"
        await query.edit_message_text(f"Введите новый chat_id канала менеджеров (текущий {bot_data.get('manager_report_channel')}):")

    elif data == "set_channel_leader":
        chat_data["state"] = "set_channel_leader"
        await query.edit_message_text(f"Введите новый chat_id канала руководителя (текущий {bot_data.get('leader_report_channel')}):")

    elif data == "set_time":
        chat_data["state"] = "set_time"
        await query.edit_message_text(f"Введите новое время отчёта в формате HH:MM (текущее {bot_data.get('report_time')}):")

    elif data == "exit":
        await query.answer("Выход из меню.")
        await query.delete_message()

    else:
        await query.answer("Неизвестная команда.")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):       
    if not update.message:
        return
    
    message = update.message        
    chat = message.chat
    chat_data = context.chat_data
    bot_data = context.bot_data

    if not message or not is_allowed_chat(chat, context):
        print("Чат не разрешён или сообщения нет")
        return

    # Проверяем разные состояния для интерактивных сценариев:
    if chat_data.get("state") == "wait_leader_comment":
        comment = message.text.strip()
        await send_report(context.bot, bot_data, bot_data["leader_report_channel"], report_type="leader", comment=comment)
        chat_data.clear()
        await message.reply_text("✅ Отчёт с комментарием отправлен.", reply_markup=report_menu_keyboard())
        return

    if chat_data.get("state") == "add_user_ask_id":
        uid = message.text.strip()
        if uid in bot_data["users"]:
            await message.reply_text("Пользователь уже существует.")
        else:
            chat_data["add_user_id"] = uid
            chat_data["state"] = "add_user_ask_ini"
            await message.reply_text(f"Введите инициалы для пользователя {uid}:")
        return

    if chat_data.get("state") == "add_user_ask_ini":
        ini = message.text.strip()
        uid = chat_data.get("add_user_id")
        if uid:
            bot_data["users"][uid] = ini
            save_config(bot_data)
            keyboard = await users_menu_keyboard(bot_data, context.bot)
            await message.reply_text(f"✅ Пользователь добавлен: {ini} ({uid})", reply_markup=keyboard)
            chat_data.clear()
        return

    if chat_data.get("state") == "edit_user":
        ini = message.text.strip()
        uid = chat_data.get("edit_uid")
        if uid:
            bot_data["users"][uid] = ini
            save_config(bot_data)
            keyboard = await users_menu_keyboard(bot_data, context.bot)
            await message.reply_text(f"✅ Инициалы обновлены: {ini} ({uid})", reply_markup=keyboard)
            chat_data.clear()
        return

    if chat_data.get("state") == "edit_norm":
        val = message.text.strip()
        proj = chat_data.get("edit_proj")
        try:
            norm_val = int(val)
            bot_data["norms"][proj] = norm_val
            save_config(bot_data)
            keyboard = norms_menu_keyboard(bot_data)
            await message.reply_text(f"✅ Норма проекта {proj} обновлена: {norm_val}", reply_markup=keyboard)
            chat_data.clear()
        except:
            await message.reply_text("Введите число.")
        return

    if chat_data.get("state") == "add_project_ask_chat":
        try:
            cid = int(message.text.strip())
            chat_data["new_project_chat_id"] = cid
            chat_data["state"] = "add_project_ask_name"
            await message.reply_text("Введите название проекта:")
        except:
            await message.reply_text("Введите корректный chat_id.")
        return

    if chat_data.get("state") == "add_project_ask_name":
        name = message.text.strip()
        cid = chat_data.get("new_project_chat_id")
        if cid:
            bot_data["projects"][cid] = name
            save_config(bot_data)
            keyboard = norms_menu_keyboard(bot_data)
            await message.reply_text(f"✅ Проект добавлен: {name} ({cid})", reply_markup=keyboard)
            chat_data.clear()
        return

    if chat_data.get("state") == "set_channel_report":
        try:
            new_id = int(message.text.strip())
            bot_data["report_channel"] = new_id
            save_config(bot_data)
            await message.reply_text(f"✅ Основной канал обновлён: {new_id}", reply_markup=channels_menu_keyboard())
            chat_data.clear()
        except:
            await message.reply_text("Введите корректный chat_id.")
        return

    if chat_data.get("state") == "set_channel_manager":
        try:
            new_id = int(message.text.strip())
            bot_data["manager_report_channel"] = new_id
            save_config(bot_data)
            await message.reply_text(f"✅ Канал менеджеров обновлён: {new_id}", reply_markup=channels_menu_keyboard())
            chat_data.clear()
        except:
            await message.reply_text("Введите корректный chat_id.")
        return

    if chat_data.get("state") == "set_channel_leader":
        try:
            new_id = int(message.text.strip())
            bot_data["leader_report_channel"] = new_id
            save_config(bot_data)
            await message.reply_text(f"✅ Канал руководителя обновлён: {new_id}", reply_markup=channels_menu_keyboard())
            chat_data.clear()
        except:
            await message.reply_text("Введите корректный chat_id.")
        return

    if chat_data.get("state") == "set_time":
        val = message.text.strip()
        try:
            hh, mm = map(int, val.split(":"))
            if 0 <= hh < 24 and 0 <= mm < 60:
                bot_data["report_time"] = f"{hh:02}:{mm:02}"
                save_config(bot_data)

                rescheduler = bot_data.get("reschedule_report")
                if rescheduler:
                    rescheduler()

                await message.reply_text(f"✅ Время отчёта установлено: {hh:02}:{mm:02}", reply_markup=main_menu_keyboard())
                chat_data.clear()
            else:
                raise ValueError
        except:
            await message.reply_text("Введите корректное время в формате HH:MM.")
        return

    # --- Сохраняем сообщение в файл, если есть текст ---
    try:
        if message.text:
            save_message_to_file(message)
    except Exception as e:
        await notify_admin(context.application, f"Ошибка при сохранении сообщения:\n{e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed_menu_chat(update.effective_chat, context):
        await update.message.reply_text("Команда /start доступна только в ЛС и разрешённых каналах.")
        return
    await update.message.reply_text(
        "Добро пожаловать! Используйте меню для работы с отчётами.",
        reply_markup=main_menu_keyboard()
    )

# 🔧 Очистка старых CSV-файлов старше N дней
def cleanup_old_data_files(days_to_keep=60):
    try:
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        for file in DATA_DIR.glob("*.csv"):
            try:
                file_date = datetime.strptime(file.stem, "%Y-%m-%d").date()
                if file_date < cutoff_date:
                    file.unlink()
                    print(f"🗑️ Удалён старый файл: {file.name}")
            except Exception as e:
                print(f"❌ Ошибка при обработке файла {file.name}: {e}")
    except Exception as e:
        print(f"❌ Ошибка при очистке старых файлов: {e}")

# bot2/flashcall_app20.py

import os
import pytz
from datetime import time
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters
)
from fastapi import Request
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

BOT_TOKEN = os.getenv("BOT2_TOKEN")
ERROR_CHANNEL_ID = int(os.getenv("BOT2_ERROR_CHANNEL_ID", "0"))
cfg = load_config()
cfg["bot_token"] = BOT_TOKEN
cfg["error_channel"] = ERROR_CHANNEL_ID

# Берём домен из переменной WEBHOOK_DOMAIN, либо из RENDER_EXTERNAL_URL на Render
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN") or os.getenv("RENDER_EXTERNAL_URL") or "https://yourdomain.com"
WEBHOOK_PATH = "/webhook/bot2"
WEBHOOK_URL = f"{WEBHOOK_DOMAIN}{WEBHOOK_PATH}"


# Инициализация приложения Telegram
application = ApplicationBuilder().token(BOT_TOKEN).build()
application.bot_data.update(cfg)

application.add_handler(CommandHandler("start", start))
application.add_handler(CallbackQueryHandler(callback_handler))
application.add_handler(MessageHandler(filters.ALL, message_handler))

import asyncio

async def scheduled_job():
    await scheduled_report(application.bot, application.bot_data)

# Планировщик
scheduler = AsyncIOScheduler()
hour, minute = map(int, cfg.get("report_time", "17:00").split(":"))
scheduler.add_job(
    scheduled_job,
    "cron", hour=hour, minute=minute, timezone=pytz.timezone("Europe/Kiev")
)

# === Экспортируемые функции для общего multi_bot ===

async def handle_startup():
    await application.initialize()
    await application.start()

    # ✅ Устанавливаем webhook
    await application.bot.set_webhook(WEBHOOK_URL)

    # Запускаем планировщик
    scheduler.start()

    # Очистка старых данных
    cleanup_old_data_files()

    if ERROR_CHANNEL_ID:
        await application.bot.send_message(ERROR_CHANNEL_ID, "✅ bot2 запущен")

async def handle_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, application.bot)
    await application.process_update(update)
    return {"ok": True}

async def handle_shutdown():
    await application.stop()
    await application.shutdown()
