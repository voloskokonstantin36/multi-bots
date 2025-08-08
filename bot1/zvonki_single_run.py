# -*- coding: utf-8 -*-
import sys
import asyncio
import json
import tempfile
from pathlib import Path
import datetime as dt
import time
import os
import hmac
import hashlib
import shutil
import pandas as pd
from aiogram import Bot, Dispatcher, types
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz

KYIV_TZ = pytz.timezone("Europe/Kyiv")

# === Настройки ===
load_dotenv()

TOKEN = os.getenv("BOT1_TOKEN")

# Берём домен из переменной WEBHOOK_DOMAIN, либо из RENDER_EXTERNAL_URL на Render
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN") or os.getenv("RENDER_EXTERNAL_URL") or "https://yourdomain.com"
WEBHOOK_PATH = "/webhook/bot1"
WEBHOOK_URL = f"{WEBHOOK_DOMAIN}{WEBHOOK_PATH}"
ERROR_CHANNEL_ID = int(os.getenv("ERROR_CHANNEL_ID", "0"))

# Локальная папка бота
BASE_DIR = Path(__file__).resolve().parent

# Каналы ошибок
ERROR_CHANNEL_ID = os.getenv("ERROR_CHANNEL_ID")

# Каналы и время из channels.json
CHANNELS_FILE = BASE_DIR / "channels.json"
DEFAULT_MANAGER_REPORT_TIME = "17:00"

def load_channels_and_time():
    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return (
                data.get("employee_chat_id", -100123),
                data.get("manager_chat_id", -100456),
                data.get("manager_report_time", DEFAULT_MANAGER_REPORT_TIME),
            )
    except:
        return -100123, -100456, DEFAULT_MANAGER_REPORT_TIME

def save_channels_and_time(emp_chat_id, mgr_chat_id, mgr_report_time):
    with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "employee_chat_id": emp_chat_id,
            "manager_chat_id": mgr_chat_id,
            "manager_report_time": mgr_report_time,
        }, f, ensure_ascii=False)

employee_chat_id, manager_chat_id, manager_report_time = load_channels_and_time()

# Подсчёт активных часов
def calculate_active_hours(call_times: pd.Series) -> float:
    times = call_times.sort_values().reset_index(drop=True)
    if times.empty:
        return 1.0
    active_periods = []
    start_time = times.iloc[0]
    prev_time = start_time
    for current_time in times[1:]:
        if (current_time - prev_time) > pd.Timedelta(hours=1):
            active_periods.append((start_time, prev_time))
            start_time = current_time
        prev_time = current_time
    active_periods.append((start_time, prev_time))
    total_seconds = sum((end - start).total_seconds() for start, end in active_periods)
    return max(total_seconds / 3600, 1.0)

def get_report_time(now=None) -> str:
    if now is None:
        now = datetime.now(KYIV_TZ)
    today = now.date()
    start_hour = 9
    end_hour = 21
    last_report_time = dt.datetime.combine(today, dt.time(end_hour, 0))

    hour, minute = now.hour, now.minute
    if hour < start_hour:
        report_time = dt.datetime.combine(today, dt.time(start_hour, 0))
    elif hour >= end_hour:
        report_time = last_report_time
    else:
        if minute <= 10:
            report_time = dt.datetime.combine(today, dt.time(hour, 0))
        else:
            next_hour = hour + 1
            report_time = dt.datetime.combine(today, dt.time(min(next_hour, end_hour), 0))
    return report_time.strftime('%H:%M %d-%m-%Y')

import csv
import requests

from collections import OrderedDict

def fetch_outgoing_calls_binotel_halfhour() -> Path | None:
    import os, json, csv, time, tempfile, shutil, requests, pytz
    from datetime import datetime, timedelta
    from pathlib import Path
    script_dir = Path(__file__).parent.resolve()

    def should_replace_file(file_path: Path, interval_end: datetime) -> bool:
        if not file_path.exists():
            return True
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=pytz.UTC)
        return file_mtime < interval_end.astimezone(pytz.UTC)

    BINOTEL_API_KEY = os.getenv("BINOTEL_API_KEY")
    BINOTEL_API_SECRET = os.getenv("BINOTEL_API_SECRET")
    if not BINOTEL_API_KEY or not BINOTEL_API_SECRET:
        print("❌ Не заданы ключи BINOTEL_API_KEY или BINOTEL_API_SECRET")
        return None

    KYIV_TZ = pytz.timezone("Europe/Kyiv")
    now_kyiv = datetime.now(KYIV_TZ)
    date_str = now_kyiv.strftime("%Y-%m-%d")
    script_dir = Path(__file__).parent.resolve()
    day_folder = script_dir / "binotel" / date_str
    day_folder.mkdir(parents=True, exist_ok=True)

    # Удаление старых папок
    for subdir in (script_dir / "binotel").iterdir():
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

    # Сбор и сохранение всех звонков в CSV
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
                    "date": datetime.fromtimestamp(int(call.get("startTime", 0)), KYIV_TZ).strftime("%d.%m.%Y %H:%M:%S"),
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

    # Сохраняем CSV с правильной кодировкой
    desired_columns = list(transformed_calls[0].keys())
    tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', encoding='utf-8-sig', newline='')
    writer = csv.DictWriter(tmpfile, fieldnames=desired_columns, delimiter=';', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(transformed_calls)
    tmpfile.close()

    final_path = script_dir / "new_data" / f"binotel_calls_{date_str}.csv"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(tmpfile.name, final_path)
    print(f"✅ CSV сохранён: {final_path}")
    return final_path

def build_reports(df: pd.DataFrame) -> tuple[str, str]:
    df.columns = df.columns.str.lower().str.strip()
    if 'employee name' not in df.columns or 'date' not in df.columns:
        raise ValueError("Не найдены нужные столбцы в файле")

    df = df[df['employee name'].str.contains(r'дж-', case=False, na=False)].copy()
    df.loc[:, 'initials'] = df['employee name'].str.extract(r'\((.*?)\)')
    df.loc[:, 'call_dt'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')

    s = df.groupby('initials', group_keys=False).apply(
        lambda x: pd.Series({
            'total': len(x),
            'cancel': (x['disposition'].str.upper() == 'CANCEL').sum(),
            'zero': (x['billsec'] == 0).sum(),
            'wait': x['waitsec'].sum(),
            'talk': x['billsec'].sum(),
            'first_call': x['call_dt'].min(),
            'last_call': x['call_dt'].max(),
            'active_hours': calculate_active_hours(x['call_dt']),
        }),
        include_groups=False
    ).reset_index()

    s['in_hour'] = s.apply(
        lambda r: round(r['total'] / r['active_hours'], 1) if r['active_hours'] else 0,
        axis=1
    )

    s['cancel_pct'] = (s['cancel'] / s['total']) * 100
    s['talk_hours'] = s['talk'] / 3600
    s['period_hours'] = (s['last_call'] - s['first_call']).dt.total_seconds() / 3600

    now_str = get_report_time()

    emp_report = f"\U0001F4DE <b>Звонки Дожим отчёт на {now_str}:</b>\n\n"
    for _, r in s.sort_values(by='total', ascending=False).iterrows():
        cancel_pct_rounded = round(r['cancel_pct'])
        cancel_style = ("<b>", "</b>") if cancel_pct_rounded >= 20 else ("", "")
        in_hour_val = (
            f"{r['in_hour']:.1f}" if pd.notnull(r['in_hour']) and r['in_hour'] != float("inf") else "0"
        )
        emp_report += (
            f"\U0001F464 <b>{r['initials']}</b> — "
            f"звонков <b>{int(r['total'])}</b>, "
            f"в час <b>{in_hour_val}</b>, "
            f"сбросов {cancel_style[0]}{int(r['cancel'])} ({cancel_pct_rounded}%){cancel_style[1]}"
            f"{'‼️' if cancel_pct_rounded >= 20 else ''}\n\n"
        )

    mgr_report = f"\U0001F4C8 <b>Звонки Дожим — для руководителя</b>\n⏰ <i>Отчёт на {now_str}</i>\n\n"
    for _, r in s.sort_values(by='total', ascending=False).iterrows():
        cancel_pct_rounded = round(r['cancel_pct'])
        cancel_str = f"<b>{int(r['cancel'])}</b>‼️" if cancel_pct_rounded >= 20 else f"{int(r['cancel'])}"
        first_call = r['first_call'].strftime('%H:%M %d-%m-%Y') if pd.notnull(r['first_call']) else "нет данных"
        last_call = r['last_call'].strftime('%H:%M %d-%m-%Y') if pd.notnull(r['last_call']) else "нет данных"
        bold = ("<b>", "</b>") if r['total'] >= 5 else ("", "")
        mgr_report += (
            f"\U0001F464 {bold[0]}{r['initials']}{bold[1]} — звонков: {bold[0]}{int(r['total'])}{bold[1]}, "
            f"сбросов: {cancel_str}, недозвонов: {int(r['zero'])},\n"
            f"первый звонок: {first_call}, последний звонок: {last_call},\n"
            f"разговоров: {r['talk_hours']:.2f} ч, период активности: {r['period_hours']:.2f} ч\n\n"
        )

    return emp_report, mgr_report
    
_last_report_time = 0  # Глобальная переменная защиты от повтора

async def send_reports(bot: Bot, path: Path, to='both'):
    print(f"📩 Отправка отчёта (to='{to}') из файла {path}")
    print(f"📌 send_reports вызван в {datetime.now(KYIV_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

    if not path or not path.exists():
        print("🚫 CSV-файл не найден — отчёт не отправлен")
        if ERROR_CHANNEL_ID:
            try:
                await bot.send_message(
                    chat_id=int(ERROR_CHANNEL_ID),
                    text="⚠️ CSV-файл для отчёта не найден"
                )
            except Exception as e:
                print(f"❌ Не удалось отправить уведомление: {e}")
        return

    try:
        df = pd.read_csv(path, sep=None, engine='python', encoding='utf-8')
        emp_text, mgr_text = build_reports(df)

        if to in ('emp', 'both'):
            await bot.send_message(chat_id=manager_chat_id, text=emp_text, parse_mode='HTML')
        if to in ('mgr', 'both'):
            await bot.send_message(chat_id=employee_chat_id, text=mgr_text, parse_mode='HTML')

    except Exception as e:
        print(f"❌ Ошибка при формировании/отправке отчёта: {e}")
        if ERROR_CHANNEL_ID:
            try:
                await bot.send_message(
                    chat_id=int(ERROR_CHANNEL_ID),
                    text=f"❌ Ошибка при формировании отчёта:\n<code>{e}</code>",
                    parse_mode='HTML'
                )
            except Exception:
                pass
    finally:
        try:
            path.unlink()
            print(f"🧹 Удалён CSV файл: {path}")
        except Exception as e:
            print(f"⚠️ Не удалось удалить CSV файл: {e}")

async def auto_report_loop(bot: Bot):
    sent_today_mgr = False     # защита для руководителя
    last_sent_hour_emp = None  # защита от двойной отправки менеджерам

    while True:
        now = datetime.now(KYIV_TZ)
        current_time_str = now.strftime("%H:%M")

        # Менеджерам — каждый час, один раз
        if now.minute == 0 and 9 <= now.hour <= 21:
            if last_sent_hour_emp != now.hour:
                try:
                    print(f"📤 Отправка отчёта менеджерам в {current_time_str}")
                    path = fetch_outgoing_calls_binotel_halfhour()
                    if path:
                        await send_reports(bot, path, to='emp')
                        last_sent_hour_emp = now.hour
                    else:
                        print("⚠️ Не удалось получить CSV — отчёт не отправлен.")
                except Exception as e:
                    print(f"❌ Ошибка при отправке отчёта менеджерам: {e}")

        # Руководителю — только в заданное время, один раз в день
        if current_time_str == manager_report_time and not sent_today_mgr:
            try:
                print(f"📤 Отправка отчёта руководителю в {current_time_str}")
                path = fetch_outgoing_calls_binotel_halfhour()
                if path:
                    await send_reports(bot, path, to='mgr')
                    sent_today_mgr = True
                else:
                    print("⚠️ Не удалось получить CSV — отчёт не отправлен.")
            except Exception as e:
                print(f"❌ Ошибка при отправке отчёта руководителю: {e}")

        # Сброс флага отправки для руководителя в полночь
        if current_time_str == "00:00":
            sent_today_mgr = False

        await asyncio.sleep(30)

# === Импорты ===
from fastapi import Request
from fastapi.responses import JSONResponse
import logging

logger = logging.getLogger(__name__)

# === Инициализация бота ===
bot = Bot(token=TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

waiting_for_manager = set()
waiting_for_boss = set()
waiting_for_manager_time = set()

# === Кнопки ===
def main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Сменить канал руководителя")
    kb.add("Сменить канал менеджеров")
    kb.add("Сменить время отчёта руководителя")
    kb.add("Отправить отчёт")
    kb.add("Полный отчёт")
    return kb

# === Хендлеры ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("Привет! Это бот для управления отправкой отчётов по звонкам.", reply_markup=main_keyboard())

@dp.message_handler(lambda m: m.text == "Сменить канал менеджеров")
async def cmd_change_manager(message: types.Message):
    waiting_for_manager.add(message.from_user.id)
    await message.answer("Отправь новый chat ID канала для менеджеров (число).")

@dp.message_handler(lambda m: m.text == "Сменить канал руководителя")
async def cmd_change_boss(message: types.Message):
    waiting_for_boss.add(message.from_user.id)
    await message.answer("Отправь новый chat ID канала для руководителя (число).")

@dp.message_handler(lambda m: m.text == "Сменить время отчёта руководителя")
async def cmd_change_manager_report_time(message: types.Message):
    waiting_for_manager_time.add(message.from_user.id)
    await message.answer("Отправь новое время отчёта руководителя в формате ЧЧ:ММ (например, 17:05).")

@dp.message_handler(lambda m: m.from_user.id in waiting_for_manager)
async def new_manager_chat(message: types.Message):
    global manager_chat_id
    try:
        new_id = int(message.text)
        manager_chat_id = new_id
        save_channels_and_time(employee_chat_id, manager_chat_id, manager_report_time)
        await message.answer(f"Канал менеджеров установлен на {new_id}", reply_markup=main_keyboard())
    except ValueError:
        await message.answer("Ошибка! Введите число.")
    waiting_for_manager.remove(message.from_user.id)

@dp.message_handler(lambda m: m.from_user.id in waiting_for_boss)
async def new_boss_chat(message: types.Message):
    global employee_chat_id
    try:
        new_id = int(message.text)
        employee_chat_id = new_id
        save_channels_and_time(employee_chat_id, manager_chat_id, manager_report_time)
        await message.answer(f"Канал руководителя установлен на {new_id}", reply_markup=main_keyboard())
    except ValueError:
        await message.answer("Ошибка! Введите число.")
    waiting_for_boss.remove(message.from_user.id)

@dp.message_handler(lambda m: m.from_user.id in waiting_for_manager_time)
async def new_manager_report_time(message: types.Message):
    global manager_report_time
    try:
        parts = message.text.split(":")
        if len(parts) != 2:
            raise ValueError
        hh, mm = int(parts[0]), int(parts[1])
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            raise ValueError
        manager_report_time = f"{hh:02d}:{mm:02d}"
        save_channels_and_time(employee_chat_id, manager_chat_id, manager_report_time)
        await message.answer(f"Время отчёта руководителя установлено на {manager_report_time}", reply_markup=main_keyboard())
    except Exception:
        await message.answer("Ошибка! Введите время в формате ЧЧ:ММ, например 17:05.")
    waiting_for_manager_time.remove(message.from_user.id)

@dp.message_handler(lambda m: m.text == "Отправить отчёт")
async def cmd_send_report(message: types.Message):
    await message.answer("Формирую и отправляю отчёт менеджерам...")
    path = fetch_outgoing_calls_binotel_halfhour()
    if path:
        await send_reports(bot, path, to='emp')
        await message.answer("Отчёт отправлен.", reply_markup=main_keyboard())
    else:
        await message.answer("⚠️ Не удалось сформировать отчёт.", reply_markup=main_keyboard())

@dp.message_handler(lambda m: m.text == "Полный отчёт")
async def cmd_full_report(message: types.Message):
    await message.answer("Формирую и отправляю полный отчёт для руководителя...")
    path = fetch_outgoing_calls_binotel_halfhour()
    if path:
        await send_reports(bot, path, to='mgr')
        await message.answer("Отчёт отправлен.", reply_markup=main_keyboard())
    else:
        await message.answer("⚠️ Не удалось сформировать отчёт.")

@dp.message_handler(commands=["report"])
async def cmd_report(message: types.Message):
    await message.answer("Формирую и отправляю полный отчёт для руководителя по команде /report...")
    path = fetch_outgoing_calls_binotel_halfhour()
    if path:
        await send_reports(bot, path, to='mgr')
        await message.answer("Отчёт отправлен.", reply_markup=main_keyboard())
    else:
        await message.answer("⚠️ Не удалось сформировать отчёт.")

# === Webhook обработка ===
async def handle_webhook(request: Request):
    try:
        data = await request.json()
        update = types.Update(**data)  # ✅ Правильно для aiogram 3.0–3.3

        bot.set_current(bot)
        await dp.process_update(update)

        return JSONResponse({"ok": True})
    except Exception as e:
        logging.exception("Ошибка при обработке апдейта:")
        return JSONResponse(status_code=400, content={"error": str(e)})


async def handle_startup():
    # Установка вебхука
    await bot.set_webhook(WEBHOOK_URL)

    now = datetime.now(KYIV_TZ)
    if now.minute == 0:
        await asyncio.sleep(60)

    asyncio.create_task(auto_report_loop(bot))

    if ERROR_CHANNEL_ID:
        await bot.send_message(ERROR_CHANNEL_ID, "✅ bot1 запущен")

async def handle_shutdown():
    await bot.delete_webhook()
    await bot.session.close()
