import os
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv
import uvicorn

# Загрузка переменных окружения
load_dotenv()

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL")  # например: https://yourdomain.com

# === Импорт ботов ===
from bot1.zvonki_single_run import (
    bot as bot1_instance,
    handle_startup as startup_bot1,
    handle_webhook as webhook_bot1,
    handle_shutdown as shutdown_bot1
)

from bot2.flashcall_app20 import (
    application as app_bot2,
    handle_startup as startup_bot2,
    handle_webhook as webhook_bot2,
    handle_shutdown as shutdown_bot2
)

from bot3.statbot_mainBinotel20 import (
    application as app_bot3,
    handle_startup as startup_bot3,
    handle_webhook as webhook_bot3,
    handle_shutdown as shutdown_bot3
)

# === Карта ботов ===
bots = {
    "bot1": {
        "startup": startup_bot1,
        "webhook": webhook_bot1,
        "shutdown": shutdown_bot1,
        "set_webhook": lambda: bot1_instance.set_webhook(f"{WEBHOOK_BASE_URL}/webhook/bot1")
    },
    "bot2": {
        "startup": startup_bot2,
        "webhook": webhook_bot2,
        "shutdown": shutdown_bot2,
        "set_webhook": lambda: app_bot2.bot.set_webhook(f"{WEBHOOK_BASE_URL}/webhook/bot2")
    },
    "bot3": {
        "startup": startup_bot3,
        "webhook": webhook_bot3,
        "shutdown": shutdown_bot3,
        "set_webhook": lambda: app_bot3.bot.set_webhook(f"{WEBHOOK_BASE_URL}/webhook/bot3")
    }
}

# === Инициализация FastAPI ===
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    for name, bot in bots.items():
        try:
            await bot["startup"]()
            await bot["set_webhook"]()
            print(f"✅ {name} успешно запущен и webhook установлен")
        except Exception as e:
            print(f"❌ Ошибка запуска {name}: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    for name, bot in bots.items():
        try:
            await bot["shutdown"]()
            print(f"🔻 {name} остановлен")
        except Exception as e:
            print(f"❌ Ошибка при остановке {name}: {e}")

@app.post("/webhook/{bot_name}")
async def webhook_router(bot_name: str, request: Request):
    if bot_name not in bots:
        raise HTTPException(status_code=404, detail=f"❌ Бот {bot_name} не найден")

    try:
        return await bots[bot_name]["webhook"](request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Ошибка при обработке update: {e}")

if __name__ == "__main__":
    uvicorn.run("multi_app:app", host="0.0.0.0", port=8000, reload=False)
