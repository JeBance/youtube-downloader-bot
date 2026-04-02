"""
YouTube Downloader Bot — Telegram-бот для загрузки видео из YouTube.
Новая версия с модульной архитектурой.
"""
import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import BotCommand

from config import (
    ADMIN_ID,
    BOT_API_SERVER_URL,
    BOT_TOKEN,
    CACHE_DB_PATH,
    DOWNLOAD_PATH,
    LOG_LEVEL,
    MAX_FILE_SIZE,
    QUEUE_YOUTUBE_RPS,
    QUEUE_TELEGRAM_UPLOADS_PER_MIN,
    QUEUE_MAX_CONCURRENT,
    QUEUE_MAX_PER_USER,
)
from database import VideoDatabase
from queue_manager import init_queue_manager, FairQueueManager

# Импорт хендлеров
from handlers import commands_router, video_router, search_router, queue_router, process_download_task

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Проверка токена
if not BOT_TOKEN:
    logger.error("BOT_TOKEN не найден в переменных окружения!")
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

# Инициализация бота
if BOT_API_SERVER_URL:
    api_server = BOT_API_SERVER_URL.rsplit('/bot', 1)[0]
    session = AiohttpSession()
    session.api_server = api_server
    bot = Bot(token=BOT_TOKEN, session=session)
    logger.info(f"Используется локальный Bot API Server: {api_server}")
else:
    bot = Bot(token=BOT_TOKEN)
    logger.info("Используется публичный Telegram API")

dp = Dispatcher()

# Инициализация базы данных
db = VideoDatabase(CACHE_DB_PATH)
logger.info(f"База данных инициализирована: {CACHE_DB_PATH}")

# Инициализация менеджера очереди
queue_mgr = init_queue_manager(
    youtube_rps=QUEUE_YOUTUBE_RPS,
    telegram_uploads_per_min=QUEUE_TELEGRAM_UPLOADS_PER_MIN,
    max_concurrent_downloads=QUEUE_MAX_CONCURRENT,
    max_queue_per_user=QUEUE_MAX_PER_USER
)
logger.info(
    f"Очередь загрузок инициализирована: "
    f"YouTube RPS={QUEUE_YOUTUBE_RPS}, Telegram uploads/min={QUEUE_TELEGRAM_UPLOADS_PER_MIN}, "
    f"max_concurrent={QUEUE_MAX_CONCURRENT}, max_per_user={QUEUE_MAX_PER_USER}"
)

# Регистрация роутеров
dp.include_router(commands_router)
dp.include_router(video_router)
dp.include_router(search_router)
dp.include_router(queue_router)

# Глобальные объекты для доступа из хендлеров
# Используем middleware для передачи db и queue_mgr
@dp.update.middleware()
async def globals_middleware(handler, event, data):
    """Добавляет db и queue_mgr в контекст всех хендлеров."""
    data["db"] = db
    data["queue_mgr"] = queue_mgr
    return await handler(event, data)


async def set_commands(bot: Bot):
    """Устанавливает команды бота."""
    commands = [
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="help", description="Справка"),
        BotCommand(command="ping", description="Проверка связи"),
        BotCommand(command="status", description="Статус кэша"),
        BotCommand(command="queue", description="Статус очереди"),
        BotCommand(command="lang", description="Выбор языка"),
    ]
    await bot.set_my_commands(commands)
    logger.info("Команды бота установлены")


async def main():
    """Основная функция запуска бота."""
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

    await set_commands(bot)

    logger.info("Запуск бота...")

    # Создаём wrapper для передачи db в process_download_task
    async def process_task_wrapper(task):
        await process_download_task(task, db)

    await queue_mgr.start(process_task_wrapper)
    logger.info("Менеджер очереди запущен")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
