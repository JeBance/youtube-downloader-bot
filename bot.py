"""
YouTube Downloader Bot — Telegram-бот для загрузки видео из YouTube.
"""
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import FSInputFile
from yt_dlp import YoutubeDL

from config import (
    BOT_TOKEN,
    DOWNLOAD_PATH,
    DOWNLOAD_TIMEOUT,
    LOG_LEVEL,
    MAX_FILE_SIZE,
    SEND_TIMEOUT,
)

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
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Паттерн для YouTube URL
YOUTUBE_PATTERN = re.compile(
    r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+"
)

# Опции для yt-dlp
YDL_OPTIONS = {
    "format": "best[height<=720]/best",  # Максимум 720p для уменьшения размера
    "outtmpl": str(DOWNLOAD_PATH / "%(id)s.%(ext)s"),
    "noplaylist": True,  # Не скачивать плейлисты
    "quiet": True,
    "no_warnings": True,
}


def is_youtube_url(url: str) -> bool:
    """Проверяет, является ли ссылка YouTube URL."""
    return bool(YOUTUBE_PATTERN.match(url))


async def download_video(url: str) -> Optional[Path]:
    """
    Скачивает видео с YouTube.
    
    Args:
        url: Ссылка на YouTube видео
        
    Returns:
        Путь к скачанному файлу или None при ошибке
    """
    loop = asyncio.get_event_loop()
    
    def _download():
        with YoutubeDL(YDL_OPTIONS) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            # Проверяем существование файла
            if Path(filename).exists():
                return Path(filename)
            # Пробуем другие расширения (иногда yt-dlp меняет формат)
            base = filename.rsplit(".", 1)[0]
            for ext in ["mp4", "webm", "mkv", "m4a"]:
                candidate = Path(f"{base}.{ext}")
                if candidate.exists():
                    return candidate
            return None
    
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _download),
            timeout=DOWNLOAD_TIMEOUT
        )
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при загрузке: {url}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при загрузке: {e}")
        return None


async def cleanup_file(filepath: Path) -> None:
    """Удаляет временный файл после отправки."""
    try:
        if filepath.exists():
            filepath.unlink()
            logger.info(f"Удалён файл: {filepath}")
    except Exception as e:
        logger.error(f"Ошибка при удалении файла: {e}")


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    """Обработчик команды /start."""
    await message.answer(
        "👋 Привет! Я бот для загрузки видео из YouTube.\n\n"
        "📥 Просто отправь мне ссылку на видео, и я скачаю его для тебя.\n\n"
        "⚙️ Доступные команды:\n"
        "/help — справка\n"
        "/status — статус бота"
    )
    logger.info(f"Команда /start от {message.from_user.id}")


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Обработчик команды /help."""
    await message.answer(
        "📖 **Инструкция по использованию:**\n\n"
        "1. Отправь ссылку на YouTube видео\n"
        "2. Я скачаю видео (максимум 720p)\n"
        "3. Отправлю файл тебе в чат\n\n"
        "⚠️ **Ограничения:**\n"
        "- Максимальный размер файла: 50 MB\n"
        "- Таймаут загрузки: 5 минут\n"
        "- Только отдельные видео (не плейлисты)\n\n"
        "💡 **Совет:** Для лучших результатов используй короткие видео."
    )
    logger.info(f"Команда /help от {message.from_user.id}")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    """Обработчик команды /status."""
    await message.answer("✅ Бот работает нормально!")
    logger.info(f"Команда /status от {message.from_user.id}")


@dp.message(F.text)
async def handle_url(message: types.Message):
    """Обработчик ссылок на YouTube."""
    url = message.text.strip()
    
    if not is_youtube_url(url):
        return  # Игнорируем не-Youtube ссылки
    
    logger.info(f"Получена ссылка от {message.from_user.id}: {url}")
    
    # Отправляем сообщение о начале загрузки
    status_msg = await message.answer("⏳ Начинаю загрузку видео...")
    
    # Скачиваем видео
    filepath = await download_video(url)
    
    if not filepath:
        await status_msg.edit_text("❌ Не удалось скачать видео. Проверьте ссылку и попробуйте снова.")
        return
    
    # Проверяем размер файла
    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        await status_msg.edit_text(
            f"❌ Файл слишком большой ({file_size / 1024 / 1024:.1f} MB).\n"
            f"Максимальный размер: {MAX_FILE_SIZE / 1024 / 1024:.0f} MB."
        )
        return
    
    # Отправляем видео
    await status_msg.edit_text("📤 Отправляю видео...")
    
    try:
        video = FSInputFile(filepath)
        await message.answer_video(
            video,
            caption="🎬 Видео загружено через YouTube Downloader Bot",
            timeout=SEND_TIMEOUT
        )
        await status_msg.delete()
    except Exception as e:
        logger.error(f"Ошибка при отправке видео: {e}")
        await status_msg.edit_text(f"❌ Ошибка при отправке: {e}")
    finally:
        # Очищаем файл в любом случае
        await cleanup_file(filepath)


async def main():
    """Основная функция запуска бота."""
    # Создаём директорию для загрузок
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    
    logger.info("Запуск бота...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
