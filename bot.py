"""
YouTube Downloader Bot — Telegram-бот для загрузки видео из YouTube.
"""
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from yt_dlp import YoutubeDL

from config import (
    ADMIN_ID,
    BOT_API_SERVER_URL,
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
if BOT_API_SERVER_URL:
    # Используем локальный Bot API Server
    from aiogram.client.session.base import BaseSession
    # BaseSession автоматически использует указанный API сервер
    bot = Bot(token=BOT_TOKEN)
    bot.session.api_server = BOT_API_SERVER_URL.rsplit('/bot', 1)[0]  # http://localhost:8081
    logger.info(f"Используется локальный Bot API Server: {bot.session.api_server}")
else:
    # Используем публичный API Telegram
    bot = Bot(token=BOT_TOKEN)
    logger.info("Используется публичный Telegram API")

dp = Dispatcher()

# Кэш для хранения URL по video_id (для callback)
url_cache: dict[str, str] = {}

# Паттерн для YouTube URL
YOUTUBE_PATTERN = re.compile(
    r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+"
)

# Опции для yt-dlp (только для получения информации)
YDL_INFO_OPTIONS = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
}

# Опции для yt-dlp (для загрузки)
YDL_OPTIONS = {
    "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
    "outtmpl": str(DOWNLOAD_PATH / "%(id)s.%(ext)s"),
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "merge_output_format": "mp4",
}


def is_youtube_url(url: str) -> bool:
    """Проверяет, является ли ссылка YouTube URL."""
    return bool(YOUTUBE_PATTERN.match(url))


def get_video_info(url: str) -> Optional[dict]:
    """
    Получает информацию о видео без загрузки.
    
    Returns:
        dict с информацией о видео или None при ошибке
    """
    try:
        with YoutubeDL(YDL_INFO_OPTIONS) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception as e:
        logger.error(f"Ошибка при получении информации: {e}")
        return None


def build_quality_keyboard(video_id: str, formats: list) -> InlineKeyboardMarkup:
    """
    Строит inline-клавиатуру с вариантами качества.
    
    Args:
        video_id: ID видео
        formats: список кортежей (format_code, description, height, estimated_size)
    """
    builder = InlineKeyboardBuilder()
    
    for fmt_code, description, height, est_size in formats:
        # Проверяем, не превышает ли размер лимит
        if est_size > MAX_FILE_SIZE:
            continue  # Пропускаем слишком большие форматы
        builder.button(
            text=f"📹 {description}",
            callback_data=f"download_{video_id}_{fmt_code}"
        )
    
    builder.button(text="❌ Отмена", callback_data=f"cancel_{video_id}")
    builder.adjust(2, 2, 1)  # 2 кнопки в ряду
    return builder.as_markup()


def get_available_formats(formats: list, max_size_mb: int = 48) -> list:
    """
    Извлекает доступные форматы из информации о видео.
    
    Args:
        formats: список форматов из yt-dlp
        max_size_mb: максимальный размер в MB (по умолчанию 48MB с запасом до 50MB)
    
    Returns:
        список кортежей (format_code, description, height, estimated_size)
    """
    available = []
    max_size_bytes = max_size_mb * 1024 * 1024
    
    # Фильтруем форматы с видео
    for fmt in formats:
        if fmt.get('vcodec') == 'none':
            continue
            
        height = fmt.get('height', 0)
        if not height:
            continue
            
        format_id = fmt.get('format_id', '')
        filesize = fmt.get('filesize', 0) or fmt.get('filesize_approx', 0)
        
        # Добавляем аудио-поток к размеру
        audio_size = 0
        for audio_fmt in formats:
            if audio_fmt.get('acodec') != 'none' and audio_fmt.get('vcodec') == 'none':
                audio_size = audio_fmt.get('filesize', 0) or audio_fmt.get('filesize_approx', 0)
                break
        
        total_size = filesize + audio_size if filesize else 0
        
        # Пропускаем форматы, превышающие лимит
        if total_size > max_size_bytes:
            continue
        
        # Формируем описание
        size_str = f" ({total_size / 1024 / 1024:.0f} MB)" if total_size else ""
        
        # Определяем качество
        quality_label = f"{height}p"
        if height >= 2160:
            quality_label = f"{height}p (4K)"
        elif height >= 1440:
            quality_label = f"{height}p (2K)"
        elif height >= 1080:
            quality_label = f"{height}p (FHD)"
        elif height >= 720:
            quality_label = f"{height}p (HD)"
        
        desc = f"{quality_label}{size_str}"
        available.append((f"{format_id}+bestaudio", desc, height, total_size))
    
    # Сортируем по высоте (убывание) и убираем дубликаты
    seen_heights = set()
    unique = []
    for fmt in sorted(available, key=lambda x: x[2], reverse=True):
        if fmt[2] not in seen_heights:
            seen_heights.add(fmt[2])
            unique.append(fmt)
    
    # Добавляем опцию "только аудио"
    unique.append(("bestaudio", "🎵 Только аудио", 0, 0))
    
    return unique[:8]  # Максимум 8 вариантов + аудио


async def download_video(url: str, format_code: str = "best") -> Tuple[Optional[Path], Optional[str]]:
    """
    Скачивает видео с YouTube.
    
    Args:
        url: Ссылка на YouTube видео
        format_code: Формат для загрузки (например, "137+bestaudio" для 1080p)
        
    Returns:
        Кортеж (путь к файлу, название формата) или None при ошибке
    """
    loop = asyncio.get_event_loop()
    
    options = YDL_OPTIONS.copy()
    options["format"] = format_code
    
    def _download():
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            # Проверяем существование файла
            if Path(filename).exists():
                return Path(filename), info.get('title', 'video')
            # Пробуем другие расширения
            base = filename.rsplit(".", 1)[0]
            for ext in ["mp4", "webm", "mkv", "m4a"]:
                candidate = Path(f"{base}.{ext}")
                if candidate.exists():
                    return candidate, info.get('title', 'video')
            return None, None
    
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _download),
            timeout=DOWNLOAD_TIMEOUT
        )
    except asyncio.TimeoutError:
        logger.error(f"Таймаут при загрузке: {url}")
        return None, None
    except Exception as e:
        logger.error(f"Ошибка при загрузке: {e}")
        return None, None


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
        "/status — статус бота",
        parse_mode="Markdown"
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
        "💡 **Совет:** Для лучших результатов используй короткие видео.",
        parse_mode="Markdown"
    )
    logger.info(f"Команда /help от {message.from_user.id}")


@dp.message(Command("status"))
async def cmd_status(message: types.Message):
    """Обработчик команды /status."""
    await message.answer("✅ Бот работает нормально!")
    logger.info(f"Команда /status от {message.from_user.id}")


@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    """Обработчик команды /ping — проверка работоспособности."""
    await message.answer("🏓 Понг! Бот на связи!")
    logger.info(f"Команда /ping от {message.from_user.id}")


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Обработчик команды /admin — только для админа."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return
    
    await message.answer(
        "👤 **Админ-панель**\n\n"
        f"Ваш ID: `{message.from_user.id}`\n"
        f"Имя: {message.from_user.full_name}\n"
        f"Username: @{message.from_user.username or 'нет'}",
        parse_mode="Markdown"
    )
    logger.info(f"Команда /admin от {message.from_user.id}")


@dp.message(F.text)
async def handle_url(message: types.Message):
    """Обработчик ссылок на YouTube."""
    url = message.text.strip()
    
    if not is_youtube_url(url):
        return  # Игнорируем не-Youtube ссылки
    
    logger.info(f"Получена ссылка от {message.from_user.id}: {url}")
    
    # Получаем информацию о видео
    status_msg = await message.answer("⏳ Получаю информацию о видео...")
    
    info = get_video_info(url)
    
    if not info:
        await status_msg.edit_text("❌ Не удалось получить информацию о видео. Проверьте ссылку.")
        return
    
    # Извлекаем доступные форматы
    formats = info.get('formats', [])
    available = get_available_formats(formats, max_size_mb=MAX_FILE_SIZE // 1024 // 1024)
    
    if not available:
        await status_msg.edit_text("❌ Нет доступных форматов для загрузки.")
        return
    
    # Формируем информацию о видео
    title = info.get('title', 'Неизвестно')
    duration = info.get('duration', 0)
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
    uploader = info.get('uploader', 'Неизвестно')
    video_id = info.get('id', 'unknown')
    
    # Создаём клавиатуру
    keyboard = build_quality_keyboard(video_id, available)
    
    # Сохраняем URL в кэш
    url_cache[video_id] = url
    
    await status_msg.edit_text(
        f"🎬 **{title}**\n\n"
        f"👤 {uploader}\n"
        f"⏱ Длительность: {duration_str}\n\n"
        f"**Выберите качество:**",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    logger.info(f"Показаны варианты качества для видео {video_id}")


@dp.callback_query(F.data.startswith("download_"))
async def handle_download(callback: types.CallbackQuery):
    """Обработчик выбора качества."""
    # Парсим callback: download_{video_id}_{format_code}
    parts = callback.data.split("_", 2)
    if len(parts) < 3:
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return
    
    video_id = parts[1]
    format_code = parts[2]
    
    # Получаем URL из кэша
    url = url_cache.get(video_id)
    if not url:
        await callback.answer("❌ Ссылка устарела, отправьте заново", show_alert=True)
        return
    
    # Определяем описание качества
    quality_desc = format_code.replace("+bestaudio", "")
    if format_code == "bestaudio":
        quality_desc = "аудио"
    
    # Получаем размер из описания
    size_match = re.search(r'\((\d+) MB\)', callback.message.text)
    size_info = f" (~{size_match.group(1)} MB)" if size_match else ""
    
    await callback.message.edit_text(
        f"{callback.message.text}\n\n⏳ Скачиваю в качестве {quality_desc}{size_info}...",
        parse_mode="Markdown"
    )
    
    await callback.answer(f"Начинаю загрузку ({quality_desc})...")
    
    # Скачиваем видео
    filepath, title = await download_video(url, format_code)
    
    if not filepath:
        await callback.message.edit_text(
            f"{callback.message.text}\n\n❌ Ошибка при загрузке видео."
        )
        return
    
    # Проверяем размер
    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        await callback.message.edit_text(
            f"{callback.message.text}\n\n❌ Файл слишком большой ({file_size / 1024 / 1024:.1f} MB)."
        )
        return
    
    # Отправляем видео
    try:
        video = FSInputFile(filepath)
        await callback.message.answer_video(
            video,
            caption=f"🎬 {title}\n📹 Качество: {quality_desc}",
            parse_mode="Markdown"
        )
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка при отправке: {e}")
        await callback.message.answer(f"❌ Ошибка при отправке: {e}")
    finally:
        await cleanup_file(filepath)
        # Очищаем кэш
        url_cache.pop(video_id, None)


@dp.callback_query(F.data.startswith("cancel_"))
async def handle_cancel(callback: types.CallbackQuery):
    """Обработчик отмены загрузки."""
    await callback.message.delete()
    await callback.answer("Загрузка отменена")


async def main():
    """Основная функция запуска бота."""
    # Создаём директорию для загрузок
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    
    logger.info("Запуск бота...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
