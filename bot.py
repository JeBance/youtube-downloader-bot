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
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from yt_dlp import YoutubeDL

from database import VideoCache

from config import (
    ADMIN_ID,
    BOT_API_SERVER_URL,
    BOT_TOKEN,
    CACHE_DB_PATH,
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
    from aiogram.client.session.aiohttp import AiohttpSession
    # Извлекаем базовый URL сервера (без /botTOKEN)
    api_server = BOT_API_SERVER_URL.rsplit('/bot', 1)[0]
    session = AiohttpSession()
    session.api_server = api_server
    bot = Bot(token=BOT_TOKEN, session=session)
    logger.info(f"Используется локальный Bot API Server: {api_server}")
else:
    # Используем публичный API Telegram
    bot = Bot(token=BOT_TOKEN)
    logger.info("Используется публичный Telegram API")

dp = Dispatcher()

# Инициализация кэша
cache = VideoCache(CACHE_DB_PATH)
logger.info(f"Кэш видео инициализирован: {CACHE_DB_PATH}")

# Кэш для хранения URL по video_id (для callback)
url_cache: dict[str, str] = {}

# Кэш для хранения метаданных видео (title, uploader, duration)
video_metadata_cache: dict[str, dict] = {}

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


def build_quality_keyboard(video_id: str, formats: list, cached_formats: list = None) -> InlineKeyboardMarkup:
    """
    Строит inline-клавиатуру с вариантами качества.

    Args:
        video_id: ID видео
        formats: список кортежей (format_code, description, height, estimated_size)
        cached_formats: список (format_code, quality_label) уже закэшированных форматов
    """
    builder = InlineKeyboardBuilder()
    
    # Создаём множество закэшированных форматов для быстрой проверки
    cached_set = set(fmt[0] for fmt in (cached_formats or []))
    
    for fmt_code, description, height, est_size in formats:
        # Проверяем, не превышает ли размер лимит
        if est_size > MAX_FILE_SIZE:
            continue  # Пропускаем слишком большие форматы
        
        # Проверяем, есть ли в кэше
        if fmt_code in cached_set:
            # Уже закэшировано — показываем с галочкой
            builder.button(
                text=f"✅ {description}",
                callback_data=f"download_{video_id}_{fmt_code}"
            )
        else:
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
    # Добавляем пользователя в базу
    cache.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    
    await message.answer(
        "👋 Привет! Я бот для загрузки видео из YouTube.\n\n"
        "📥 Просто отправь мне ссылку на видео, и я скачаю его для тебя.\n\n"
        "⚙️ Доступные команды:\n"
        "/help — справка\n"
        "/status — статус бота\n"
        "/ping — проверка связи",
        parse_mode="Markdown"
    )
    logger.info(f"Команда /start от {message.from_user.id}")


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Обработчик команды /help."""
    await message.answer(
        "📖 **Инструкция по использованию:**\n\n"
        "1️⃣ Отправь ссылку на YouTube видео\n"
        "2️⃣ Выбери нужное качество из списка\n"
        "3️⃣ Бот скачает и отправит видео\n\n"
        "📹 **Доступные качества:**\n"
        "- 144p, 240p, 360p\n"
        "- 480p, 720p (HD)\n"
        "- 1080p (FHD), 1440p (2K), 2160p (4K)\n"
        "- 🎵 Только аудио (MP3)\n\n"
        "⚡ **Кэширование:**\n"
        "Повторные запросы отправляются мгновенно из кэша!\n"
        "Закэшированные форматы отмечены ✅\n\n"
        "👑 **Команды:**\n"
        "/start — Приветствие\n"
        "/help — Эта справка\n"
        "/ping — Проверка связи\n"
        "/status — Статистика кэша\n"
        "/admin — Админ-панель (только админ)\n"
        "/clear — Очистка кэша (только админ)\n"
        "/stats — Подробная статистика (только админ)\n"
        "/users — Список пользователей (только админ)\n"
        "/ban, /unban — Бан/разбан (только админ)\n"
        "/broadcast — Рассылка (только админ)",
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

    # Проверяем, не забанен ли пользователь
    if cache.is_banned(message.from_user.id):
        await message.answer("🚫 Вы заблокированы администратором!")
        logger.warning(f"Забаненный пользователь попытался загрузить видео: {message.from_user.id}")
        return

    # Добавляем/обновляем пользователя
    cache.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )

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

    # Получаем закэшированные форматы
    cached_formats = cache.get_all_for_video(video_id)
    logger.info(f"Найдено {len(cached_formats)} закэшированных форматов для видео {video_id}")

    # Создаём клавиатуру с отметками кэша
    keyboard = build_quality_keyboard(video_id, available, cached_formats)

    # Сохраняем URL в кэш
    url_cache[video_id] = url
    
    # Сохраняем метаданные видео
    video_metadata_cache[video_id] = {
        "title": title,
        "uploader": uploader,
        "duration": duration
    }

    # Формируем описание (без ссылки в названии — ссылка будет в загруженном видео)
    caption = (
        f"🎬 **{title}**\n\n"
        f"👤 {uploader}\n"
        f"⏱ Длительность: {duration_str}\n\n"
        f"**Выберите качество:**"
    )

    await status_msg.edit_text(
        caption,
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

    # Проверяем кэш видео
    cached = cache.get(video_id, format_code)
    if cached:
        # Уже есть в кэше — отправляем по file_id
        logger.info(f"Отправка из кэша: {video_id} / {format_code}")
        
        # Получаем описание качества из кэша или определяем по format_code
        quality_desc = cached.get("quality_label", "")
        
        # Если quality_label пустой или равен format_code, определяем по format_code
        if not quality_desc or quality_desc == format_code or quality_desc.isdigit():
            # Определяем качество по format_code
            if format_code == "bestaudio":
                quality_desc = "Только аудио"
            # Видео + аудио (merged formats)
            elif "278" in format_code:
                quality_desc = "144p"
            elif "242" in format_code or "271" in format_code:
                quality_desc = "240p"
            elif "243" in format_code or "272" in format_code:
                quality_desc = "360p"
            elif "244" in format_code or "135" in format_code:
                quality_desc = "480p"
            elif "247" in format_code or "136" in format_code:
                quality_desc = "720p (HD)"
            elif "248" in format_code or "137" in format_code:
                quality_desc = "1080p (FHD)"
            elif "271" in format_code:
                quality_desc = "1440p (2K)"
            elif "313" in format_code:
                quality_desc = "2160p (4K)"
            # Альтернативные format_id
            elif "160" in format_code:
                quality_desc = "144p"
            elif "133" in format_code:
                quality_desc = "240p"
            elif "134" in format_code:
                quality_desc = "360p"
            elif "135" in format_code:
                quality_desc = "480p"
            elif "136" in format_code:
                quality_desc = "720p (HD)"
            elif "137" in format_code:
                quality_desc = "1080p (FHD)"
            elif "264" in format_code:
                quality_desc = "1440p (2K)"
            elif "266" in format_code:
                quality_desc = "2160p (4K)"
            else:
                quality_desc = format_code.replace("+bestaudio", "")
        
        # Форматируем длительность
        duration = cached.get("duration", 0)
        if duration:
            duration_str = f"{duration // 60}:{duration % 60:02d}"
        else:
            duration_str = "N/A"
        
        # Формируем красивое описание со ссылкой на источник
        title = cached.get("title", "Видео")
        uploader = cached.get("uploader", "Неизвестно")
        
        # Получаем URL из кэша
        source_url = url_cache.get(video_id, f"https://www.youtube.com/watch?v={video_id}")
        
        caption = (
            f"🎬 **[{title}]({source_url})**\n\n"
            f"👤 {uploader}\n"
            f"⏱ Длительность: {duration_str}\n"
            f"📹 Качество: {quality_desc}"
        )
        
        try:
            await callback.message.answer_video(
                video=cached["file_id"],
                caption=caption,
                parse_mode="Markdown"
            )
            # Логируем запрос
            cache.log_request(
                callback.from_user.id, video_id, format_code,
                cached.get("file_size", 0), from_cache=True
            )
            # Удаляем сообщение с кнопками
            await callback.message.delete()
            await callback.answer("✅ Отправлено")
        except Exception as e:
            logger.error(f"Ошибка отправки из кэша: {e}")
            await callback.answer("❌ Ошибка при отправке из кэша", show_alert=True)
        return

    # Определяем описание качества для сохранения в кэш
    if format_code == "bestaudio":
        quality_desc = "Только аудио"
    # Видео + аудио (merged formats)
    elif "278" in format_code:
        quality_desc = "144p"
    elif "242" in format_code or "271" in format_code:
        quality_desc = "240p"
    elif "243" in format_code or "272" in format_code:
        quality_desc = "360p"
    elif "244" in format_code or "135" in format_code:
        quality_desc = "480p"
    elif "247" in format_code or "136" in format_code:
        quality_desc = "720p (HD)"
    elif "248" in format_code or "137" in format_code:
        quality_desc = "1080p (FHD)"
    elif "271" in format_code:
        quality_desc = "1440p (2K)"
    elif "313" in format_code:
        quality_desc = "2160p (4K)"
    # Альтернативные format_id
    elif "160" in format_code:
        quality_desc = "144p"
    elif "133" in format_code:
        quality_desc = "240p"
    elif "134" in format_code:
        quality_desc = "360p"
    elif "135" in format_code:
        quality_desc = "480p"
    elif "136" in format_code:
        quality_desc = "720p (HD)"
    elif "137" in format_code:
        quality_desc = "1080p (FHD)"
    elif "264" in format_code:
        quality_desc = "1440p (2K)"
    elif "266" in format_code:
        quality_desc = "2160p (4K)"
    else:
        quality_desc = format_code.replace("+bestaudio", "")

    # Получаем размер из описания
    size_match = re.search(r'\((\d+) MB\)', callback.message.text)
    size_info = f" (~{size_match.group(1)} MB)" if size_match else ""

    # Получаем оригинальное описание (без "Выберите качество:")
    original_text = callback.message.text
    if "Выберите качество:" in original_text:
        original_text = original_text.split("Выберите качество:")[0].strip()

    await callback.message.edit_text(
        f"{original_text}\n\n⏳ Скачиваю в качестве {quality_desc}{size_info}...",
        parse_mode="Markdown"
    )

    await callback.answer(f"Начинаю загрузку ({quality_desc})...")

    # Скачиваем видео
    filepath, title = await download_video(url, format_code)

    if not filepath:
        await callback.message.edit_text(
            f"{original_text}\n\n❌ Ошибка при загрузке видео."
        )
        return

    # Проверяем размер
    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        await callback.message.edit_text(
            f"{original_text}\n\n❌ Файл слишком большой ({file_size / 1024 / 1024:.1f} MB)."
        )
        return

    # Отправляем видео через локальный Bot API Server
    try:
        if BOT_API_SERVER_URL:
            # Используем aiohttp для прямой отправки через локальный API
            import aiohttp
            import aiofiles

            # Читаем файл
            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            # Отправляем через локальный API
            api_url = f"{BOT_API_SERVER_URL}/sendVideo"

            data = aiohttp.FormData()
            data.add_field('chat_id', str(callback.message.chat.id))
            data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')
            
            # Формируем caption со ссылкой на источник
            metadata = video_metadata_cache.get(video_id, {})
            duration = metadata.get('duration', 0)
            duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
            uploader = metadata.get('uploader', 'Неизвестно')
            
            caption = (
                f"🎬 **[{title}]({url})**\n\n"
                f"👤 {uploader}\n"
                f"⏱ Длительность: {duration_str}\n"
                f"📹 Качество: {quality_desc}"
            )
            data.add_field('caption', caption)
            data.add_field('parse_mode', 'Markdown')

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, data=data) as response:
                    result = await response.json()
                    if not result.get('ok'):
                        raise Exception(f"Bot API error: {result.get('description', 'Unknown error')}")
                    
                    # Сохраняем file_id в кэш
                    file_id = result.get('result', {}).get('video', {}).get('file_id')
                    if file_id:
                        # Получаем метаданные из кэша
                        metadata = video_metadata_cache.get(video_id, {})
                        cache.set(
                            video_id, format_code, file_id, file_size, quality_desc,
                            metadata.get('title', ''), metadata.get('duration', 0),
                            metadata.get('uploader', '')
                        )
                        logger.info(f"Сохранено в кэш: {video_id} / {format_code} → {file_id[:20]}...")

            await callback.message.delete()
        else:
            # Публичный API
            video = FSInputFile(filepath)
            
            # Формируем caption со ссылкой на источник
            metadata = video_metadata_cache.get(video_id, {})
            duration = metadata.get('duration', 0)
            duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
            uploader = metadata.get('uploader', 'Неизвестно')
            
            caption = (
                f"🎬 **[{title}]({url})**\n\n"
                f"👤 {uploader}\n"
                f"⏱ Длительность: {duration_str}\n"
                f"📹 Качество: {quality_desc}"
            )
            
            msg = await callback.message.answer_video(
                video,
                caption=caption,
                parse_mode="Markdown"
            )
            # Получаем метаданные из кэша
            metadata = video_metadata_cache.get(video_id, {})
            # Сохраняем file_id в кэш
            cache.set(
                video_id, format_code, msg.video.file_id, file_size, quality_desc,
                metadata.get('title', ''), metadata.get('duration', 0),
                metadata.get('uploader', '')
            )
            # Логируем запрос
            cache.log_request(callback.from_user.id, video_id, format_code, file_size, from_cache=False)
            logger.info(f"Сохранено в кэш: {video_id} / {format_code}")
            await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка при отправке: {e}")
        await callback.message.answer(f"{original_text}\n\n❌ Ошибка при отправке: {e}")
    finally:
        await cleanup_file(filepath)


@dp.callback_query(F.data.startswith("cancel_"))
async def handle_cancel(callback: types.CallbackQuery):
    """Обработчик отмены загрузки."""
    await callback.message.delete()
    await callback.answer("Загрузка отменена")


@dp.message(Command("status"))
async def cmd_cache_status(message: types.Message):
    """Обработчик команды /status — статистика кэша."""
    stats = cache.get_stats()
    
    # Форматируем размер
    total_size = stats["total_size"]
    if total_size > 1024 * 1024 * 1024:
        size_str = f"{total_size / 1024 / 1024 / 1024:.2f} GB"
    elif total_size > 1024 * 1024:
        size_str = f"{total_size / 1024 / 1024:.2f} MB"
    else:
        size_str = f"{total_size / 1024:.2f} KB"
    
    await message.answer(
        f"📊 **Статистика кэша:**\n\n"
        f"🎬 Видео: {stats['total_videos']}\n"
        f"📹 Форматов: {stats['total_files']}\n"
        f"💾 Общий размер: {size_str}\n\n"
        f"_file_id хранятся в Telegram, локальные файлы не хранятся._",
        parse_mode="Markdown"
    )
    logger.info(f"Команда /status от {message.from_user.id}")


@dp.message(Command("clear"))
async def cmd_cache_clear(message: types.Message):
    """Обработчик команды /clear — очистка кэша."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор может очищать кэш!")
        return
    
    count = cache.clear()
    await message.answer(f"🗑 Кэш очищен! Удалено записей: {count}")
    logger.info(f"Кэш очищен пользователем {message.from_user.id}: {count} записей")


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Обработчик команды /stats — подробная статистика (админ)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор!")
        return
    
    stats = cache.get_detailed_stats()
    top_users = cache.get_top_users(5)
    
    # Форматируем размер кэша
    cache_size = stats["cache_size"]
    if cache_size > 1024 * 1024 * 1024:
        size_str = f"{cache_size / 1024 / 1024 / 1024:.2f} GB"
    elif cache_size > 1024 * 1024:
        size_str = f"{cache_size / 1024 / 1024:.2f} MB"
    else:
        size_str = f"{cache_size / 1024:.2f} KB"
    
    # Процент кэш-попаданий
    total_req = stats["total_requests"]
    cache_hit_rate = (stats["cache_hits"] / total_req * 100) if total_req > 0 else 0
    
    text = (
        f"📊 **Подробная статистика:**\n\n"
        f"👥 **Пользователи:**\n"
        f"   Всего: {stats['total_users']}\n"
        f"   Активные: {stats['active_users']}\n"
        f"   Забанены: {stats['banned_users']}\n\n"
        f"📥 **Запросы:**\n"
        f"   Всего: {stats['total_requests']}\n"
        f"   Из кэша: {stats['cache_hits']} ({cache_hit_rate:.1f}%)\n"
        f"   Загрузок: {stats['cache_misses']}\n\n"
        f"💾 **Кэш:**\n"
        f"   Файлов: {stats['cached_files']}\n"
        f"   Размер: {size_str}\n\n"
    )
    
    if top_users:
        text += "🏆 **Топ пользователей:**\n"
        for i, user in enumerate(top_users, 1):
            name = user.get('username') or user.get('first_name') or f"User {user['user_id']}"
            text += f"   {i}. {name}: {user['request_count']} запросов\n"
    
    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /stats от {message.from_user.id}")


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    """Обработчик команды /broadcast — рассылка всем пользователям (админ)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор!")
        return
    
    # Проверяем, есть ли текст после команды
    text = message.text.replace("/broadcast", "").strip()
    if not text:
        await message.answer(
            "📢 **Рассылка всем пользователям**\n\n"
            "Отправь текст сообщения после команды:\n"
            "`/broadcast Текст сообщения...`\n\n"
            "Можно использовать Markdown.",
            parse_mode="Markdown"
        )
        return
    
    # Получаем всех пользователей
    users = cache.get_all_users()
    banned_count = sum(1 for u in users if u.get('is_banned'))
    active_users = [u for u in users if not u.get('is_banned')]
    
    await message.answer(f"📢 Начинаю рассылку {len(active_users)} пользователям...")
    
    success = 0
    errors = 0
    
    for user in active_users:
        try:
            await bot.send_message(
                user['user_id'],
                f"📢 **Сообщение от админа:**\n\n{text}",
                parse_mode="Markdown"
            )
            success += 1
        except Exception as e:
            logger.error(f"Не удалось отправить пользователю {user['user_id']}: {e}")
            errors += 1
        
        # Небольшая задержка чтобы не спамить
        import asyncio
        await asyncio.sleep(0.1)
    
    await message.answer(
        f"✅ Рассылка завершена!\n\n"
        f"Отправлено: {success}\n"
        f"Ошибок: {errors}"
    )
    logger.info(f"Рассылка от {message.from_user.id}: {success} успешно, {errors} ошибок")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    """Обработчик команды /ban — заблокировать пользователя (админ)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор!")
        return
    
    # Проверяем, есть ли ID пользователя
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "🚫 **Бан пользователя**\n\n"
            "Использование: `/ban user_id`\n\n"
            "Пример: `/ban 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Неверный формат ID!")
        return
    
    if user_id == ADMIN_ID:
        await message.answer("⛔ Нельзя забанить администратора!")
        return
    
    cache.ban_user(user_id)
    await message.answer(f"🚫 Пользователь {user_id} забанен!")
    logger.info(f"Пользователь {user_id} забанен админом {message.from_user.id}")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    """Обработчик команды /unban — разблокировать пользователя (админ)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор!")
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer(
            "✅ **Разбан пользователя**\n\n"
            "Использование: `/unban user_id`\n\n"
            "Пример: `/unban 123456789`",
            parse_mode="Markdown"
        )
        return
    
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Неверный формат ID!")
        return
    
    cache.unban_user(user_id)
    await message.answer(f"✅ Пользователь {user_id} разбанен!")
    logger.info(f"Пользователь {user_id} разбанен админом {message.from_user.id}")


@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    """Обработчик команды /users — список всех пользователей (админ)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Только администратор!")
        return
    
    users = cache.get_all_users()
    
    if not users:
        await message.answer("📋 Пользователей пока нет.")
        return
    
    text = f"👥 **Пользователи ({len(users)}):**\n\n"
    for i, user in enumerate(users[:20], 1):  # Показываем первые 20
        status = "🚫" if user.get('is_banned') else "✅"
        name = user.get('username') or user.get('first_name') or f"User {user['user_id']}"
        text += f"{i}. {status} {name} (`{user['user_id']}`)\n"
    
    if len(users) > 20:
        text += f"\n... и ещё {len(users) - 20} пользователей"
    
    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /users от {message.from_user.id}")


async def main():
    """Основная функция запуска бота."""
    # Создаём директорию для загрузок
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
    
    logger.info("Запуск бота...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
