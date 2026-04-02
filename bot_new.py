"""
YouTube Downloader Bot — Telegram-бот для загрузки видео из YouTube.
Новая версия с упрощённой схемой БД (без кэшей в памяти).
"""
import asyncio
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from yt_dlp import YoutubeDL

from database import VideoCache

from queue_manager import (
    init_queue_manager,
    get_queue_manager,
    FairQueueManager,
    DownloadTask,
    TaskStatus
)

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
    QUEUE_YOUTUBE_RPS,
    QUEUE_TELEGRAM_UPLOADS_PER_MIN,
    QUEUE_MAX_CONCURRENT,
    QUEUE_MAX_PER_USER,
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
    from aiogram.client.session.aiohttp import AiohttpSession
    api_server = BOT_API_SERVER_URL.rsplit('/bot', 1)[0]
    session = AiohttpSession()
    session.api_server = api_server
    bot = Bot(token=BOT_TOKEN, session=session)
    logger.info(f"Используется локальный Bot API Server: {api_server}")
else:
    bot = Bot(token=BOT_TOKEN)
    logger.info("Используется публичный Telegram API")

dp = Dispatcher()

# Инициализация кэша (БД)
cache = VideoCache(CACHE_DB_PATH)
logger.info(f"Кэш видео инициализирован: {CACHE_DB_PATH}")

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

# ⚠️ КЭШИ В ПАМЯТИ УДАЛЕНЫ - ВСЁ ХРАНИТСЯ ТОЛЬКО В БД

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

# Опции для yt-dlp (для поиска)
YDL_SEARCH_OPTIONS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "default_search": "ytsearch10",
}


def is_youtube_url(url: str) -> bool:
    """Проверяет, является ли ссылка YouTube URL."""
    return bool(YOUTUBE_PATTERN.match(url))


def get_video_info(url: str) -> Optional[dict]:
    """Получает информацию о видео без загрузки."""
    try:
        with YoutubeDL(YDL_INFO_OPTIONS) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception as e:
        logger.error(f"Ошибка при получении информации: {e}")
        return None


async def search_youtube_videos(query: str, limit: int = 10, language: str = "ru") -> List[dict]:
    """Ищет видео на YouTube по поисковому запросу."""
    try:
        options = YDL_SEARCH_OPTIONS.copy()
        search_query = f"{query}"
        if language == "ru":
            search_query = f"{query} русский язык"

        options["default_search"] = f"ytsearch{limit}"

        loop = asyncio.get_event_loop()

        def _search():
            with YoutubeDL(options) as ydl:
                result = ydl.extract_info(f"ytsearch{limit}:{search_query}", download=False)
                return result.get('entries', [])

        entries = await loop.run_in_executor(None, _search)

        videos = []
        for entry in entries:
            if entry and entry.get('id'):
                videos.append({
                    'id': entry.get('id'),
                    'title': entry.get('title', 'Без названия'),
                    'uploader': entry.get('uploader', 'Неизвестно'),
                    'duration': entry.get('duration', 0),
                    'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                    'type': 'video'
                })

        logger.info(f"Найдено {len(videos)} видео по запросу '{search_query}'")
        return videos

    except Exception as e:
        logger.error(f"Ошибка при поиске видео: {e}")
        return []


async def search_youtube_shorts(query: str, limit: int = 10, language: str = "ru") -> List[dict]:
    """Ищет Shorts на YouTube по поисковому запросу."""
    try:
        options = YDL_SEARCH_OPTIONS.copy()
        search_query = f"{query} shorts"
        if language == "ru":
            search_query = f"{query} shorts русский"

        options["default_search"] = f"ytsearch{limit}"

        loop = asyncio.get_event_loop()

        def _search():
            with YoutubeDL(options) as ydl:
                result = ydl.extract_info(f"ytsearch{limit}:{search_query}", download=False)
                return result.get('entries', [])

        entries = await loop.run_in_executor(None, _search)

        shorts = []
        for entry in entries:
            if entry and entry.get('id'):
                duration = entry.get('duration', 0)
                if duration and duration <= 60:
                    shorts.append({
                        'id': entry.get('id'),
                        'title': entry.get('title', 'Без названия'),
                        'uploader': entry.get('uploader', 'Неизвестно'),
                        'duration': duration,
                        'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                        'type': 'shorts'
                    })

        if len(shorts) < limit:
            for entry in entries:
                if len(shorts) >= limit:
                    break
                if entry and entry.get('id') and entry not in shorts:
                    duration = entry.get('duration', 0)
                    if duration and duration <= 180:
                        shorts.append({
                            'id': entry.get('id'),
                            'title': entry.get('title', 'Без названия'),
                            'uploader': entry.get('uploader', 'Неизвестно'),
                            'duration': duration,
                            'url': f"https://www.youtube.com/watch?v={entry.get('id')}",
                            'type': 'shorts'
                        })

        logger.info(f"Найдено {len(shorts)} shorts по запросу '{search_query}'")
        return shorts

    except Exception as e:
        logger.error(f"Ошибка при поиске shorts: {e}")
        return []


def build_quality_keyboard(
    video_db_id: int,
    formats: list,
    cached_formats: list = None
) -> InlineKeyboardMarkup:
    """
    Строит inline-клавиатуру с вариантами качества.

    Args:
        video_db_id: ID видео в БД (не YouTube ID!)
        formats: список кортежей (format_code, description, height, estimated_size)
        cached_formats: список (format_code, quality_label) уже закэшированных форматов
    """
    builder = InlineKeyboardBuilder()

    cached_set = set(fmt[0] for fmt in (cached_formats or []))

    for fmt_code, description, height, est_size in formats:
        if est_size > MAX_FILE_SIZE:
            continue

        if fmt_code in cached_set:
            builder.button(
                text=f"✅ {description}",
                callback_data=f"download_{video_db_id}_{fmt_code}"
            )
        else:
            builder.button(
                text=f"📹 {description}",
                callback_data=f"download_{video_db_id}_{fmt_code}"
            )

    builder.button(text="❌ Отмена", callback_data=f"cancel_{video_db_id}")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def get_available_formats(formats: list, max_size_mb: int = 48, max_height: int = 1080) -> list:
    """Извлекает доступные форматы из информации о видео."""
    available = []
    max_size_bytes = max_size_mb * 1024 * 1024

    for fmt in formats:
        if fmt.get('vcodec') == 'none':
            continue

        height = fmt.get('height', 0)
        if not height:
            continue

        if height > max_height:
            continue

        format_id = fmt.get('format_id', '')
        filesize = fmt.get('filesize', 0) or fmt.get('filesize_approx', 0)

        audio_size = 0
        for audio_fmt in formats:
            if audio_fmt.get('acodec') != 'none' and audio_fmt.get('vcodec') == 'none':
                audio_size = audio_fmt.get('filesize', 0) or audio_fmt.get('filesize_approx', 0)
                break

        total_size = filesize + audio_size if filesize else 0

        if total_size > max_size_bytes:
            continue

        size_str = f" ({total_size / 1024 / 1024:.0f} MB)" if total_size else ""

        quality_label = f"{height}p"

        if format_id in ("160", "278"):
            quality_label = "144p"
        elif format_id in ("133", "242"):
            quality_label = "240p"
        elif format_id in ("134", "243"):
            quality_label = "360p"
        elif format_id in ("135", "244"):
            quality_label = "480p"
        elif format_id in ("136", "247"):
            quality_label = "720p (HD)"
        elif format_id in ("137", "248"):
            quality_label = "1080p (FHD)"
        elif format_id in ("264", "271", "308"):
            quality_label = "1440p (2K)"
        elif format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
            quality_label = "2160p (4K)"
        elif format_id in ("272", "309", "316"):
            quality_label = "4320p (8K)"
        elif height >= 4320:
            quality_label = f"{height}p (8K)"
        elif height >= 2160:
            quality_label = f"{height}p (4K)"
        elif height >= 1440:
            quality_label = f"{height}p (2K)"
        elif height >= 1080:
            quality_label = f"{height}p (FHD)"
        elif height >= 720:
            quality_label = f"{height}p (HD)"

        desc = f"{quality_label}{size_str}"
        available.append((f"{format_id}+bestaudio", desc, height, total_size))

    seen_heights = set()
    unique = []
    for fmt in sorted(available, key=lambda x: x[2], reverse=True):
        if fmt[2] not in seen_heights:
            seen_heights.add(fmt[2])
            unique.append(fmt)

    unique.append(("bestaudio", "🎵 Только аудио", 0, 0))

    return unique[:8]


async def download_video(url: str, format_code: str = "best") -> Tuple[Optional[Path], Optional[str]]:
    """Скачивает видео с YouTube."""
    loop = asyncio.get_event_loop()

    options = YDL_OPTIONS.copy()
    options["format"] = format_code

    def _download():
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(info)
            if Path(filename).exists():
                return Path(filename), info.get('title', 'video')
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
        "/status — статус бота",
        parse_mode="Markdown"
    )
    logger.info(f"Команда /start от {message.from_user.id}")


@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Обработчик команды /help."""
    is_admin = message.from_user.id == ADMIN_ID

    if is_admin:
        await message.answer(
            "👑 **Команды администратора:**\n\n"
            "/clear — Очистка кэша\n"
            "/stats — Подробная статистика\n"
            "/users — Список пользователей\n"
            "/ban, /unban — Бан/разбан пользователей\n"
            "/broadcast — Рассылка\n\n"
            "⚙️ **Лимиты:**\n"
            f"• YouTube: ~{QUEUE_YOUTUBE_RPS} запрос/сек\n"
            f"• Telegram: ~{QUEUE_TELEGRAM_UPLOADS_PER_MIN} загрузок/мин\n"
            f"• Макс. одновременных загрузок: {QUEUE_MAX_CONCURRENT}\n"
            f"• Макс. в очереди на пользователя: {QUEUE_MAX_PER_USER}",
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            "📖 **Инструкция по использованию:**\n\n"
            "1️⃣ Отправь ссылку на YouTube видео\n"
            "2️⃣ Выбери нужное качество из списка\n"
            "3️⃣ Бот скачает и отправит видео\n\n"
            "🔍 **Поиск видео:**\n"
            "Просто отправь текстовый запрос (например: Steam Deck OLED)\n"
            "Бот найдёт 10 видео + 10 shorts и отправит их!\n\n"
            "🌐 **Язык поиска:**\n"
            "/lang — Выбрать язык поиска (русский/English)\n\n"
            "📹 **Доступные качества:**\n"
            "- 144p, 240p, 360p\n"
            "- 480p, 720p (HD)\n"
            "- 1080p (FHD), 1440p (2K), 2160p (4K)\n"
            "- 🎵 Только аудио (MP3)\n\n"
            "⚡ **Кэширование:**\n"
            "Повторные запросы отправляются мгновенно из кэша!\n"
            "Закэшированные форматы отмечены ✅",
            parse_mode="Markdown"
        )
    logger.info(f"Команда /help от {message.from_user.id}")


@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    """Обработчик команды /ping."""
    await message.answer("🏓 Понг! Бот на связи!")
    logger.info(f"Команда /ping от {message.from_user.id}")


@dp.message(Command("lang"))
async def cmd_lang(message: types.Message):
    """Обработчик команды /lang."""
    current_lang = cache.get_user_language(message.from_user.id)
    current_lang_name = "🇷🇺 Русский" if current_lang == "ru" else "🇬🇧 English"

    builder = InlineKeyboardBuilder()
    builder.button(text="🇷🇺 Русский", callback_data="lang_ru")
    builder.button(text="🇬🇧 English", callback_data="lang_en")
    builder.adjust(2)

    await message.answer(
        f"🌐 **Выбор языка поиска**\n\n"
        f"Текущий язык: {current_lang_name}\n\n"
        f"Выберите предпочтительный язык для поиска видео:",
        parse_mode="Markdown",
        reply_markup=builder.as_markup()
    )
    logger.info(f"Команда /lang от {message.from_user.id}")


@dp.callback_query(F.data.startswith("lang_"))
async def handle_lang_change(callback: types.CallbackQuery):
    """Обработчик выбора языка."""
    language = callback.data.split("_")[1]
    cache.set_user_language(callback.from_user.id, language)
    lang_name = "🇷🇺 Русский" if language == "ru" else "🇬🇧 English"

    await callback.message.edit_text(
        f"✅ **Язык изменён**\n\n"
        f"Теперь поиск будет выполняться на языке: {lang_name}\n\n"
        f"Теперь отправьте любой текстовый запрос для поиска видео!",
        parse_mode="Markdown"
    )
    await callback.answer(f"Язык: {lang_name}")
    logger.info(f"Язык изменён пользователем {callback.from_user.id}: {language}")


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Обработчик команды /admin."""
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


@dp.message(Command("status"))
async def cmd_cache_status(message: types.Message):
    """Обработчик команды /status."""
    try:
        # Пробуем получить статистику по новой схеме
        new_stats = cache.get_new_stats()
        
        if new_stats:
            # Новая схема
            cache_size = new_stats.get("total_size", 0)
            if cache_size > 1024 * 1024 * 1024:
                cache_size_str = f"{cache_size / 1024 / 1024 / 1024:.2f} GB"
            elif cache_size > 1024 * 1024:
                cache_size_str = f"{cache_size / 1024 / 1024:.2f} MB"
            else:
                cache_size_str = f"{cache_size / 1024:.2f} KB"

            await message.answer(
                f"📊 **Статистика:**\n\n"
                f"💾 **Кэш Telegram:**\n"
                f"   Видео: {new_stats.get('total_videos', 0)}\n"
                f"   Форматов: {new_stats.get('total_formats', 0)}\n"
                f"   Размер: {cache_size_str}\n\n"
                f"📥 **Загрузки:**\n"
                f"   Завершено: {new_stats.get('completed_formats', 0)}\n"
                f"   В очереди: {new_stats.get('pending_formats', 0)}",
                parse_mode="Markdown"
            )
        else:
            # Старая схема
            cache_stats = cache.get_stats()
            with sqlite3.connect(cache.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*), COUNT(DISTINCT video_id), COALESCE(SUM(file_size), 0) FROM requests")
                row = cursor.fetchone()
                total_downloads = row[0]
                unique_videos_downloaded = row[1]
                total_size_downloaded = row[2]

            cache_size = cache_stats["total_size"]
            if cache_size > 1024 * 1024 * 1024:
                cache_size_str = f"{cache_size / 1024 / 1024 / 1024:.2f} GB"
            elif cache_size > 1024 * 1024:
                cache_size_str = f"{cache_size / 1024 / 1024:.2f} MB"
            else:
                cache_size_str = f"{cache_size / 1024:.2f} KB"

            if total_size_downloaded > 1024 * 1024 * 1024:
                total_size_str = f"{total_size_downloaded / 1024 / 1024 / 1024:.2f} GB"
            elif total_size_downloaded > 1024 * 1024:
                total_size_str = f"{total_size_downloaded / 1024 / 1024:.2f} MB"
            else:
                total_size_str = f"{total_size_downloaded / 1024:.2f} KB"

            await message.answer(
                f"📊 **Статистика:**\n\n"
                f"💾 **Кэш Telegram:**\n"
                f"   Видео: {cache_stats['total_videos']}\n"
                f"   Форматов: {cache_stats['total_files']}\n"
                f"   Размер: {cache_size_str}\n\n"
                f"📥 **Всего загрузок:**\n"
                f"   Запросов: {total_downloads}\n"
                f"   Видео: {unique_videos_downloaded}\n"
                f"   Общий размер: {total_size_str}",
                parse_mode="Markdown"
            )
        logger.info(f"Команда /status от {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка в /status: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("clear"))
async def cmd_cache_clear(message: types.Message):
    """Обработчик команды /clear."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    count = cache.clear()
    await message.answer(f"🗑 Кэш очищен! Удалено записей: {count}")
    logger.info(f"Кэш очищен пользователем {message.from_user.id}: {count} записей")


@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Обработчик команды /stats."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    stats = cache.get_detailed_stats()
    top_users = cache.get_top_users(5)

    cache_size = stats["cache_size"]
    if cache_size > 1024 * 1024 * 1024:
        size_str = f"{cache_size / 1024 / 1024 / 1024:.2f} GB"
    elif cache_size > 1024 * 1024:
        size_str = f"{cache_size / 1024 / 1024:.2f} MB"
    else:
        size_str = f"{cache_size / 1024:.2f} KB"

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
            text += f"   {i}. {name}: {user['video_count']} видео\n"

    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /stats от {message.from_user.id}")


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    """Обработчик команды /broadcast."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

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

        await asyncio.sleep(0.1)

    await message.answer(
        f"✅ Рассылка завершена!\n\n"
        f"Отправлено: {success}\n"
        f"Ошибок: {errors}"
    )
    logger.info(f"Рассылка от {message.from_user.id}: {success} успешно, {errors} ошибок")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    """Обработчик команды /ban."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

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
    """Обработчик команды /unban."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
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
    """Обработчик команды /users."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    users = cache.get_all_users()

    if not users:
        await message.answer("📋 Пользователей пока нет.")
        return

    text = f"👥 **Пользователи ({len(users)}):**\n\n"
    for i, user in enumerate(users[:20], 1):
        status = "🚫" if user.get('is_banned') else "✅"
        name = user.get('username') or user.get('first_name') or f"User {user['user_id']}"
        text += f"{i}. {status} {name} (`{user['user_id']}`)\n"

    if len(users) > 20:
        text += f"... и ещё {len(users) - 20} пользователей"

    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /users от {message.from_user.id}")


@dp.message(Command("queue"))
async def cmd_queue(message: types.Message):
    """Обработчик команды /queue."""
    user_status = await queue_mgr.get_user_queue_status(message.from_user.id)
    queue_stats = queue_mgr.get_stats()

    if user_status["queue_size"] == 0:
        await message.answer(
            f"📊 **Ваша очередь:**\n\n"
            f"✅ У вас нет активных загрузок.\n\n"
            f"📈 **Общая статистика:**\n"
            f"• В очереди: {queue_stats['queued']}\n"
            f"• Активных загрузок: {queue_stats['active']}\n"
            f"• Обработано: {queue_stats['total_processed']}\n"
            f"• Ошибок: {queue_stats['total_failed']}",
            parse_mode="Markdown"
        )
        return

    text = f"📊 **Ваша очередь ({user_status['queue_size']}/{user_status['limit']}):**\n\n"

    for i, task_info in enumerate(user_status["tasks"][:10], 1):
        status_emoji = {
            "pending": "⏳",
            "queued": "📋",
            "downloading": "📥",
            "uploading": "📤",
            "completed": "✅",
            "failed": "❌",
            "cancelled": "⛔"
        }.get(task_info["status"], "❓")

        text += f"{i}. {status_emoji} **{task_info['quality']}** ({task_info['video_id'][:11]}...)\n"
        text += f"   Статус: {task_info['status']}\n\n"

    if len(user_status["tasks"]) > 10:
        text += f"... и ещё {len(user_status['tasks']) - 10} задач\n\n"

    text += (
        f"📈 **Общая статистика:**\n"
        f"• В очереди: {queue_stats['queued']}\n"
        f"• Активных загрузок: {queue_stats['active']}\n"
        f"• Обработано: {queue_stats['total_processed']}\n"
        f"• Ошибок: {queue_stats['total_failed']}"
    )

    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /queue от {message.from_user.id}")


@dp.message(Command("qstat"))
async def cmd_queue_stats(message: types.Message):
    """Обработчик команды /qstat."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    stats = queue_mgr.get_stats()
    db_stats = cache.get_queue_stats()

    text = (
        f"📊 **Статистика очереди загрузок:**\n\n"
        f"🔄 **Текущее состояние:**\n"
        f"• В очереди: {stats['queued']}\n"
        f"• Активных загрузок: {stats['active']}/{stats['max_concurrent']}\n\n"
        f"📈 **Обработано:**\n"
        f"• Успешно: {stats['total_processed']}\n"
        f"• Ошибок: {stats['total_failed']}\n"
        f"• Отменено: {stats['total_cancelled']}\n\n"
        f"⚙️ **Настройки:**\n"
        f"• YouTube RPS: {stats['youtube_rps']}\n"
        f"• Telegram uploads/min: {stats['telegram_uploads_per_min']}\n\n"
        f"🗄 **База данных:**\n"
        f"• Ожидает: {db_stats['pending']}\n"
        f"• Скачивается: {db_stats['downloading']}\n"
        f"• Загружается: {db_stats['uploading']}\n"
        f"• Завершено: {db_stats['completed']}\n"
        f"• Провалено: {db_stats['failed']}"
    )

    await message.answer(text, parse_mode="Markdown")
    logger.info(f"Команда /qstat от {message.from_user.id}")


# Глобальный словарь для хранения результатов поиска (только для текущей сессии)
search_results_cache: Dict[int, Dict[str, any]] = {}


@dp.message(F.text)
async def handle_text_message(message: types.Message):
    """Обработчик текстовых сообщений (поиск видео)."""
    text = message.text.strip()

    if text.startswith('/'):
        return

    if cache.is_banned(message.from_user.id):
        await message.answer("🚫 Вы заблокированы администратором!")
        logger.warning(f"Забаненный пользователь попытался выполнить поиск: {message.from_user.id}")
        return

    if is_youtube_url(text):
        await handle_url(message)
        return

    query = text

    cache.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )

    logger.info(f"Поисковый запрос от {message.from_user.id}: '{query}'")

    status_msg = await message.answer(
        f"🔍 Ищу видео по запросу: **{query}**\n\n"
        f"⏳ Пожалуйста, подождите...",
        parse_mode="Markdown"
    )

    try:
        user_language = cache.get_user_language(message.from_user.id)

        videos, shorts = await asyncio.gather(
            search_youtube_videos(query, limit=10, language=user_language),
            search_youtube_shorts(query, limit=10, language=user_language)
        )

        if not videos and not shorts:
            await status_msg.edit_text(
                f"❌ Ничего не найдено по запросу: **{query}**\n\n"
                f"Попробуйте другой запрос.",
                parse_mode="Markdown"
            )
            return

        search_results_cache[message.from_user.id] = {
            'query': query,
            'videos': videos,
            'shorts': shorts,
            'timestamp': datetime.now()
        }

        report_text = (
            f"✅ **Поиск завершён!**\n\n"
            f"🔍 Запрос: **{query}**\n\n"
            f"📹 Найдено видео: **{len(videos)}**\n"
            f"🎬 Найдено shorts: **{len(shorts)}**\n\n"
        )

        if videos:
            report_text += "\n📋 **Видео:**\n"
            for i, video in enumerate(videos[:10], 1):
                duration = int(video.get('duration', 0) or 0)
                duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
                title = video.get('title', 'Без названия')[:50]
                if len(video.get('title', '')) > 50:
                    title += "..."
                escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                report_text += f"{i}. 📹 [{escaped_title}]({video['url']}) ({duration_str})\n"

        if shorts:
            report_text += "\n🎬 **Shorts:**\n"
            for i, short in enumerate(shorts[:10], 1):
                duration = int(short.get('duration', 0) or 0)
                duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
                title = short.get('title', 'Без названия')[:50]
                if len(short.get('title', '')) > 50:
                    title += "..."
                escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                report_text += f"{i}. 🎬 [{escaped_title}]({short['url']}) ({duration_str})\n"

        report_text += "\n\n⏳ **Начинаю загрузку и отправку...**"

        await status_msg.edit_text(
            report_text,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

        await send_search_report(
            user_id=message.from_user.id,
            username=message.from_user.username,
            query=query,
            videos_count=len(videos),
            shorts_count=len(shorts),
            videos=videos,
            shorts=shorts
        )

        await process_search_results(
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            videos=videos,
            shorts=shorts,
            query=query
        )

    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}", exc_info=True)
        await status_msg.edit_text(
            f"❌ Произошла ошибка при поиске: {str(e)}\n\n"
            f"Попробуйте позже.",
            parse_mode="Markdown"
        )


async def send_search_report(
    user_id: int,
    username: str,
    query: str,
    videos_count: int,
    shorts_count: int,
    videos: List[dict],
    shorts: List[dict]
):
    """Отправляет отчёт о результатах поиска через server-bot."""
    import subprocess

    report = (
        f"🔍 **YouTube Search Report**\n\n"
        f"👤 User: @{username or user_id}\n"
        f"🔑 Query: `{query}`\n\n"
        f"📊 **Results:**\n"
        f"• Videos found: {videos_count}\n"
        f"• Shorts found: {shorts_count}\n\n"
    )

    if videos:
        report += "📹 **Top Videos:**\n"
        for i, v in enumerate(videos[:10], 1):
            title = v.get('title', 'N/A')[:60]
            duration = int(v.get('duration', 0) or 0)
            duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
            report += f"{i}. {title} ({duration_str})\n"
        report += "\n"

    if shorts:
        report += "🎬 **Top Shorts:**\n"
        for i, s in enumerate(shorts[:10], 1):
            title = s.get('title', 'N/A')[:60]
            duration = int(s.get('duration', 0) or 0)
            duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
            report += f"{i}. {title} ({duration_str})\n"

    report += f"\n✅ Search completed successfully!"

    try:
        script_path = "/root/git/server-bot/send_report.py"
        escaped_report = report.replace('"', '\\"').replace('$', '\\$')
        command = f'python3 {script_path} "{escaped_report}"'

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info(f"Отчёт о поиске отправлен через server-bot для пользователя {user_id}")
        else:
            logger.error(f"Ошибка отправки отчёта: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("Таймаут при отправке отчёта через server-bot")
    except Exception as e:
        logger.error(f"Ошибка при отправке отчёта: {e}")


async def process_search_results(
    user_id: int,
    chat_id: int,
    videos: List[dict],
    shorts: List[dict],
    query: str
):
    """Обрабатывает результаты поиска: добавляет видео в очередь."""
    total_added = 0
    total_failed = 0

    all_items = []

    for video in videos:
        all_items.append({
            'url': video['url'],
            'id': video['id'],
            'title': video['title'],
            'type': 'video',
            'duration': video.get('duration', 0)
        })

    for short in shorts:
        all_items.append({
            'url': short['url'],
            'id': short['id'],
            'title': short['title'],
            'type': 'shorts',
            'duration': short.get('duration', 0)
        })

    logger.info(f"Добавляю {len(all_items)} видео из поиска в очередь")

    for idx, item in enumerate(all_items):
        try:
            logger.info(f"Обработка [{idx+1}/{len(all_items)}]: {item['type']} - {item['title'][:50]}")

            info = get_video_info(item['url'])

            if not info:
                logger.warning(f"Не удалось получить информацию о видео: {item['url']}")
                total_failed += 1
                continue

            formats = info.get('formats', [])
            available = get_available_formats(
                formats,
                max_size_mb=MAX_FILE_SIZE // 1024 // 1024,
                max_height=720
            )

            if not available:
                logger.warning(f"Нет доступных форматов для видео: {item['url']}")
                total_failed += 1
                continue

            format_code, description, height, est_size = available[0]
            quality_desc = description.split(' (')[0] if ' (' in description else description

            # === НОВАЯ ЛОГИКА С БД ===
            # 1. Создаём или получаем видео из БД
            video_db_id = cache.create_video(
                source_url=item['url'],
                youtube_video_id=item['id'],
                title=item['title'],
                uploader=item.get('uploader', 'Неизвестно'),
                duration=item.get('duration', 0)
            )

            # 2. Создаём или получаем формат
            format_id = cache.create_or_get_format(
                video_id=video_db_id,
                format_code=format_code,
                quality_label=quality_desc,
                requested_by_user_id=user_id
            )

            # 3. Создаём задачу для очереди
            task = DownloadTask(
                task_id=f"search_{item['id']}_{format_code}_{int(datetime.now().timestamp())}_{total_added}",
                user_id=user_id,
                username=f"search_{user_id}",
                video_url=item['url'],
                video_id=item['id'],
                format_code=format_code,
                quality_label=quality_desc,
                callback_query=None
            )

            # 4. Добавляем в очередь
            added = await queue_mgr.add_task(task)

            if added:
                cache.log_queue_task(user_id, item['id'], format_code, "pending")
                total_added += 1
                logger.info(f"Задача добавлена в очередь: {task.task_id}")
            else:
                logger.warning(f"Не удалось добавить задачу в очередь: {item['id']}")
                total_failed += 1

            await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"Ошибка при добавлении {item['type']} в очередь: {e}", exc_info=True)
            total_failed += 1

    logger.info(f"Поиск завершён: добавлено {total_added} видео в очередь, ошибок: {total_failed}")

    try:
        await bot.send_message(
            chat_id,
            f"✅ **Добавлено в очередь:** {total_added} видео\n\n"
            f"📹 Видео будут отправлены по мере загрузки.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение о статусе очереди: {e}")


@dp.message(F.text)
async def handle_url(message: types.Message):
    """Обработчик ссылок на YouTube."""
    url = message.text.strip()

    if url.startswith('/'):
        return

    if not is_youtube_url(url):
        return

    if cache.is_banned(message.from_user.id):
        await message.answer("🚫 Вы заблокированы администратором!")
        logger.warning(f"Забаненный пользователь попытался загрузить видео: {message.from_user.id}")
        return

    cache.add_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )

    logger.info(f"Получена ссылка от {message.from_user.id}: {url}")

    status_msg = await message.answer("⏳ Получаю информацию о видео...")

    info = get_video_info(url)

    if not info:
        await status_msg.edit_text("❌ Не удалось получить информацию о видео. Проверьте ссылку.")
        return

    formats = info.get('formats', [])
    available = get_available_formats(formats, max_size_mb=MAX_FILE_SIZE // 1024 // 1024, max_height=1080)

    if not available:
        await status_msg.edit_text("❌ Нет доступных форматов для загрузки.")
        return

    title = info.get('title', 'Неизвестно')
    duration = info.get('duration', 0)
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
    uploader = info.get('uploader', 'Неизвестно')
    youtube_video_id = info.get('id', 'unknown')

    # === НОВАЯ ЛОГИКА С БД ===
    # 1. Создаём или получаем видео из БД
    video_db_id = cache.create_video(
        source_url=url,
        youtube_video_id=youtube_video_id,
        title=title,
        uploader=uploader,
        duration=duration
    )

    # 2. Получаем все форматы из БД
    cached_formats = cache.get_all_formats_for_video(video_db_id)
    logger.info(f"Найдено {len(cached_formats)} закэшированных форматов для видео {video_db_id}")

    # 3. Создаём клавиатуру с video_db_id вместо youtube_video_id
    keyboard = build_quality_keyboard(video_db_id, available, cached_formats)

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
    logger.info(f"Показаны варианты качества для видео {video_db_id}")


@dp.callback_query(F.data.startswith("download_"))
async def handle_download(callback: types.CallbackQuery):
    """Обработчик выбора качества."""
    callback_data = callback.data

    if not callback_data.startswith("download_"):
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    remainder = callback_data[9:]
    last_underscore = remainder.rfind("_")
    if last_underscore == -1:
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    # video_db_id - это ID видео в БД (не YouTube ID!)
    video_db_id = int(remainder[:last_underscore])
    format_code = remainder[last_underscore + 1:]

    if not video_db_id or not format_code:
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    # Проверяем кэш в БД
    cached = cache.get(video_db_id, format_code)
    if cached:
        # Уже есть в кэше — отправляем по file_id
        logger.info(f"Отправка из кэша: {video_db_id} / {format_code}")

        quality_desc = cached.get("quality_label", "")

        if not quality_desc or quality_desc == format_code or quality_desc.isdigit():
            main_format_id = format_code.split('+')[0]

            if format_code == "bestaudio":
                quality_desc = "Только аудио"
            elif main_format_id in ("160", "278"):
                quality_desc = "144p"
            elif main_format_id in ("133", "242"):
                quality_desc = "240p"
            elif main_format_id in ("134", "243"):
                quality_desc = "360p"
            elif main_format_id in ("135", "244"):
                quality_desc = "480p"
            elif main_format_id in ("136", "247"):
                quality_desc = "720p (HD)"
            elif main_format_id in ("137", "248"):
                quality_desc = "1080p (FHD)"
            elif main_format_id in ("264", "271", "308"):
                quality_desc = "1440p (2K)"
            elif main_format_id in ("266", "313", "315"):
                quality_desc = "2160p (4K)"
            else:
                quality_desc = main_format_id

        # Получаем данные из БД videos, а не из кэша
        video_info = cache.get_video_by_internal_id(video_db_id)
        if not video_info:
            video_info = cache.get_video_by_youtube_id(cached.get("video_id", ""))

        if video_info:
            title = video_info.get('title', cached.get('title', 'Видео'))
            duration = video_info.get('duration', cached.get('duration', 0))
            uploader = video_info.get('uploader') or cached.get('uploader', 'Неизвестно')
        else:
            title = cached.get('title', 'Видео')
            duration = cached.get('duration', 0)
            uploader = cached.get('uploader', 'Неизвестно')

        duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

        # Получаем URL из БД
        source_url = video_info["source_url"] if video_info else f"https://www.youtube.com/watch?v={cached.get('video_id')}"

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
            cache.log_request(
                callback.from_user.id, video_db_id, format_code,
                cached.get("file_size", 0), from_cache=True
            )
            await callback.message.delete()
            await callback.answer("✅ Отправлено")
        except Exception as e:
            logger.error(f"Ошибка отправки из кэша: {e}")
            await callback.answer("❌ Ошибка при отправке из кэша", show_alert=True)
        return

    # Определяем описание качества
    main_format_id = format_code.split('+')[0]

    if format_code == "bestaudio":
        quality_desc = "Только аудио"
    elif main_format_id in ("160", "278"):
        quality_desc = "144p"
    elif main_format_id in ("133", "242"):
        quality_desc = "240p"
    elif main_format_id in ("134", "243"):
        quality_desc = "360p"
    elif main_format_id in ("135", "244"):
        quality_desc = "480p"
    elif main_format_id in ("136", "247"):
        quality_desc = "720p (HD)"
    elif main_format_id in ("137", "248"):
        quality_desc = "1080p (FHD)"
    elif main_format_id in ("264", "271", "308"):
        quality_desc = "1440p (2K)"
    elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
        quality_desc = "2160p (4K)"
    else:
        info = get_video_info(url)
        if info and 'formats' in info:
            for fmt in info['formats']:
                if fmt.get('format_id') == main_format_id:
                    height = fmt.get('height', 0)
                    if height >= 4320:
                        quality_desc = "4320p (8K)"
                    elif height >= 2160:
                        quality_desc = "2160p (4K)"
                    elif height >= 1440:
                        quality_desc = "1440p (2K)"
                    elif height >= 1080:
                        quality_desc = "1080p (FHD)"
                    elif height >= 720:
                        quality_desc = "720p (HD)"
                    elif height >= 480:
                        quality_desc = "480p"
                    elif height >= 360:
                        quality_desc = "360p"
                    elif height >= 240:
                        quality_desc = "240p"
                    else:
                        quality_desc = "144p"
                    break
        if quality_desc == main_format_id:
            quality_desc = f"{main_format_id} (неизвестно)"

    size_match = re.search(r'\((\d+) MB\)', callback.message.text)
    size_info = f" (~{size_match.group(1)} MB)" if size_match else ""

    original_text = callback.message.text
    if "Выберите качество:" in original_text:
        original_text = original_text.split("Выберите качество:")[0].strip()

    # Получаем длительность и uploader из БД videos
    video_info = cache.get_video_by_internal_id(video_db_id)
    if not video_info:
        video_info = cache.get_video_by_youtube_id(str(video_db_id))

    duration = video_info.get('duration', 0) if video_info else 0
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
    uploader = video_info.get('uploader') or 'Неизвестно'

    await callback.message.edit_text(
        f"{original_text}\n\n⏳ Скачиваю в качестве {quality_desc}{size_info}...",
        parse_mode="Markdown"
    )

    await callback.answer(f"Начинаю загрузку ({quality_desc})...")

    # Скачиваем видео
    filepath, downloaded_title = await download_video(url, format_code)
    title = downloaded_title or title

    if not filepath:
        await callback.message.edit_text(
            f"{original_text}\n\n❌ Ошибка при загрузке видео."
        )
        return

    # Обновляем данные в БД videos (title, uploader могли измениться)
    cache.update_video_metadata(video_db_id, title=title, uploader=uploader)
    logger.info(f"Обновлены метаданные видео {video_db_id}: title={title}, uploader={uploader}")

    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        max_mb = MAX_FILE_SIZE / 1024 / 1024
        actual_mb = file_size / 1024 / 1024
        await callback.message.edit_text(
            f"{original_text}\n\n"
            f"❌ Файл слишком большой ({actual_mb:.1f} MB).\n\n"
            f"Максимальный размер: {max_mb:.0f} MB.\n"
            f"Попробуйте выбрать качество ниже."
        )
        return

    # Отправляем видео
    try:
        if BOT_API_SERVER_URL:
            import aiohttp
            import aiofiles

            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            api_url = f"{BOT_API_SERVER_URL}/sendVideo"

            data = aiohttp.FormData()
            data.add_field('chat_id', str(callback.message.chat.id))
            data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')

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

                    file_id = result.get('result', {}).get('video', {}).get('file_id')
                    if file_id:
                        cache.set(
                            video_db_id, format_code, file_id, file_size, quality_desc,
                            title, duration, uploader
                        )
                        logger.info(f"Сохранено в кэш: {video_db_id} / {format_code} → {file_id[:20]}...")

            await callback.message.delete()
        else:
            video = FSInputFile(filepath)

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
            cache.set(
                video_db_id, format_code, msg.video.file_id, file_size, quality_desc,
                title, duration, uploader
            )
            cache.log_request(callback.from_user.id, video_db_id, format_code, file_size, from_cache=False)
            logger.info(f"Сохранено в кэш: {video_db_id} / {format_code}")
            await callback.message.delete()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Ошибка при отправке: {error_msg}")

        if ("Server disconnected" in error_msg or
            "disconnected" in error_msg.lower() or
            "Connection reset by peer" in error_msg):
            error_text = (
                "❌ **Ошибка при отправке: соединение разорвано**\n\n"
                "Это произошло из-за большого размера файла или таймаута.\n\n"
                "Попробуйте:\n"
                "• Выбрать качество ниже\n"
                "• Проверить статус Bot API Server\n"
                "• Увеличить `MAX_FILE_SIZE` в `.env`"
            )
        elif "timeout" in error_msg.lower():
            error_text = (
                "❌ **Превышено время ожидания**\n\n"
                "Загрузка заняла слишком много времени.\n\n"
                "Попробуйте выбрать качество ниже."
            )
        else:
            error_text = f"{original_text}\n\n❌ Ошибка при отправке: {error_msg}"

        await callback.message.answer(error_text, parse_mode="Markdown" if "**" in error_text else None)
    finally:
        await cleanup_file(filepath)


@dp.callback_query(F.data.startswith("cancel_"))
async def handle_cancel(callback: types.CallbackQuery):
    """Обработчик отмены загрузки."""
    await callback.message.delete()
    await callback.answer("Загрузка отменена")


async def process_download_task(task: DownloadTask):
    """Обработчик задачи из очереди загрузок."""
    cache.update_queue_task_status(
        task.user_id, task.video_id, task.format_code, "started"
    )

    url = task.video_url

    if not url:
        logger.error(f"Не найден URL для видео {task.video_id}")
        if task.callback_query:
            await task.callback_query.answer("⚠️ Ошибка загрузки", show_alert=True)
        return

    filepath, title = await download_video(url, task.format_code)

    if not filepath:
        logger.error(f"Не удалось скачать видео {task.video_id}")
        if task.callback_query:
            await task.callback_query.answer("⚠️ Ошибка загрузки", show_alert=True)
        return

    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        logger.warning(f"Файл слишком большой: {file_size} байт")
        await cleanup_file(filepath)
        if task.callback_query:
            await task.callback_query.answer("⚠️ Файл слишком большой", show_alert=True)
        return

    task.file_size = file_size
    task.file_path = str(filepath)

    # Получаем данные из БД videos (task.video_id — строка, конвертируем в int)
    try:
        video_id_int = int(task.video_id)
        video_info = cache.get_video_by_internal_id(video_id_int)
    except (ValueError, TypeError):
        video_info = None
    
    if not video_info:
        video_info = cache.get_video_by_youtube_id(str(task.video_id))

    # Если видео нет в БД, пробуем получить из кэша
    if not video_info:
        metadata = cache.get(task.video_id, task.format_code)
        if metadata:
            title = metadata.get('title', title or 'Видео')
            duration = metadata.get('duration', 0)
            uploader = metadata.get('uploader', 'Неизвестно')
        else:
            uploader = 'Неизвестно'
            duration = 0
    else:
        title = video_info.get('title', title or 'Видео')
        duration = video_info.get('duration', 0)
        uploader = video_info.get('uploader') or 'Неизвестно'

    duration_str = f"{int(duration) // 60}:{int(duration) % 60:02d}" if duration else "N/A"

    chat_id = task.user_id
    if task.callback_query:
        chat_id = task.callback_query.message.chat.id

    try:
        if BOT_API_SERVER_URL:
            import aiohttp
            import aiofiles

            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            api_url = f"{BOT_API_SERVER_URL}/sendVideo"
            data = aiohttp.FormData()
            data.add_field('chat_id', str(chat_id))
            data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')

            if task.callback_query is None:
                caption = (
                    f"🎬 **[{title}]({url})**\n\n"
                    f"👤 {uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )
            else:
                caption = (
                    f"🎬 **{title}**\n\n"
                    f"👤 {uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )

            data.add_field('caption', caption)
            data.add_field('parse_mode', 'Markdown')

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, data=data) as response:
                    result = await response.json()
                    if not result.get('ok'):
                        raise Exception(f"Bot API error: {result.get('description', 'Unknown error')}")

                    file_id = result.get('result', {}).get('video', {}).get('file_id')
                    if file_id:
                        cache.set(
                            task.video_id, task.format_code, file_id, file_size, task.quality_label,
                            title, duration, uploader
                        )
                        logger.info(f"Сохранено в кэш: {task.video_id} / {task.format_code} → {file_id[:20]}...")
                    else:
                        logger.error(f"Не получен file_id из Bot API Server: {result}")

            if task.callback_query:
                await task.callback_query.message.delete()
        else:
            video = FSInputFile(filepath)

            if task.callback_query is None:
                caption = (
                    f"🎬 **[{title}]({url})**\n\n"
                    f"👤 {uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )
            else:
                caption = (
                    f"🎬 **{title}**\n\n"
                    f"👤 {uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )

            msg = await bot.send_video(chat_id, video, caption=caption, parse_mode="Markdown")

            cache.set(
                task.video_id, task.format_code, msg.video.file_id, file_size, task.quality_label,
                title, duration, uploader
            )
            logger.info(f"Сохранено в кэш: {task.video_id} / {task.format_code} → {msg.video.file_id[:20]}...")

            if task.callback_query:
                await task.callback_query.message.delete()

    except Exception as e:
        logger.error(f"Ошибка при отправке {task.video_id}: {e}")
    finally:
        await cleanup_file(filepath)

    cache.log_request(task.user_id, task.video_id, task.format_code, file_size, from_cache=False)
    cache.update_queue_task_status(
        task.user_id, task.video_id, task.format_code, "completed"
    )


@dp.callback_query(F.data.startswith("download_"))
async def handle_download_queued(callback: types.CallbackQuery):
    """Обработчик выбора качества с добавлением в очередь."""
    callback_data = callback.data

    if not callback_data.startswith("download_"):
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    remainder = callback_data[9:]
    last_underscore = remainder.rfind("_")
    if last_underscore == -1:
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    video_db_id = int(remainder[:last_underscore])
    format_code = remainder[last_underscore + 1:]

    if not video_db_id or not format_code:
        await callback.answer("❌ Ошибка формата", show_alert=True)
        return

    if cache.is_banned(callback.from_user.id):
        await callback.answer("🚫 Вы заблокированы администратором!", show_alert=True)
        return

    size_match = re.search(r'\((\d+) MB\)', callback.message.text)
    size_info = f" (~{size_match.group(1)} MB)" if size_match else ""

    main_format_id = format_code.split('+')[0]
    if format_code == "bestaudio":
        quality_desc = "Только аудио"
    elif main_format_id in ("160", "278"):
        quality_desc = "144p"
    elif main_format_id in ("133", "242"):
        quality_desc = "240p"
    elif main_format_id in ("134", "243"):
        quality_desc = "360p"
    elif main_format_id in ("135", "244"):
        quality_desc = "480p"
    elif main_format_id in ("136", "247"):
        quality_desc = "720p (HD)"
    elif main_format_id in ("137", "248"):
        quality_desc = "1080p (FHD)"
    elif main_format_id in ("264", "271", "308"):
        quality_desc = "1440p (2K)"
    elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
        quality_desc = "2160p (4K)"
    else:
        quality_desc = f"{main_format_id}"

    # Получаем URL из БД
    video_info = cache.get_video_by_youtube_id(str(video_db_id))
    url = video_info["source_url"] if video_info else None

    if not url:
        await callback.answer("❌ Ссылка устарела, отправьте заново", show_alert=True)
        return

    # Проверяем кэш
    cached = cache.get(video_db_id, format_code)
    if cached:
        logger.info(f"Отправка из кэша: {video_db_id} / {format_code}")

        duration = cached.get("duration", 0)
        duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"
        title = cached.get("title", "Видео")
        uploader = cached.get("uploader", "Неизвестно")
        source_url = url

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
            cache.log_request(
                callback.from_user.id, video_db_id, format_code,
                cached.get("file_size", 0), from_cache=True
            )
            await callback.message.delete()
            await callback.answer("✅ Отправлено из кэша")
        except Exception as e:
            logger.error(f"Ошибка отправки из кэша: {e}")
            await callback.answer("❌ Ошибка при отправке из кэша", show_alert=True)
        return

    # Создаём задачу для очереди
    task = DownloadTask(
        task_id=f"{video_db_id}_{format_code}_{int(datetime.now().timestamp())}",
        user_id=callback.from_user.id,
        username=callback.from_user.username or str(callback.from_user.id),
        video_url=url,
        video_id=str(video_db_id),
        format_code=format_code,
        quality_label=quality_desc,
        callback_query=callback
    )

    await queue_mgr.add_task(task)

    cache.log_queue_task(
        callback.from_user.id, str(video_db_id), format_code, "pending"
    )

    # Получаем данные из БД videos
    video_info = cache.get_video_by_internal_id(video_db_id)
    if not video_info:
        video_info = cache.get_video_by_youtube_id(str(video_db_id))

    if video_info:
        title = video_info.get('title', 'Видео')
        duration = video_info.get('duration', 0)
        uploader = video_info.get('uploader') or 'Неизвестно'
    else:
        metadata = cache.get(video_db_id, format_code)
        title = metadata.get('title', 'Видео') if metadata else 'Видео'
        duration = metadata.get('duration', 0) if metadata else 0
        uploader = metadata.get('uploader', 'Неизвестно') if metadata else 'Неизвестно'

    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

    original_text = callback.message.text
    if "Выберите качество:" in original_text:
        original_text = original_text.split("Выберите качество:")[0].strip()

    await callback.message.edit_text(
        f"{original_text}\n\n"
        f"⏳ **Скачиваю {quality_desc}...**\n\n"
        f"🎬 {title}\n"
        f"👤 {uploader}\n"
        f"⏱ {duration_str}",
        parse_mode="Markdown"
    )

    await callback.answer(f"⏳ Загружаю {quality_desc}...")
    logger.info(
        f"Задача добавлена в очередь: {task.task_id} "
        f"(user={callback.from_user.id}, video={video_db_id}, format={format_code})"
    )


async def main():
    """Основная функция запуска бота."""
    DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("Запуск бота...")

    await queue_mgr.start(process_download_task)
    logger.info("Менеджер очереди запущен")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
