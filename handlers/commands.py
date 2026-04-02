"""
Handlers для команд бота (/start, /help, /ping, /lang, /admin, /status, /clear, /queue, /qstat).
"""
import logging
from aiogram import Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import ADMIN_ID, QUEUE_YOUTUBE_RPS, QUEUE_TELEGRAM_UPLOADS_PER_MIN, QUEUE_MAX_CONCURRENT, QUEUE_MAX_PER_USER
from database import VideoDatabase
from queue_manager import FairQueueManager

logger = logging.getLogger(__name__)

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, db: VideoDatabase):
    """Обработчик команды /start."""
    db.add_user(
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


@router.message(Command("help"))
async def cmd_help(message: Message, db: VideoDatabase):
    """Обработчик команды /help."""
    is_admin = message.from_user.id == ADMIN_ID

    if is_admin:
        await message.answer(
            "👑 **Команды администратора:**\n\n"
            "/clear — Очистка кэша\n"
            "/admin — Админ-панель\n"
            "/qstat — Статистика очереди\n\n"
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


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    """Обработчик команды /ping."""
    await message.answer("🏓 Понг! Бот на связи!")
    logger.info(f"Команда /ping от {message.from_user.id}")


@router.message(Command("lang"))
async def cmd_lang(message: Message, db: VideoDatabase):
    """Обработчик команды /lang."""
    current_lang = db.get_user_language(message.from_user.id)
    current_lang_name = "🇷 Русский" if current_lang == "ru" else "🇬🇧 English"

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


@router.callback_query(F.data.startswith("lang_"))
async def handle_lang_change(callback: CallbackQuery, db: VideoDatabase):
    """Обработчик выбора языка."""
    language = callback.data.split("_")[1]
    db.set_user_language(callback.from_user.id, language)
    lang_name = "🇷🇺 Русский" if language == "ru" else "🇬🇧 English"

    await callback.message.edit_text(
        f"✅ **Язык изменён**\n\n"
        f"Теперь поиск будет выполняться на языке: {lang_name}\n\n"
        f"Теперь отправьте любой текстовый запрос для поиска видео!",
        parse_mode="Markdown"
    )
    await callback.answer(f"Язык: {lang_name}")
    logger.info(f"Язык изменён пользователем {callback.from_user.id}: {language}")


@router.message(Command("admin"))
async def cmd_admin(message: Message):
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


@router.message(Command("status"))
async def cmd_cache_status(message: Message, db: VideoDatabase):
    """Обработчик команды /status."""
    try:
        new_stats = db.get_new_stats()

        if new_stats:
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
        logger.info(f"Команда /status от {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка в /status: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("clear"))
async def cmd_cache_clear(message: Message, db: VideoDatabase):
    """Обработчик команды /clear."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    count = db.clear()
    await message.answer(f"🗑 Кэш очищен! Удалено записей: {count}")
    logger.info(f"Кэш очищен пользователем {message.from_user.id}: {count} записей")


@router.message(Command("queue"))
async def cmd_queue(message: Message, queue_mgr: FairQueueManager):
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


@router.message(Command("qstat"))
async def cmd_queue_stats(message: Message, queue_mgr: FairQueueManager, db: VideoDatabase):
    """Обработчик команды /qstat."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён!")
        return

    stats = queue_mgr.get_stats()
    db_stats = db.get_queue_stats()

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
