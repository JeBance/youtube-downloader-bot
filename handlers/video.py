"""
Handlers для работы с видео (ссылки, выбор качества, загрузка).
"""
import asyncio
import logging
import re
from datetime import datetime
from typing import Dict

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import MAX_FILE_SIZE
from database import VideoDatabase
from queue_manager import FairQueueManager, DownloadTask
from services.video_service import (
    is_youtube_url,
    get_video_info,
    get_available_formats,
)
from .search import search_results_cache, send_search_report, process_search_results

logger = logging.getLogger(__name__)

router = Router()


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
        cached_formats: список dict из БД с telegram_file_id и quality_label
    """
    builder = InlineKeyboardBuilder()

    # cached_formats это список dict из БД, берём quality_label оттуда
    cached_dict = {fmt.get('format_code'): fmt.get('quality_label') for fmt in (cached_formats or [])}

    for fmt_code, description, height, est_size in formats:
        if est_size > MAX_FILE_SIZE:
            continue

        # Берём quality_label из БД (если есть) или определяем по format_id
        quality_label = cached_dict.get(fmt_code)
        if not quality_label:
            main_format_id = fmt_code.split('+')[0]
            if fmt_code == "bestaudio":
                quality_label = "Только аудио"
            elif main_format_id in ("160", "278"):
                quality_label = "144p"
            elif main_format_id in ("133", "242", "299"):
                quality_label = "240p"
            elif main_format_id in ("134", "243", "300"):
                quality_label = "360p"
            elif main_format_id in ("135", "244", "298", "301"):
                quality_label = "480p"
            elif main_format_id in ("136", "247", "302"):
                quality_label = "720p (HD)"
            elif main_format_id in ("137", "248", "303"):
                quality_label = "1080p (FHD)"
            elif main_format_id in ("264", "271", "308"):
                quality_label = "1440p (2K)"
            elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
                quality_label = "2160p (4K)"
            else:
                quality_label = f"{main_format_id}"

        # Извлекаем размер из description
        size_match = re.search(r'\((\d+) MB\)', description)
        size_str = f" ({size_match.group(1)} MB)" if size_match else ""

        button_text = f"{quality_label}{size_str}"

        if fmt_code in cached_dict:
            builder.button(
                text=f"✅ {button_text}",
                callback_data=f"download_{video_db_id}_{fmt_code}"
            )
        else:
            builder.button(
                text=f"📹 {button_text}",
                callback_data=f"download_{video_db_id}_{fmt_code}"
            )

    builder.button(text="❌ Отмена", callback_data=f"cancel_{video_db_id}")
    builder.adjust(2, 2)
    return builder.as_markup()


@router.message(F.text)
async def handle_text_message(message: Message, db: VideoDatabase, queue_mgr: FairQueueManager):
    """Обработчик текстовых сообщений (поиск видео или ссылка)."""
    text = message.text.strip()

    if text.startswith('/'):
        return

    if db.is_banned(message.from_user.id):
        await message.answer("🚫 Вы заблокированы администратором!")
        logger.warning(f"Забаненный пользователь попытался выполнить поиск: {message.from_user.id}")
        return

    if is_youtube_url(text):
        await handle_url(message, db)
        return

    # Это поисковый запрос
    query = text
    db.add_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    logger.info(f"Поисковый запрос от {message.from_user.id}: '{query}'")

    status_msg = await message.answer(
        f"🔍 Ищу видео по запросу: **{query}**\n\n⏳ Пожалуйста, подождите...",
        parse_mode="Markdown"
    )

    try:
        from services.video_service import search_youtube_videos, search_youtube_shorts

        user_language = db.get_user_language(message.from_user.id)

        videos, shorts = await asyncio.gather(
            search_youtube_videos(query, limit=10, language=user_language),
            search_youtube_shorts(query, limit=10, language=user_language)
        )

        if not videos and not shorts:
            await status_msg.edit_text(
                f"❌ Ничего не найдено по запросу: **{query}**\n\nПопробуйте другой запрос.",
                parse_mode="Markdown"
            )
            return

        # Сохраняем результаты поиска
        search_results_cache[message.from_user.id] = {
            'query': query,
            'videos': videos,
            'shorts': shorts,
            'timestamp': datetime.now()
        }

        # Формируем отчёт
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

        # Отправляем отчёт через server-bot
        await send_search_report(
            user_id=message.from_user.id,
            username=message.from_user.username,
            query=query,
            videos_count=len(videos),
            shorts_count=len(shorts),
            videos=videos,
            shorts=shorts
        )

        # Добавляем видео в очередь
        await process_search_results(
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            videos=videos,
            shorts=shorts,
            query=query,
            db=db,
            bot=message.bot,
            queue_mgr=queue_mgr
        )

    except Exception as e:
        logger.error(f"Ошибка при поиске: {e}", exc_info=True)
        await status_msg.edit_text(
            f"❌ Произошла ошибка при поиске: {str(e)}\n\nПопробуйте позже.",
            parse_mode="Markdown"
        )


async def handle_url(message: Message, db: VideoDatabase):
    """Обработчик ссылок на YouTube."""
    url = message.text.strip()

    if url.startswith('/'):
        return

    if not is_youtube_url(url):
        return

    if db.is_banned(message.from_user.id):
        await message.answer("🚫 Вы заблокированы администратором!")
        logger.warning(f"Забаненный пользователь попытался загрузить видео: {message.from_user.id}")
        return

    db.add_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
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
    video_db_id = db.create_video(
        source_url=url,
        youtube_video_id=youtube_video_id,
        title=title,
        uploader=uploader,
        duration=duration
    )

    # 2. Сохраняем форматы в БД с правильным quality_label
    for fmt_code, description, height, est_size in available:
        # Определяем quality_label по format_id (а не из description!)
        main_format_id = fmt_code.split('+')[0]
        if fmt_code == "bestaudio":
            quality_label = "Только аудио"
        elif main_format_id in ("160", "278"):
            quality_label = "144p"
        elif main_format_id in ("133", "242", "299"):
            quality_label = "240p"
        elif main_format_id in ("134", "243", "300"):
            quality_label = "360p"
        elif main_format_id in ("135", "244", "298", "301"):
            quality_label = "480p"
        elif main_format_id in ("136", "247", "302"):
            quality_label = "720p (HD)"
        elif main_format_id in ("137", "248", "303"):
            quality_label = "1080p (FHD)"
        elif main_format_id in ("264", "271", "308"):
            quality_label = "1440p (2K)"
        elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
            quality_label = "2160p (4K)"
        else:
            quality_label = f"{main_format_id}"
        
        db.create_or_get_format(
            video_id=video_db_id,
            format_code=fmt_code,
            quality_label=quality_label,
            requested_by_user_id=message.from_user.id
        )

    # 3. Получаем все форматы из БД (теперь они там есть)
    cached_formats = db.get_all_formats_for_video(video_db_id)
    logger.info(f"Найдено {len(cached_formats)} закэшированных форматов для видео {video_db_id}")

    # 4. Создаём клавиатуру с video_db_id
    keyboard = build_quality_keyboard(video_db_id, available, cached_formats)

    # Экранируем special символы
    escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

    caption = (
        f"🎬 **[{escaped_title}]({url})**\n\n"
        f"👤 {escaped_uploader}\n"
        f"⏱ Длительность: {duration_str}\n\n"
        f"**Выберите качество:**"
    )

    await status_msg.edit_text(
        caption,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    logger.info(f"Показаны варианты качества для видео {video_db_id}")


@router.callback_query(F.data.startswith("download_"))
async def handle_download_queued(callback: CallbackQuery, db: VideoDatabase, queue_mgr: FairQueueManager):
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

    if db.is_banned(callback.from_user.id):
        await callback.answer("🚫 Вы заблокированы администратором!", show_alert=True)
        return

    # Определяем quality_desc для кнопки
    main_format_id = format_code.split('+')[0]
    if format_code == "bestaudio":
        quality_desc = "Только аудио"
    elif main_format_id in ("160", "278"):
        quality_desc = "144p"
    elif main_format_id in ("133", "242", "299"):
        quality_desc = "240p"
    elif main_format_id in ("134", "243", "300"):
        quality_desc = "360p"
    elif main_format_id in ("135", "244", "298", "301"):
        quality_desc = "480p"
    elif main_format_id in ("136", "247", "302"):
        quality_desc = "720p (HD)"
    elif main_format_id in ("137", "248", "303"):
        quality_desc = "1080p (FHD)"
    elif main_format_id in ("264", "271", "308"):
        quality_desc = "1440p (2K)"
    elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
        quality_desc = "2160p (4K)"
    else:
        quality_desc = f"{main_format_id}"

    # Проверяем кэш по quality_label, а не по format_code
    # (YouTube может выдавать разные format_id для одного качества)
    all_formats = db.get_all_formats_for_video(video_db_id)
    cached = None
    for fmt in all_formats:
        if fmt.get('quality_label') == quality_desc and fmt.get('telegram_file_id'):
            cached = fmt
            break
    
    if cached:
        # Уже есть в кэше — отправляем по file_id
        logger.info(f"Отправка из кэша: {video_db_id} / {quality_desc}")

        # Получаем данные из БД videos
        video_info = db.get_video_by_internal_id(video_db_id)
        if not video_info:
            video_info = db.get_video_by_youtube_id(str(video_db_id))

        if video_info:
            title = video_info.get('title', cached.get('title', 'Видео'))
            duration = video_info.get('duration', cached.get('duration', 0))
            uploader = video_info.get('uploader') or cached.get('uploader', 'Неизвестно')
            url = video_info.get('source_url')
        else:
            title = cached.get('title', 'Видео')
            duration = cached.get('duration', 0)
            uploader = cached.get('uploader', 'Неизвестно')
            url = f"https://www.youtube.com/watch?v={video_db_id}"

        duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

        # Экранируем special символы
        escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
        escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

        caption = (
            f"🎬 **[{escaped_title}]({url})**\n\n"
            f"👤 {escaped_uploader}\n"
            f"⏱ Длительность: {duration_str}\n"
            f"📹 Качество: {quality_desc}"
        )

        try:
            await callback.message.answer_video(
                video=cached["telegram_file_id"],
                caption=caption,
                parse_mode="Markdown"
            )
            db.log_request(callback.from_user.id, video_db_id, format_code, cached.get("telegram_file_size", 0), from_cache=True)
            await callback.message.delete()
            await callback.answer("✅ Отправлено из кэша")
        except Exception as e:
            logger.error(f"Ошибка отправки из кэша: {e}")
            await callback.answer("❌ Ошибка при отправке из кэша", show_alert=True)
        return

    size_match = re.search(r'\((\d+) MB\)', callback.message.text)
    size_info = f" (~{size_match.group(1)} MB)" if size_match else ""

    # Получаем данные из БД videos
    video_info = db.get_video_by_internal_id(video_db_id)
    if not video_info:
        video_info = db.get_video_by_youtube_id(str(video_db_id))

    if video_info:
        title = video_info.get('title', 'Видео')
        duration = video_info.get('duration', 0)
        uploader = video_info.get('uploader') or 'Неизвестно'
        url = video_info.get('source_url')
    else:
        title = 'Видео'
        duration = 0
        uploader = 'Неизвестно'
        url = None

    # Если URL не получен, пробуем найти в кэше
    if not url:
        cached = db.get(video_db_id, format_code)
        if cached:
            logger.error(f"Не получен URL для видео {video_db_id}")
            await callback.answer("❌ Ошибка: URL видео не найден", show_alert=True)
            return

    # Определяем quality_desc
    main_format_id = format_code.split('+')[0]
    if format_code == "bestaudio":
        quality_desc = "Только аудио"
    elif main_format_id in ("160", "278"):
        quality_desc = "144p"
    elif main_format_id in ("133", "242", "299"):
        quality_desc = "240p"
    elif main_format_id in ("134", "243", "300"):
        quality_desc = "360p"
    elif main_format_id in ("135", "244", "298", "301"):
        quality_desc = "480p"
    elif main_format_id in ("136", "247", "302"):
        quality_desc = "720p (HD)"
    elif main_format_id in ("137", "248", "303"):
        quality_desc = "1080p (FHD)"
    elif main_format_id in ("264", "271", "308"):
        quality_desc = "1440p (2K)"
    elif main_format_id in ("266", "313", "315", "396", "397", "398", "399", "400", "401", "402"):
        quality_desc = "2160p (4K)"
    else:
        quality_desc = f"{main_format_id}"

    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

    # Экранируем special символы для Markdown
    escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

    await callback.message.edit_text(
        f"🎬 **{escaped_title}**\n\n"
        f"👤 {escaped_uploader}\n"
        f"⏱ Длительность: {duration_str}\n\n"
        f"⏳ **Скачиваю в качестве {quality_desc}{size_info}...**",
        parse_mode="Markdown"
    )

    await callback.answer(f"Начинаю загрузку ({quality_desc})...")

    # Скачиваем видео
    from services.video_service import download_video, cleanup_file
    filepath, downloaded_title = await download_video(url, format_code)
    title = downloaded_title or title

    if not filepath:
        await callback.message.edit_text(
            f"🎬 **{escaped_title}**\n\n"
            f"👤 {escaped_uploader}\n"
            f"⏱ Длительность: {duration_str}\n\n"
            f"❌ Ошибка при загрузке видео.",
            parse_mode="Markdown"
        )
        return

    # Обновляем данные в БД videos (title, uploader могли измениться)
    db.update_video_metadata(video_db_id, title=title, uploader=uploader)
    logger.info(f"Обновлены метаданные видео {video_db_id}: title={title}, uploader={uploader}")

    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        max_mb = MAX_FILE_SIZE / 1024 / 1024
        actual_mb = file_size / 1024 / 1024
        await callback.message.edit_text(
            f"🎬 **{escaped_title}**\n\n"
            f"👤 {escaped_uploader}\n"
            f"⏱ Длительность: {duration_str}\n\n"
            f"❌ Файл слишком большой ({actual_mb:.1f} MB).\n\n"
            f"Максимальный размер: {max_mb:.0f} MB.\n"
            f"Попробуйте выбрать качество ниже.",
            parse_mode="Markdown"
        )
        return

    # Отправляем видео
    try:
        from config import BOT_API_SERVER_URL
        import aiofiles
        import aiohttp
        from aiogram.types import FSInputFile

        if BOT_API_SERVER_URL:
            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            # Для аудио используем sendAudio, для видео — sendVideo
            if format_code == "bestaudio":
                api_url = f"{BOT_API_SERVER_URL}/sendAudio"
                data = aiohttp.FormData()
                data.add_field('chat_id', str(callback.message.chat.id))
                data.add_field('audio', video_data, filename=filepath.name, content_type='audio/mpeg')

                caption = (
                    f"🎵 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}"
                )
                data.add_field('caption', caption)
                data.add_field('parse_mode', 'Markdown')

                async with aiohttp.ClientSession() as session:
                    async with session.post(api_url, data=data) as response:
                        result = await response.json()
                        if result.get('ok') and result.get('result', {}).get('audio', {}).get('file_id'):
                            file_id = result['result']['audio']['file_id']
                            db.set(video_db_id, format_code, file_id, file_size, quality_desc, title, duration, uploader)
                            logger.info(f"Сохранено в кэш: {video_db_id} / {format_code}")
                        else:
                            error_msg = result.get('description', 'Unknown error')
                            raise Exception(f"Bot API error: {error_msg}")
            else:
                api_url = f"{BOT_API_SERVER_URL}/sendVideo"
                data = aiohttp.FormData()
                data.add_field('chat_id', str(callback.message.chat.id))
                data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')

                caption = (
                    f"🎬 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {quality_desc}"
                )
                data.add_field('caption', caption)
                data.add_field('parse_mode', 'Markdown')

                async with aiohttp.ClientSession() as session:
                    async with session.post(api_url, data=data) as response:
                        result = await response.json()
                        if result.get('ok') and result.get('result', {}).get('video', {}).get('file_id'):
                            file_id = result['result']['video']['file_id']
                            db.set(video_db_id, format_code, file_id, file_size, quality_desc, title, duration, uploader)
                            logger.info(f"Сохранено в кэш: {video_db_id} / {format_code}")
                        else:
                            error_msg = result.get('description', 'Unknown error')
                            raise Exception(f"Bot API error: {error_msg}")

            await cleanup_file(filepath)
        else:
            # Для аудио используем send_audio, для видео — send_video
            if format_code == "bestaudio":
                audio = FSInputFile(filepath)
                caption = (
                    f"🎵 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}"
                )
                msg = await callback.message.answer_audio(audio, caption=caption, parse_mode="Markdown")
                db.set(video_db_id, format_code, msg.audio.file_id, file_size, quality_desc, title, duration, uploader)
            else:
                video = FSInputFile(filepath)
                caption = (
                    f"🎬 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {quality_desc}"
                )
                msg = await callback.message.answer_video(video, caption=caption, parse_mode="Markdown")
                db.set(video_db_id, format_code, msg.video.file_id, file_size, quality_desc, title, duration, uploader)
            
            db.log_request(callback.from_user.id, video_db_id, format_code, file_size, from_cache=False)
            logger.info(f"Сохранено в кэш: {video_db_id} / {format_code}")
            await cleanup_file(filepath)

        await callback.message.delete()

    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        await callback.message.answer(f"❌ Ошибка при отправке: {e}")
        await cleanup_file(filepath)
