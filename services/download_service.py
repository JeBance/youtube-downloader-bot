"""
Сервис для отправки видео в Telegram.
"""
import logging
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp
from aiogram import Bot
from aiogram.types import FSInputFile

from config import BOT_API_SERVER_URL, MAX_FILE_SIZE

logger = logging.getLogger(__name__)


async def send_video_from_file_id(
    bot: Bot,
    chat_id: int,
    file_id: str,
    title: str,
    uploader: str,
    duration: int,
    quality_desc: str,
    source_url: str
):
    """Отправить видео по file_id из кэша."""
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

    # Экранируем special символы
    escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
    escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

    caption = (
        f"🎬 **[{escaped_title}]({source_url})**\\n\\n"
        f"👤 {escaped_uploader}\\n"
        f"⏱ Длительность: {duration_str}\\n"
        f"📹 Качество: {quality_desc}"
    )

    await bot.send_video(
        chat_id,
        video=file_id,
        caption=caption,
        parse_mode="Markdown"
    )


async def download_and_send_video(
    bot: Bot,
    chat_id: int,
    url: str,
    format_code: str,
    quality_desc: str,
    title: str,
    uploader: str,
    duration: int,
    db,
    video_db_id: int
):
    """Скачать и отправить видео."""
    from services.video_service import download_video, cleanup_file

    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

    # Скачиваем
    filepath, downloaded_title = await download_video(url, format_code)
    title = downloaded_title or title

    if not filepath:
        await bot.send_message(chat_id, f"❌ {title[:40]} — ошибка загрузки")
        return

    # Обновляем метаданные в БД
    db.update_video_metadata(video_db_id, title=title, uploader=uploader)
    logger.info(f"Обновлены метаданные видео {video_db_id}: title={title}, uploader={uploader}")

    file_size = filepath.stat().st_size
    if file_size > MAX_FILE_SIZE:
        await cleanup_file(filepath)
        max_mb = MAX_FILE_SIZE / 1024 / 1024
        actual_mb = file_size / 1024 / 1024
        await bot.send_message(
            chat_id,
            f"❌ Файл слишком большой ({actual_mb:.1f} MB). Максимальный размер: {max_mb:.0f} MB."
        )
        return

    # Отправляем
    try:
        if BOT_API_SERVER_URL:
            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            api_url = f"{BOT_API_SERVER_URL}/sendVideo"
            data = aiohttp.FormData()
            data.add_field('chat_id', str(chat_id))
            data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')

            escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
            escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

            caption = (
                f"🎬 **[{escaped_title}]({url})**\\n\\n"
                f"👤 {escaped_uploader}\\n"
                f"⏱ Длительность: {duration_str}\\n"
                f"📹 Качество: {quality_desc}"
            )
            data.add_field('caption', caption)
            data.add_field('parse_mode', 'Markdown')

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, data=data) as response:
                    result = await response.json()
                    if result.get('ok'):
                        file_id = result['result']['video']['file_id']
                        # Сохраняем в кэш
                        db.set(
                            video_db_id, format_code, file_id, file_size, quality_desc,
                            title, duration, uploader
                        )
                    else:
                        raise Exception(f"Bot API error: {result.get('description')}")

            await cleanup_file(filepath)
        else:
            video = FSInputFile(filepath)

            escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
            escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

            caption = (
                f"🎬 **[{escaped_title}]({url})**\\n\\n"
                f"👤 {escaped_uploader}\\n"
                f"⏱ Длительность: {duration_str}\\n"
                f"📹 Качество: {quality_desc}"
            )
            msg = await bot.send_video(chat_id, video=video, caption=caption, parse_mode="Markdown")

            db.set(
                video_db_id, format_code, msg.video.file_id, file_size, quality_desc,
                title, duration, uploader
            )
            await cleanup_file(filepath)

    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        await cleanup_file(filepath)
        await bot.send_message(chat_id, f"❌ {title[:40]} — ошибка отправки: {e}")
