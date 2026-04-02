"""
Handlers для обработки очереди загрузок.
"""
import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery

from database import VideoDatabase
from queue_manager import FairQueueManager, DownloadTask, TaskStatus

logger = logging.getLogger(__name__)

router = Router()


@router.callback_query(F.data.startswith("cancel_"))
async def handle_cancel(callback: CallbackQuery, queue_mgr: FairQueueManager):
    """Обработчик отмены загрузки."""
    await callback.message.delete()
    await callback.answer("Загрузка отменена")


async def process_download_task(task: DownloadTask, db: VideoDatabase):
    """Обработчик задачи из очереди загрузок."""
    from services.video_service import download_video, cleanup_file
    from config import BOT_API_SERVER_URL, MAX_FILE_SIZE
    import aiofiles
    import aiohttp
    
    # Добавляем db в task для доступа из всех мест
    task.db = db

    task.db.update_queue_task_status(
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
        video_info = task.db.get_video_by_internal_id(video_id_int)
        logger.info(f"Получено video_info по internal ID {video_id_int}: {video_info}")
    except (ValueError, TypeError):
        video_info = None
        logger.warning(f"Не удалось конвертировать task.video_id={task.video_id} в int")

    if not video_info:
        video_info = task.db.get_video_by_youtube_id(str(task.video_id))
        logger.info(f"Получено video_info по YouTube ID {task.video_id}: {video_info}")

    # Если видео нет в БД, пробуем получить из кэша
    if not video_info:
        metadata = task.db.get(task.video_id, task.format_code)
        if metadata:
            title = metadata.get('title', title or 'Видео')
            duration = metadata.get('duration', 0)
            uploader = metadata.get('uploader', 'Неизвестно')
        else:
            uploader = 'Неизвестно'
            duration = 0
        logger.warning(f"Видео не найдено в БД, используем metadata из кэша")
    else:
        title = video_info.get('title', title or 'Видео')
        duration = video_info.get('duration', 0)
        uploader = video_info.get('uploader') or 'Неизвестно'
        logger.info(f"Видео найдено в БД: title={title}, uploader={uploader}")

    # Обновляем данные в БД videos (title, uploader могли измениться)
    try:
        video_id_int = int(task.video_id)
        task.db.update_video_metadata(video_id_int, title=title, uploader=uploader)
        logger.info(f"Обновлены метаданные видео {video_id_int}: title={title}, uploader={uploader}")
    except (ValueError, TypeError) as e:
        logger.warning(f"Не удалось обновить метаданные: {e}")

    duration_str = f"{int(duration) // 60}:{int(duration) % 60:02d}" if duration else "N/A"

    chat_id = task.user_id
    if task.callback_query:
        chat_id = task.callback_query.message.chat.id

    try:
        if BOT_API_SERVER_URL:
            async with aiofiles.open(filepath, 'rb') as f:
                video_data = await f.read()

            # Для аудио используем sendAudio, для видео — sendVideo
            if task.format_code == "bestaudio":
                api_url = f"{BOT_API_SERVER_URL}/sendAudio"
                data = aiohttp.FormData()
                data.add_field('chat_id', str(chat_id))
                data.add_field('audio', video_data, filename=filepath.name, content_type='audio/mpeg')

                escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

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
                        if result.get('ok'):
                            file_id = result.get('result', {}).get('audio', {}).get('file_id')
                            if file_id:
                                task.db.set(task.video_id, task.format_code, file_id, file_size, task.quality_label, title, duration, uploader)
                                logger.info(f"Сохранено в кэш: {task.video_id} / {task.format_code}")
                            else:
                                logger.error(f"Не получен file_id из Bot API Server: {result}")
                        else:
                            raise Exception(f"Bot API error: {result.get('description', 'Unknown error')}")
            else:
                api_url = f"{BOT_API_SERVER_URL}/sendVideo"
                data = aiohttp.FormData()
                data.add_field('chat_id', str(chat_id))
                data.add_field('video', video_data, filename=filepath.name, content_type='video/mp4')

                escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

                caption = (
                    f"🎬 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )
                data.add_field('caption', caption)
                data.add_field('parse_mode', 'Markdown')

                async with aiohttp.ClientSession() as session:
                    async with session.post(api_url, data=data) as response:
                        result = await response.json()
                        if result.get('ok'):
                            file_id = result.get('result', {}).get('video', {}).get('file_id')
                            if file_id:
                                task.db.set(task.video_id, task.format_code, file_id, file_size, task.quality_label, title, duration, uploader)
                                logger.info(f"Сохранено в кэш: {task.video_id} / {task.format_code}")
                            else:
                                logger.error(f"Не получен file_id из Bot API Server: {result}")
                        else:
                            raise Exception(f"Bot API error: {result.get('description', 'Unknown error')}")

            if task.callback_query:
                await task.callback_query.message.delete()
        else:
            from aiogram.types import FSInputFile

            escaped_title = title.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
            escaped_uploader = uploader.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`') if uploader else 'Неизвестно'

            # Для аудио используем send_audio, для видео — send_video
            if task.format_code == "bestaudio":
                audio = FSInputFile(filepath)
                caption = (
                    f"🎵 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}"
                )
                msg = await task.db.bot.send_audio(chat_id, audio, caption=caption, parse_mode="Markdown")
                task.db.set(task.video_id, task.format_code, msg.audio.file_id, file_size, task.quality_label, title, duration, uploader)
            else:
                video = FSInputFile(filepath)
                caption = (
                    f"🎬 **[{escaped_title}]({url})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {task.quality_label}"
                )
                msg = await task.db.bot.send_video(chat_id, video, caption=caption, parse_mode="Markdown")
                task.db.set(task.video_id, task.format_code, msg.video.file_id, file_size, task.quality_label, title, duration, uploader)

            logger.info(f"Сохранено в кэш: {task.video_id} / {task.format_code}")

            if task.callback_query:
                await task.callback_query.message.delete()

    except Exception as e:
        logger.error(f"Ошибка при отправке {task.video_id}: {e}")
    finally:
        await cleanup_file(filepath)

    task.db.log_request(task.user_id, task.video_id, task.format_code, file_size, from_cache=False)
    task.db.update_queue_task_status(
        task.user_id, task.video_id, task.format_code, "completed"
    )
