"""
Handlers для поиска видео.
"""
import logging
import asyncio
from datetime import datetime
from typing import List, Dict

from aiogram import Router, F
from aiogram.types import Message

from database import VideoDatabase

logger = logging.getLogger(__name__)

router = Router()

# Глобальный словарь для хранения результатов поиска
search_results_cache: Dict[int, Dict[str, any]] = {}


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
        escaped_report = report.replace('"', '\\\\"').replace('$', '\\\\$')
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
    query: str,
    db: VideoDatabase,
    bot,
    queue_mgr
):
    """Обрабатывает результаты поиска: добавляет до 10 видео + 10 shorts в очередь БД."""
    from services.video_service import get_video_info, get_available_formats
    from config import MAX_FILE_SIZE

    total_added = 0
    total_failed = 0

    all_items = []

    # Добавляем видео (до 10)
    for video in videos[:10]:
        all_items.append({
            'url': video['url'],
            'id': video['id'],
            'title': video['title'],
            'uploader': video.get('uploader', 'Неизвестно'),
            'duration': video.get('duration', 0),
            'type': 'video'
        })

    # Добавляем shorts (до 10)
    for short in shorts[:10]:
        all_items.append({
            'url': short['url'],
            'id': short['id'],
            'title': short['title'],
            'uploader': short.get('uploader', 'Неизвестно'),
            'duration': short.get('duration', 0),
            'type': 'shorts'
        })

    logger.info(f"Добавляю {len(all_items)} видео из поиска в очередь БД")

    await bot.send_message(
        chat_id,
        f"⏳ **Добавление в очередь...**\n\n"
        f"Найдено видео: {len(videos)}\n"
        f"Найдено shorts: {len(shorts)}\n"
        f"Будет добавлено: {len(all_items)}",
        parse_mode="Markdown"
    )

    for idx, item in enumerate(all_items):
        try:
            logger.info(f"Обработка [{idx+1}/{len(all_items)}]: {item['type']} - {item['title'][:50]}")

            # 1. Создаём или получаем видео из БД
            video_db_id = db.create_video(
                source_url=item['url'],
                youtube_video_id=item['id'],
                title=item['title'],
                uploader=item['uploader'],
                duration=item['duration']
            )

            # 2. Получаем информацию о видео для определения форматов
            info = get_video_info(item['url'])
            if not info:
                logger.warning(f"Не удалось получить информацию: {item['url']}")
                total_failed += 1
                continue

            # 3. Получаем доступные форматы
            formats = info.get('formats', [])
            available = get_available_formats(
                formats,
                max_size_mb=MAX_FILE_SIZE // 1024 // 1024,
                max_height=720  # Для поиска ограничиваем 720p
            )

            if not available:
                logger.warning(f"Нет доступных форматов: {item['url']}")
                total_failed += 1
                continue

            format_code, description, height, est_size = available[0]
            
            # Определяем quality_desc по format_id (как в handlers/video.py)
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

            # 4. Создаём или получаем формат
            format_id = db.create_or_get_format(
                video_id=video_db_id,
                format_code=format_code,
                quality_label=quality_desc,
                requested_by_user_id=user_id
            )

            # 5. Проверяем, есть ли уже в кэше ПО КАЧЕСТВУ
            # (YouTube может выдавать разные format_id для одного качества)
            all_formats = db.get_all_formats_for_video(video_db_id)
            cached_format = None
            for fmt in all_formats:
                if fmt.get('quality_label') == quality_desc and fmt.get('telegram_file_id'):
                    cached_format = fmt
                    break
            
            if cached_format:
                # Уже есть в кэше — отправляем сразу
                logger.info(f"Отправка из кэша: {video_db_id} / {quality_desc}")

                # Получаем длительность из БД videos
                video_info = db.get_video_by_internal_id(video_db_id)
                duration = video_info.get('duration', 0) if video_info else 0
                duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "N/A"

                # Экранируем special символы
                escaped_title = item['title'].replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                escaped_uploader = item['uploader'].replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')

                caption = (
                    f"🎬 **[{escaped_title}]({item['url']})**\n\n"
                    f"👤 {escaped_uploader}\n"
                    f"⏱ Длительность: {duration_str}\n"
                    f"📹 Качество: {quality_desc}"
                )

                await bot.send_video(
                    chat_id,
                    video=cached_format['telegram_file_id'],
                    caption=caption,
                    parse_mode="Markdown"
                )

                db.log_request(user_id, video_db_id, format_code, cached_format.get('telegram_file_size', 0), from_cache=True)
                total_added += 1
                continue

            # 6. Добавляем задачу в очередь
            from queue_manager import DownloadTask
            task = DownloadTask(
                task_id=f"{video_db_id}_{format_code}_{int(datetime.now().timestamp())}",
                user_id=user_id,
                username=str(user_id),
                video_url=item['url'],
                video_id=str(video_db_id),
                format_code=format_code,
                quality_label=quality_desc
            )
            await queue_mgr.add_task(task)

            total_added += 1
            await asyncio.sleep(0.2)  # Небольшая задержка

        except Exception as e:
            logger.error(f"Ошибка при добавлении {item['type']}: {e}", exc_info=True)
            total_failed += 1

    logger.info(f"Поиск завершён: добавлено {total_added} видео, ошибок: {total_failed}")

    # Финальное сообщение
    await bot.send_message(
        chat_id,
        f"✅ **Добавлено в очередь!**\n\n"
        f"Видео: {total_added}\n"
        f"Ошибок: {total_failed}\n\n"
        f"📹 Видео будут отправлены по мере загрузки.",
        parse_mode="Markdown"
    )
