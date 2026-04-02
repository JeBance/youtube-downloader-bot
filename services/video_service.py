"""
Сервис для работы с видео (получение информации, поиск, загрузка).
"""
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional, Tuple, List

from yt_dlp import YoutubeDL

from config import DOWNLOAD_PATH, DOWNLOAD_TIMEOUT, MAX_FILE_SIZE

logger = logging.getLogger(__name__)

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
