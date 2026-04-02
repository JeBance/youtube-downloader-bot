"""
Handlers для YouTube Downloader Bot.
"""
from .commands import router as commands_router
from .video import router as video_router
from .search import router as search_router
from .queue import router as queue_router, process_download_task

__all__ = ['commands_router', 'video_router', 'search_router', 'queue_router', 'process_download_task']
