"""
Менеджер очереди загрузок для YouTube Downloader Bot.

Реализует:
- Честное распределение (Fair Queue) между пользователями
- Лимиты на запросы к YouTube (RPS)
- Лимиты на загрузку в Telegram (uploads/min)
- Приоритеты и статусы задач
"""
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Optional, Dict, List
from collections import deque

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Статусы задачи в очереди."""
    PENDING = "pending"           # Ожидает в очереди
    QUEUED = "queued"             # В очереди на загрузку
    DOWNLOADING = "downloading"   # Скачивается с YouTube
    UPLOADING = "uploading"       # Загружается в Telegram
    COMPLETED = "completed"       # Завершена
    FAILED = "failed"             # Ошибка
    CANCELLED = "cancelled"       # Отменена пользователем


@dataclass
class DownloadTask:
    """Задача на загрузку."""
    task_id: str
    user_id: int
    username: str
    video_url: str
    video_id: str
    format_code: str
    quality_label: str
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    error_message: Optional[str] = None
    file_size: Optional[int] = None
    file_path: Optional[str] = None
    callback_query: Optional[object] = field(default=None, repr=False)
    
    # Для отслеживания прогресса
    progress_percent: float = 0.0
    status_message: Optional[str] = None
    
    def __post_init__(self):
        # Генерируем уникальный task_id если не передан
        if not self.task_id:
            self.task_id = f"{self.video_id}_{self.format_code}_{int(self.created_at.timestamp())}"


class RateLimiter:
    """
    Ограничитель частоты запросов (Rate Limiter).
    
    Реализует token bucket algorithm для контроля RPS.
    """
    def __init__(self, rate: float, burst: int = 1):
        """
        Args:
            rate: Количество запросов в секунду
            burst: Максимальное количество запросов подряд (пакет)
        """
        self.rate = rate  # запросов в секунду
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Ждёт доступного токена."""
        async with self._lock:
            while True:
                now = datetime.now()
                elapsed = (now - self.last_update).total_seconds()
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                
                # Ждём пока появится токен
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)


class FairQueueManager:
    """
    Менеджер очереди с честным распределением (Fair Queue).
    
    Принцип работы:
    - Каждый пользователь имеет свою персональную очередь
    - Обработка идёт по принципу round-robin между пользователями
    - Если у пользователя 100 задач, а у другого 10 — они получают равный приоритет
    - Глобальные лимиты на YouTube и Telegram соблюдаются для всех
    """
    
    def __init__(
        self,
        youtube_rps: float = 1.0,      # Запросов к YouTube в секунду
        telegram_uploads_per_min: int = 15,  # Загрузок в Telegram в минуту
        max_concurrent_downloads: int = 2,   # Макс. одновременных загрузок
        max_queue_per_user: int = 50         # Макс. задач в очереди на пользователя
    ):
        """
        Args:
            youtube_rps: Лимит запросов к YouTube в секунду (рекомендуется 1-2)
            telegram_uploads_per_min: Лимит загрузок в Telegram в минуту
            max_concurrent_downloads: Максимум одновременных загрузок
            max_queue_per_user: Максимум задач в очереди на одного пользователя
        """
        self.youtube_rps = youtube_rps
        self.telegram_uploads_per_min = telegram_uploads_per_min
        self.max_concurrent_downloads = max_concurrent_downloads
        self.max_queue_per_user = max_queue_per_user
        
        # Персональные очереди пользователей: user_id -> deque задач
        self._user_queues: Dict[int, deque[DownloadTask]] = defaultdict(deque)
        
        # Глобальная очередь для round-robin обработки
        self._global_queue: deque[DownloadTask] = deque()
        
        # Активные задачи (скачиваются/загружаются)
        self._active_tasks: Dict[str, DownloadTask] = {}
        
        # Все задачи по task_id для быстрого доступа
        self._all_tasks: Dict[str, DownloadTask] = {}
        
        # Блокировки
        self._queue_lock = asyncio.Lock()
        self._active_lock = asyncio.Lock()
        
        # Rate limiters
        self._youtube_limiter = RateLimiter(youtube_rps, burst=2)
        
        # Для Telegram: вычисляем rate из uploads per minute
        telegram_rate = telegram_uploads_per_min / 60.0
        self._telegram_limiter = RateLimiter(telegram_rate, burst=3)
        
        # Семафор для ограничения одновременных загрузок
        self._download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        
        # Флаг работы менеджера
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        
        # Callback для обработки задачи
        self._process_callback: Optional[Callable] = None
        
        # Статистика
        self._stats = {
            "total_processed": 0,
            "total_failed": 0,
            "total_cancelled": 0,
            "by_user": defaultdict(lambda: {"processed": 0, "failed": 0})
        }
        
        logger.info(
            f"FairQueueManager инициализирован: "
            f"YouTube RPS={youtube_rps}, Telegram uploads/min={telegram_uploads_per_min}, "
            f"max_concurrent={max_concurrent_downloads}"
        )
    
    async def start(self, process_callback: Callable):
        """
        Запуск менеджера очереди.
        
        Args:
            process_callback: Асинхронная функция для обработки задачи
                           signature: async def process(task: DownloadTask)
        """
        if self._running:
            logger.warning("FairQueueManager уже запущен")
            return
        
        self._process_callback = process_callback
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("FairQueueManager запущен")
    
    async def stop(self):
        """Остановка менеджера очереди."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("FairQueueManager остановлен")
    
    async def add_task(self, task: DownloadTask) -> bool:
        """
        Добавить задачу в очередь.
        
        Args:
            task: Задача на загрузку
            
        Returns:
            True если добавлена, False если превышен лимит
        """
        async with self._queue_lock:
            # Проверяем лимит на пользователя
            user_queue = self._user_queues[task.user_id]
            if len(user_queue) >= self.max_queue_per_user:
                logger.warning(
                    f"Пользователь {task.user_id} превысил лимит очереди "
                    f"({len(user_queue)}/{self.max_queue_per_user})"
                )
                return False
            
            # Проверяем дубликаты
            for existing in user_queue:
                if (existing.video_id == task.video_id and 
                    existing.format_code == task.format_code):
                    logger.warning(f"Дубликат задачи: {task.task_id}")
                    return False
            
            # Добавляем в очередь пользователя
            task.status = TaskStatus.QUEUED
            user_queue.append(task)
            self._all_tasks[task.task_id] = task
            
            # Добавляем в глобальную очередь (в конец для fair scheduling)
            self._global_queue.append(task)
            
            logger.info(
                f"Задача добавлена: {task.task_id} "
                f"(user={task.user_id}, video={task.video_id}, "
                f"queue_size={len(user_queue)})"
            )
            return True
    
    async def cancel_task(self, user_id: int, task_id: str) -> bool:
        """
        Отменить задачу пользователя.
        
        Args:
            user_id: ID пользователя
            task_id: ID задачи
            
        Returns:
            True если отменена, False если не найдена или уже выполняется
        """
        async with self._queue_lock:
            user_queue = self._user_queues[user_id]
            
            for i, task in enumerate(user_queue):
                if task.task_id == task_id:
                    if task.status in (TaskStatus.DOWNLOADING, TaskStatus.UPLOADING):
                        logger.warning(f"Нельзя отменить активную задачу {task_id}")
                        return False
                    
                    task.status = TaskStatus.CANCELLED
                    user_queue.remove(task)
                    
                    # Удаляем из глобальной очереди
                    try:
                        self._global_queue.remove(task)
                    except ValueError:
                        pass
                    
                    self._stats["total_cancelled"] += 1
                    logger.info(f"Задача отменена: {task_id}")
                    return True
            
            return False
    
    async def get_user_queue_status(self, user_id: int) -> dict:
        """
        Получить статус очереди пользователя.
        
        Returns:
            dict с информацией об очереди пользователя
        """
        async with self._queue_lock:
            user_queue = self._user_queues[user_id]
            
            tasks = []
            for task in user_queue:
                tasks.append({
                    "task_id": task.task_id,
                    "video_id": task.video_id,
                    "quality": task.quality_label,
                    "status": task.status.value,
                    "position": list(user_queue).index(task) + 1,
                    "created_at": task.created_at.isoformat()
                })
            
            return {
                "user_id": user_id,
                "queue_size": len(user_queue),
                "tasks": tasks,
                "limit": self.max_queue_per_user
            }
    
    async def get_task_status(self, task_id: str) -> Optional[DownloadTask]:
        """Получить статус задачи по ID."""
        return self._all_tasks.get(task_id)
    
    async def _worker_loop(self):
        """
        Основной цикл обработки очереди.
        
        Работает по принципу round-robin:
        1. Берёт задачу из глобальной очереди
        2. Проверяет лимиты
        3. Обрабатывает через callback
        4. Возвращает задачу в конец если не готова
        """
        while self._running:
            try:
                task = await self._get_next_task()
                
                if task is None:
                    # Очередь пуста, ждём
                    await asyncio.sleep(0.5)
                    continue
                
                # Проверяем, не отменена ли задача
                if task.status == TaskStatus.CANCELLED:
                    logger.debug(f"Пропуск отменённой задачи: {task.task_id}")
                    continue
                
                # Пытаемся начать обработку
                if await self._try_process_task(task):
                    # Задача началась, удаляем из очередей
                    await self._remove_from_queues(task)
                else:
                    # Не готовы, возвращаем в конец очереди
                    async with self._queue_lock:
                        self._global_queue.append(task)
                        # Небольшая задержка чтобы не зацикливаться
                        await asyncio.sleep(0.1)
                        
            except asyncio.CancelledError:
                logger.info("Worker loop cancelled")
                break
            except Exception as e:
                logger.error(f"Ошибка в worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _get_next_task(self) -> Optional[DownloadTask]:
        """
        Получить следующую задачу для обработки (round-robin).
        
        Returns:
            Задача или None если очередь пуста
        """
        async with self._queue_lock:
            if not self._global_queue:
                return None
            
            # Берём первую задачу из глобальной очереди
            # Это обеспечивает fair scheduling между пользователями
            return self._global_queue.popleft()
    
    async def _try_process_task(self, task: DownloadTask) -> bool:
        """
        Попытаться обработать задачу.
        
        Returns:
            True если задача началась, False если нужно подождать
        """
        # Проверяем семафор одновременных загрузок
        if self._download_semaphore.locked():
            return False
        
        async with self._download_semaphore:
            # Ждём лимит YouTube
            await self._youtube_limiter.acquire()
            
            # Обновляем статус
            task.status = TaskStatus.DOWNLOADING
            task.started_at = datetime.now()
            
            async with self._active_lock:
                self._active_tasks[task.task_id] = task
            
            logger.info(f"Начата обработка: {task.task_id}")
            
            # Запускаем обработку в фоне
            asyncio.create_task(self._process_task_safe(task))
            
            return True
    
    async def _process_task_safe(self, task: DownloadTask):
        """
        Безопасная обработка задачи с перехватом ошибок.
        """
        try:
            if self._process_callback:
                # Ждём лимит Telegram перед отправкой
                await self._telegram_limiter.acquire()
                
                task.status = TaskStatus.UPLOADING
                await self._process_callback(task)
                
                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.now()
                
                self._stats["total_processed"] += 1
                self._stats["by_user"][task.user_id]["processed"] += 1
                
                logger.info(f"Задача завершена: {task.task_id}")
            else:
                raise RuntimeError("Process callback not set")
                
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            self._stats["total_cancelled"] += 1
            logger.info(f"Задача отменена: {task.task_id}")
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_message = str(e)
            self._stats["total_failed"] += 1
            self._stats["by_user"][task.user_id]["failed"] += 1
            
            logger.error(f"Задача провалена: {task.task_id} - {e}", exc_info=True)
            
        finally:
            # Удаляем из активных
            async with self._active_lock:
                self._active_tasks.pop(task.task_id, None)
    
    async def _remove_from_queues(self, task: DownloadTask):
        """Удалить задачу из всех очередей."""
        async with self._queue_lock:
            user_queue = self._user_queues[task.user_id]
            try:
                user_queue.remove(task)
            except ValueError:
                pass
            
            # Также удаляем из глобальной очереди если там есть
            try:
                self._global_queue.remove(task)
            except ValueError:
                pass
    
    def get_stats(self) -> dict:
        """Получить статистику менеджера."""
        async def _get_stats():
            async with self._queue_lock:
                total_queued = sum(len(q) for q in self._user_queues.values())
            
            async with self._active_lock:
                active_count = len(self._active_tasks)
            
            return {
                "queued": total_queued,
                "active": active_count,
                "total_processed": self._stats["total_processed"],
                "total_failed": self._stats["total_failed"],
                "total_cancelled": self._stats["total_cancelled"],
                "by_user": dict(self._stats["by_user"]),
                "youtube_rps": self.youtube_rps,
                "telegram_uploads_per_min": self.telegram_uploads_per_min,
                "max_concurrent": self.max_concurrent_downloads
            }
        
        return asyncio.run(_get_stats())


# Глобальный экземпляр (будет инициализирован в bot.py)
queue_manager: Optional[FairQueueManager] = None


def init_queue_manager(
    youtube_rps: float = 1.0,
    telegram_uploads_per_min: int = 15,
    max_concurrent_downloads: int = 2,
    max_queue_per_user: int = 50
) -> FairQueueManager:
    """
    Инициализировать глобальный менеджер очереди.
    
    Args:
        youtube_rps: Лимит запросов к YouTube в секунду
        telegram_uploads_per_min: Лимит загрузок в Telegram в минуту
        max_concurrent_downloads: Макс. одновременных загрузок
        max_queue_per_user: Макс. задач на пользователя
        
    Returns:
        Экземпляр FairQueueManager
    """
    global queue_manager
    queue_manager = FairQueueManager(
        youtube_rps=youtube_rps,
        telegram_uploads_per_min=telegram_uploads_per_min,
        max_concurrent_downloads=max_concurrent_downloads,
        max_queue_per_user=max_queue_per_user
    )
    return queue_manager


def get_queue_manager() -> FairQueueManager:
    """Получить глобальный менеджер очереди."""
    if queue_manager is None:
        raise RuntimeError("Queue manager not initialized. Call init_queue_manager() first.")
    return queue_manager
