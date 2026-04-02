"""
Новая база данных для YouTube Downloader Bot.

Упрощённая схема:
- videos: основная информация о видео (одно видео = одна запись)
- video_formats: все качества видео со статусами и telegram_file_id
- users: пользователи
- download_requests: история запросов для статистики
"""
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime


class VideoDatabase:
    """Новая схема базы данных."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных."""
        with sqlite3.connect(self.db_path) as conn:
            # Таблица видео (основная)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_url TEXT NOT NULL UNIQUE,
                    youtube_video_id TEXT NOT NULL,
                    title TEXT,
                    uploader TEXT,
                    duration INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Таблица форматов
            conn.execute("""
                CREATE TABLE IF NOT EXISTS video_formats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id INTEGER NOT NULL REFERENCES videos(id),
                    format_code TEXT NOT NULL,
                    quality_label TEXT NOT NULL,
                    requested_by_user_id INTEGER NOT NULL,
                    telegram_file_id TEXT,
                    telegram_file_size INTEGER,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    UNIQUE(video_id, format_code)
                )
            """)

            # Таблица пользователей (совместима со старой схемой)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned INTEGER DEFAULT 0
                )
            """)

            # Таблица запросов (для статистики)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS download_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    video_format_id INTEGER NOT NULL REFERENCES video_formats(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Индексы для скорости
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_videos_youtube_id
                ON videos(youtube_video_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_formats_video
                ON video_formats(video_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_formats_status
                ON video_formats(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_formats_requested_by
                ON video_formats(requested_by_user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_download_requests_user
                ON download_requests(user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_banned
                ON users(is_banned)
            """)

            conn.commit()

    # ============================================================
    # Методы для работы с видео (таблица videos)
    # ============================================================

    def get_video_by_url(self, url: str) -> Optional[Dict]:
        """
        Получить видео по URL.

        Returns:
            dict с id, source_url, youtube_video_id, title, uploader, duration или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM videos WHERE source_url = ?",
                (url,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_video_by_youtube_id(self, youtube_id: str) -> Optional[Dict]:
        """
        Получить видео по YouTube ID.

        Returns:
            dict с информацией о видео или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM videos WHERE youtube_video_id = ?",
                (youtube_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def create_video(
        self,
        source_url: str,
        youtube_video_id: str,
        title: str = "",
        uploader: str = "",
        duration: int = 0
    ) -> int:
        """
        Создать запись о видео.

        Returns:
            id созданной записи или id существующей
        """
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем, нет ли уже такого видео
            cursor = conn.execute(
                "SELECT id FROM videos WHERE source_url = ?",
                (source_url,)
            )
            row = cursor.fetchone()
            if row:
                # Видео уже есть — обновляем метаданные
                conn.execute("""
                    UPDATE videos SET title = ?, uploader = ?, duration = ?
                    WHERE id = ?
                """, (title, uploader, duration, row[0]))
                conn.commit()
                return row[0]

            # Создаём новую запись
            cursor = conn.execute("""
                INSERT INTO videos (source_url, youtube_video_id, title, uploader, duration)
                VALUES (?, ?, ?, ?, ?)
            """, (source_url, youtube_video_id, title, uploader, duration))
            conn.commit()
            return cursor.lastrowid

    def update_video_metadata(
        self,
        video_id: int,
        title: str = None,
        uploader: str = None,
        duration: int = None
    ) -> bool:
        """Обновить метаданные видео."""
        with sqlite3.connect(self.db_path) as conn:
            updates = []
            values = []
            if title is not None:
                updates.append("title = ?")
                values.append(title)
            if uploader is not None:
                updates.append("uploader = ?")
                values.append(uploader)
            if duration is not None:
                updates.append("duration = ?")
                values.append(duration)

            if not updates:
                return True

            values.append(video_id)
            conn.execute(f"""
                UPDATE videos SET {', '.join(updates)}
                WHERE id = ?
            """, values)
            conn.commit()
            return True

    # ============================================================
    # Методы для работы с форматами (таблица video_formats)
    # ============================================================

    def get_format(
        self,
        video_id: int,
        format_code: str
    ) -> Optional[Dict]:
        """
        Получить формат видео по ID видео и коду формата.

        Returns:
            dict с информацией о формате или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM video_formats WHERE video_id = ? AND format_code = ?",
                (video_id, format_code)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_format_by_id(self, format_id: int) -> Optional[Dict]:
        """
        Получить формат по его ID.

        Returns:
            dict с информацией о формате или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM video_formats WHERE id = ?",
                (format_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def create_or_get_format(
        self,
        video_id: int,
        format_code: str,
        quality_label: str,
        requested_by_user_id: int
    ) -> int:
        """
        Создать или получить существующий формат.

        Returns:
            id формата
        """
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем, есть ли уже такой формат
            cursor = conn.execute(
                "SELECT id FROM video_formats WHERE video_id = ? AND format_code = ?",
                (video_id, format_code)
            )
            row = cursor.fetchone()
            if row:
                return row[0]

            # Создаём новый формат
            cursor = conn.execute("""
                INSERT INTO video_formats (video_id, format_code, quality_label, requested_by_user_id)
                VALUES (?, ?, ?, ?)
            """, (video_id, format_code, quality_label, requested_by_user_id))
            conn.commit()
            return cursor.lastrowid

    def get_all_formats_for_video(self, video_id: int) -> List[Dict]:
        """
        Получить все форматы для видео.

        Returns:
            список dict с информацией о форматах
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM video_formats WHERE video_id = ? ORDER BY id",
                (video_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def update_format_telegram_file(
        self,
        format_id: int,
        telegram_file_id: str,
        telegram_file_size: int
    ) -> bool:
        """
        Обновить информацию о загруженном файле в Telegram.

        Также устанавливает status='completed' и completed_at.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE video_formats
                SET telegram_file_id = ?,
                    telegram_file_size = ?,
                    status = 'completed',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (telegram_file_id, telegram_file_size, format_id))
            conn.commit()
            return True

    def update_format_status(
        self,
        format_id: int,
        status: str,
        error_message: str = None
    ) -> bool:
        """
        Обновить статус формата.

        status: 'pending', 'downloading', 'completed', 'failed'
        """
        with sqlite3.connect(self.db_path) as conn:
            if status == 'completed':
                conn.execute("""
                    UPDATE video_formats
                    SET status = ?, completed_at = CURRENT_TIMESTAMP, error_message = ?
                    WHERE id = ?
                """, (status, error_message, format_id))
            elif status == 'failed':
                conn.execute("""
                    UPDATE video_formats
                    SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (status, error_message, format_id))
            else:
                conn.execute("""
                    UPDATE video_formats
                    SET status = ?, error_message = ?
                    WHERE id = ?
                """, (status, error_message, format_id))
            conn.commit()
            return True

    def get_pending_formats(self, limit: int = 50) -> List[Dict]:
        """
        Получить форматы, ожидающие загрузки.

        Returns:
            список форматов со status='pending' или status='downloading'
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT vf.*, v.source_url, v.youtube_video_id, v.title, v.uploader, v.duration
                FROM video_formats vf
                JOIN videos v ON vf.video_id = v.id
                WHERE vf.status IN ('pending', 'downloading')
                ORDER BY vf.created_at ASC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_user_pending_formats(self, user_id: int, limit: int = 20) -> List[Dict]:
        """
        Получить ожидающие форматы конкретного пользователя.

        Returns:
            список форматов
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT vf.*, v.source_url, v.youtube_video_id, v.title
                FROM video_formats vf
                JOIN videos v ON vf.video_id = v.id
                WHERE vf.requested_by_user_id = ?
                AND vf.status NOT IN ('completed', 'failed')
                ORDER BY vf.created_at DESC
                LIMIT ?
            """, (user_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    # ============================================================
    # Методы для работы с пользователями (совместимо со старой схемой)
    # ============================================================

    def add_user(self, user_id: int, username: str = None, first_name: str = None) -> bool:
        """Добавить или обновить пользователя."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO users (user_id, username, first_name, last_seen)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_seen = CURRENT_TIMESTAMP
            """, (user_id, username, first_name))
            conn.commit()
            return True

    def is_banned(self, user_id: int) -> bool:
        """Проверить, забанен ли пользователь."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT is_banned FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            return bool(row and row[0])

    def ban_user(self, user_id: int) -> bool:
        """Забанить пользователя."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE users SET is_banned = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
            return True

    def unban_user(self, user_id: int) -> bool:
        """Разбанить пользователя."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (user_id,))
            conn.commit()
            return True

    def get_all_users(self) -> List[Dict]:
        """Получить всех пользователей."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT user_id, username, first_name, created_at, last_seen, is_banned
                FROM users
                ORDER BY last_seen DESC
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_user_language(self, user_id: int) -> str:
        """Получить предпочтительный язык пользователя."""
        # Для совместимости со старой схемой
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем, есть ли таблица user_settings
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='user_settings'
            """)
            if not cursor.fetchone():
                return "ru"

            cursor = conn.execute("""
                SELECT setting_value FROM user_settings
                WHERE user_id = ? AND setting_key = 'language'
            """, (user_id,))
            row = cursor.fetchone()
            return row[0] if row else "ru"

    def set_user_language(self, user_id: int, language: str) -> bool:
        """Установить предпочтительный язык пользователя."""
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем, есть ли таблица user_settings
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='user_settings'
            """)
            if not cursor.fetchone():
                # Таблицы нет, создаём запись в users (для совместимости)
                conn.execute("""
                    INSERT INTO users (user_id, last_seen)
                    VALUES (?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
                """, (user_id,))
                return True

            conn.execute("""
                INSERT OR REPLACE INTO user_settings (user_id, setting_key, setting_value)
                VALUES (?, 'language', ?)
            """, (user_id, language))
            conn.commit()
            return True

    # ============================================================
    # Методы для статистики
    # ============================================================

    def log_download_request(self, user_id: int, video_format_id: int) -> bool:
        """Записать запрос на загрузку."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO download_requests (user_id, video_format_id)
                VALUES (?, ?)
            """, (user_id, video_format_id))
            conn.commit()
            return True

    def get_stats(self) -> Dict:
        """
        Получить общую статистику.

        Returns:
            dict со статистикой
        """
        with sqlite3.connect(self.db_path) as conn:
            # Количество видео
            cursor = conn.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]

            # Количество форматов
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats")
            total_formats = cursor.fetchone()[0]

            # Загруженные форматы
            cursor = conn.execute("""
                SELECT COUNT(*) FROM video_formats WHERE status = 'completed'
            """)
            completed_formats = cursor.fetchone()[0]

            # Ожидающие форматы
            cursor = conn.execute("""
                SELECT COUNT(*) FROM video_formats WHERE status = 'pending'
            """)
            pending_formats = cursor.fetchone()[0]

            # Скачиваемые форматы
            cursor = conn.execute("""
                SELECT COUNT(*) FROM video_formats WHERE status = 'downloading'
            """)
            downloading_formats = cursor.fetchone()[0]

            # Общий размер загруженных файлов
            cursor = conn.execute("""
                SELECT COALESCE(SUM(telegram_file_size), 0)
                FROM video_formats WHERE status = 'completed'
            """)
            total_size = cursor.fetchone()[0]

            # Количество пользователей
            cursor = conn.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]

            # Забаненные пользователи
            cursor = conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            banned_users = cursor.fetchone()[0]

            # Количество запросов
            cursor = conn.execute("SELECT COUNT(*) FROM download_requests")
            total_requests = cursor.fetchone()[0]

            return {
                "total_videos": total_videos,
                "total_formats": total_formats,
                "completed_formats": completed_formats,
                "pending_formats": pending_formats,
                "downloading_formats": downloading_formats,
                "total_size": total_size,
                "total_users": total_users,
                "banned_users": banned_users,
                "active_users": total_users - banned_users,
                "total_requests": total_requests
            }

    def get_top_users(self, limit: int = 10) -> List[Dict]:
        """Получить топ пользователей по количеству запросов."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT u.user_id, u.username, u.first_name,
                       COUNT(dr.id) as request_count,
                       COUNT(DISTINCT vf.video_id) as video_count
                FROM users u
                LEFT JOIN download_requests dr ON u.user_id = dr.user_id
                LEFT JOIN video_formats vf ON dr.video_format_id = vf.id
                GROUP BY u.user_id
                ORDER BY request_count DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_queue_stats(self) -> Dict:
        """Получить статистику очереди."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) as count
                FROM video_formats
                GROUP BY status
            """)
            stats = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                "pending": stats.get("pending", 0),
                "downloading": stats.get("downloading", 0),
                "completed": stats.get("completed", 0),
                "failed": stats.get("failed", 0),
                "total": sum(stats.values())
            }

    # ============================================================
    # Методы для очистки
    # ============================================================

    def clear_completed_formats(self) -> int:
        """
        Очистить завершённые форматы.

        Returns:
            количество удалённых записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE status = 'completed'")
            count = cursor.fetchone()[0]
            conn.execute("DELETE FROM video_formats WHERE status = 'completed'")
            conn.commit()
            return count

    def clear_all_formats(self) -> int:
        """
        Очистить все форматы.

        Returns:
            количество удалённых записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats")
            count = cursor.fetchone()[0]
            conn.execute("DELETE FROM video_formats")
            conn.commit()
            return count

    def clear_all(self) -> int:
        """
        Очистить все данные (кроме таблицы users).

        Returns:
            количество удалённых записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM videos")
            videos_count = cursor.fetchone()[0]
            conn.execute("DELETE FROM videos")

            cursor = conn.execute("SELECT COUNT(*) FROM video_formats")
            formats_count = cursor.fetchone()[0]
            conn.execute("DELETE FROM video_formats")

            cursor = conn.execute("SELECT COUNT(*) FROM download_requests")
            requests_count = cursor.fetchone()[0]
            conn.execute("DELETE FROM download_requests")

            conn.commit()
            return videos_count + formats_count + requests_count
