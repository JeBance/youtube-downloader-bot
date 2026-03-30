"""
База данных для кэширования file_id видео.
"""
import sqlite3
from pathlib import Path
from typing import Optional, List
from datetime import datetime


class VideoCache:
    """Кэш file_id для загруженных видео."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS video_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    format_code TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_size INTEGER,
                    quality_label TEXT,
                    title TEXT,
                    duration INTEGER,
                    uploader TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(video_id, format_code)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS video_urls (
                    video_id TEXT PRIMARY KEY,
                    source_url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    format_code TEXT NOT NULL,
                    file_size INTEGER,
                    from_cache INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_id 
                ON video_cache(video_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_banned
                ON users(is_banned)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_requests_user
                ON requests(user_id)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    format_code TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_stats_user
                ON queue_stats(user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_queue_stats_status
                ON queue_stats(status)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    setting_key TEXT NOT NULL,
                    setting_value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, setting_key)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_settings_user
                ON user_settings(user_id)
            """)
            conn.commit()
    
    def get(self, video_id: str, format_code: str) -> Optional[dict]:
        """
        Получить file_id из кэша.
        
        Returns:
            dict с file_id, file_size, quality_label, title, duration, uploader или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT file_id, file_size, quality_label, title, duration, uploader, created_at "
                "FROM video_cache "
                "WHERE video_id = ? AND format_code = ?",
                (video_id, format_code)
            )
            row = cursor.fetchone()
            
            if row:
                return {
                    "file_id": row["file_id"],
                    "file_size": row["file_size"],
                    "quality_label": row["quality_label"],
                    "title": row["title"],
                    "duration": row["duration"],
                    "uploader": row["uploader"],
                    "created_at": row["created_at"]
                }
            return None
    
    def set(self, video_id: str, format_code: str, file_id: str,
            file_size: int = 0, quality_label: str = "",
            title: str = "", duration: int = 0, uploader: str = "",
            source_url: str = "") -> bool:
        """
        Сохранить file_id в кэш.

        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO video_cache
                    (video_id, format_code, file_id, file_size, quality_label, title, duration, uploader, source_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (video_id, format_code, file_id, file_size, quality_label, title, duration, uploader, source_url))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error saving to cache: {e}")
            return False
    
    def get_all_for_video(self, video_id: str) -> list:
        """
        Получить все закэшированные форматы для видео.

        Returns:
            список (format_code, quality_label)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT format_code, quality_label "
                "FROM video_cache "
                "WHERE video_id = ?",
                (video_id,)
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_url_for_video(self, video_id: str) -> Optional[str]:
        """
        Получить URL для видео по video_id.

        Returns:
            URL или None
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT source_url FROM video_urls WHERE video_id = ?",
                (video_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def set_url_for_video(self, video_id: str, url: str) -> bool:
        """
        Сохранить URL для видео.

        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO video_urls (video_id, source_url)
                    VALUES (?, ?)
                """, (video_id, url))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error saving URL: {e}")
            return False
    
    def count(self) -> int:
        """Получить количество записей в кэше."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_cache")
            return cursor.fetchone()[0]
    
    def clear(self) -> int:
        """
        Очистить кэш.
        
        Returns:
            количество удалённых записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_cache")
            count = cursor.fetchone()[0]
            conn.execute("DELETE FROM video_cache")
            conn.commit()
            return count
    
    def get_stats(self) -> dict:
        """Получить статистику кэша."""
        with sqlite3.connect(self.db_path) as conn:
            # Общее количество
            cursor = conn.execute("SELECT COUNT(*) FROM video_cache")
            total_files = cursor.fetchone()[0]
            
            # Общий размер
            cursor = conn.execute("SELECT COALESCE(SUM(file_size), 0) FROM video_cache")
            total_size = cursor.fetchone()[0]
            
            # Количество уникальных видео
            cursor = conn.execute("SELECT COUNT(DISTINCT video_id) FROM video_cache")
            total_videos = cursor.fetchone()[0]
            
            return {
                "total_files": total_files,
                "total_size": total_size,
                "total_videos": total_videos
            }
    
    # === Методы для работы с пользователями ===
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None) -> bool:
        """Добавить или обновить пользователя."""
        try:
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
        except Exception as e:
            print(f"Error adding user: {e}")
            return False

    def set_user_language(self, user_id: int, language: str) -> bool:
        """Установить предпочтительный язык для пользователя."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Сначала убедимся что пользователь есть
                conn.execute("""
                    INSERT INTO users (user_id, last_seen)
                    VALUES (?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
                """, (user_id,))
                # Добавляем или обновляем язык в отдельной таблице
                conn.execute("""
                    INSERT OR REPLACE INTO user_settings (user_id, setting_key, setting_value)
                    VALUES (?, 'language', ?)
                """, (user_id, language))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error setting user language: {e}")
            return False

    def get_user_language(self, user_id: int) -> str:
        """Получить предпочтительный язык пользователя."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT setting_value FROM user_settings 
                    WHERE user_id = ? AND setting_key = 'language'
                """, (user_id,))
                row = cursor.fetchone()
                return row[0] if row else "ru"  # По умолчанию русский
        except Exception as e:
            print(f"Error getting user language: {e}")
            return "ru"
    
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
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE users SET is_banned = 1 WHERE user_id = ?",
                    (user_id,)
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Error banning user: {e}")
            return False
    
    def unban_user(self, user_id: int) -> bool:
        """Разбанить пользователя."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE users SET is_banned = 0 WHERE user_id = ?",
                    (user_id,)
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Error unbanning user: {e}")
            return False
    
    def get_all_users(self) -> List[dict]:
        """Получить всех пользователей."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT user_id, username, first_name, created_at, last_seen, is_banned
                FROM users
                ORDER BY last_seen DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def log_request(self, user_id: int, video_id: str, format_code: str, 
                    file_size: int = 0, from_cache: bool = False) -> bool:
        """Записать запрос на загрузку."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO requests (user_id, video_id, format_code, file_size, from_cache)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, video_id, format_code, file_size, 1 if from_cache else 0))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error logging request: {e}")
            return False
    
    def get_detailed_stats(self) -> dict:
        """Получить подробную статистику."""
        with sqlite3.connect(self.db_path) as conn:
            # Пользователи
            cursor = conn.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            banned_users = cursor.fetchone()[0]
            
            # Запросы
            cursor = conn.execute("SELECT COUNT(*) FROM requests")
            total_requests = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM requests WHERE from_cache = 1")
            cache_hits = cursor.fetchone()[0]
            
            # Кэш
            cursor = conn.execute("SELECT COUNT(*) FROM video_cache")
            cached_files = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COALESCE(SUM(file_size), 0) FROM video_cache")
            cache_size = cursor.fetchone()[0]
            
            return {
                "total_users": total_users,
                "banned_users": banned_users,
                "active_users": total_users - banned_users,
                "total_requests": total_requests,
                "cache_hits": cache_hits,
                "cache_misses": total_requests - cache_hits,
                "cached_files": cached_files,
                "cache_size": cache_size
            }
    
    def get_top_users(self, limit: int = 10) -> List[dict]:
        """Получить топ пользователей по количеству скачанных видео."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT u.user_id, u.username, u.first_name, COUNT(DISTINCT r.video_id) as video_count
                FROM users u
                LEFT JOIN requests r ON u.user_id = r.user_id
                GROUP BY u.user_id
                ORDER BY video_count DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # === Методы для работы с очередью ===

    def log_queue_task(
        self, user_id: int, video_id: str, format_code: str,
        status: str = "pending"
    ) -> bool:
        """Записать задачу в очередь."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO queue_stats (user_id, video_id, format_code, status)
                    VALUES (?, ?, ?, ?)
                """, (user_id, video_id, format_code, status))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error logging queue task: {e}")
            return False

    def update_queue_task_status(
        self, user_id: int, video_id: str, format_code: str,
        status: str, error_message: str = None
    ) -> bool:
        """Обновить статус задачи в очереди."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                if status == "started":
                    conn.execute("""
                        UPDATE queue_stats
                        SET status = ?, started_at = CURRENT_TIMESTAMP
                        WHERE user_id = ? AND video_id = ? AND format_code = ?
                    """, (status, user_id, video_id, format_code))
                elif status in ("completed", "failed", "cancelled"):
                    conn.execute("""
                        UPDATE queue_stats
                        SET status = ?, completed_at = CURRENT_TIMESTAMP, error_message = ?
                        WHERE user_id = ? AND video_id = ? AND format_code = ?
                    """, (status, error_message, user_id, video_id, format_code))
                else:
                    conn.execute("""
                        UPDATE queue_stats
                        SET status = ?
                        WHERE user_id = ? AND video_id = ? AND format_code = ?
                    """, (status, user_id, video_id, format_code))
                conn.commit()
            return True
        except Exception as e:
            print(f"Error updating queue task: {e}")
            return False

    def get_user_queue(self, user_id: int, limit: int = 20) -> List[dict]:
        """Получить очередь пользователя."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT video_id, format_code, status, created_at, started_at, completed_at, error_message
                FROM queue_stats
                WHERE user_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')
                ORDER BY created_at DESC
                LIMIT ?
            """, (user_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_queue_stats(self) -> dict:
        """Получить статистику очереди."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM queue_stats WHERE status = 'pending'")
            pending = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM queue_stats WHERE status = 'downloading'")
            downloading = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM queue_stats WHERE status = 'uploading'")
            uploading = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM queue_stats WHERE status = 'completed'")
            completed = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM queue_stats WHERE status = 'failed'")
            failed = cursor.fetchone()[0]

            return {
                "pending": pending,
                "downloading": downloading,
                "uploading": uploading,
                "completed": completed,
                "failed": failed,
                "total": pending + downloading + uploading + completed + failed
            }
