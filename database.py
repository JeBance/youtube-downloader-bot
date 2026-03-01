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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(video_id, format_code)
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
            conn.commit()
    
    def get(self, video_id: str, format_code: str) -> Optional[dict]:
        """
        Получить file_id из кэша.
        
        Returns:
            dict с file_id, file_size, quality_label или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT file_id, file_size, quality_label, created_at "
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
                    "created_at": row["created_at"]
                }
            return None
    
    def set(self, video_id: str, format_code: str, file_id: str, 
            file_size: int = 0, quality_label: str = "") -> bool:
        """
        Сохранить file_id в кэш.
        
        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO video_cache 
                    (video_id, format_code, file_id, file_size, quality_label)
                    VALUES (?, ?, ?, ?, ?)
                """, (video_id, format_code, file_id, file_size, quality_label))
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
        """Получить топ пользователей по количеству запросов."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT u.user_id, u.username, u.first_name, COUNT(r.id) as request_count
                FROM users u
                LEFT JOIN requests r ON u.user_id = r.user_id
                GROUP BY u.user_id
                ORDER BY request_count DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
