"""
База данных для кэширования file_id видео.
"""
import sqlite3
from pathlib import Path
from typing import Optional
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
                CREATE INDEX IF NOT EXISTS idx_video_id 
                ON video_cache(video_id)
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
            список format_code
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
