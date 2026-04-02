"""
База данных для кэширования file_id видео.
"""
import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class VideoCache:
    """Кэш file_id для загруженных видео."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных."""
        with sqlite3.connect(self.db_path) as conn:
            # Таблица videos (основная)
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

            # Таблица video_formats
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    language TEXT DEFAULT 'ru',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned INTEGER DEFAULT 0
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS download_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    video_format_id INTEGER NOT NULL REFERENCES video_formats(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_banned
                ON users(is_banned)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_download_requests_user
                ON download_requests(user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_formats_video
                ON video_formats(video_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_formats_status
                ON video_formats(status)
            """)
            conn.commit()

    # ============================================================
    # НОВЫЕ МЕТОДЫ ДЛЯ НОВОЙ СХЕМЫ (database_new.py совместимость)
    # ============================================================

    def get_video_by_url(self, url: str) -> Optional[Dict]:
        """Получить видео по URL (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            # Проверяем, есть ли таблица videos
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='videos'
            """)
            if not cursor.fetchone():
                return None
            
            cursor = conn.execute(
                "SELECT * FROM videos WHERE source_url = ?",
                (url,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_video_by_youtube_id(self, youtube_id: str) -> Optional[Dict]:
        """Получить видео по YouTube ID (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='videos'
            """)
            if not cursor.fetchone():
                return None
            
            cursor = conn.execute(
                "SELECT * FROM videos WHERE youtube_video_id = ?",
                (youtube_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_video_by_internal_id(self, video_id: int) -> Optional[Dict]:
        """Получить видео по внутреннему ID записи (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='videos'
            """)
            if not cursor.fetchone():
                return None
            
            cursor = conn.execute(
                "SELECT * FROM videos WHERE id = ?",
                (video_id,)
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
        """Создать запись о видео (новая схема)."""
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

    def get_format(self, video_id: int, format_code: str) -> Optional[Dict]:
        """Получить формат видео (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='video_formats'
            """)
            if not cursor.fetchone():
                return None
            
            cursor = conn.execute(
                "SELECT * FROM video_formats WHERE video_id = ? AND format_code = ?",
                (video_id, format_code)
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
        """Создать или получить формат (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT id FROM video_formats WHERE video_id = ? AND format_code = ?",
                (video_id, format_code)
            )
            row = cursor.fetchone()
            if row:
                return row[0]

            cursor = conn.execute("""
                INSERT INTO video_formats (video_id, format_code, quality_label, requested_by_user_id)
                VALUES (?, ?, ?, ?)
            """, (video_id, format_code, quality_label, requested_by_user_id))
            conn.commit()
            return cursor.lastrowid

    def get_all_formats_for_video(self, video_id: int) -> List[Dict]:
        """Получить все форматы для видео (новая схема)."""
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
        """Обновить информацию о файле Telegram (новая схема)."""
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
        """Обновить статус формата (новая схема)."""
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
        """Получить ожидающие форматы (новая схема)."""
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
        """Получить ожидающие форматы пользователя (новая схема)."""
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

    def log_download_request(self, user_id: int, video_format_id: int) -> bool:
        """Записать запрос на загрузку (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO download_requests (user_id, video_format_id)
                VALUES (?, ?)
            """, (user_id, video_format_id))
            conn.commit()
            return True

    def get_new_stats(self) -> Dict:
        """Получить статистику по новой схеме."""
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем наличие новых таблиц
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='videos'
            """)
            if not cursor.fetchone():
                return {}
            
            cursor = conn.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM video_formats")
            total_formats = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE status = 'completed'")
            completed_formats = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE status = 'pending'")
            pending_formats = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COALESCE(SUM(telegram_file_size), 0) FROM video_formats WHERE status = 'completed'")
            total_size = cursor.fetchone()[0]

            return {
                "total_videos": total_videos,
                "total_formats": total_formats,
                "completed_formats": completed_formats,
                "pending_formats": pending_formats,
                "total_size": total_size
            }

    # ============================================================
    # СТАРЫЕ МЕТОДЫ (для обратной совместимости)
    # ============================================================

    def get(self, video_id: str, format_code: str) -> Optional[dict]:
        """
        Получить file_id из кэша.

        Args:
            video_id: ID видео в БД (INTEGER) или YouTube ID (TEXT)
            format_code: код формата

        Returns:
            dict с file_id, file_size, quality_label, title, duration, uploader или None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Проверяем, есть ли таблица video_formats (новая схема)
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='video_formats'
            """)
            if cursor.fetchone():
                # Новая схема: video_id это INTEGER ID из videos
                try:
                    video_id_int = int(video_id)
                    cursor = conn.execute("""
                        SELECT vf.telegram_file_id as file_id,
                               vf.telegram_file_size as file_size,
                               vf.quality_label,
                               v.title,
                               v.duration,
                               v.uploader,
                               vf.created_at
                        FROM video_formats vf
                        JOIN videos v ON vf.video_id = v.id
                        WHERE vf.video_id = ? AND vf.format_code = ?
                        AND vf.telegram_file_id IS NOT NULL
                    """, (video_id_int, format_code))
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
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка конвертации video_id в get(): {e}")
                    return None
                
            logger.error("Таблица video_formats не найдена в get()!")
            return None
    
    def set(self, video_id: str, format_code: str, file_id: str,
            file_size: int = 0, quality_label: str = "",
            title: str = "", duration: int = 0, uploader: str = "",
            source_url: str = "") -> bool:
        """
        Сохранить file_id в кэш.

        Args:
            video_id: ID видео в БД (INTEGER) или YouTube ID (TEXT)
            format_code: код формата
            file_id: telegram file_id
            file_size: размер файла
            quality_label: описание качества
            title: заголовок видео
            duration: длительность
            uploader: автор

        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Проверяем, есть ли таблица video_formats (новая схема)
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='video_formats'
                """)
                if cursor.fetchone():
                    # Новая схема: video_id это INTEGER ID из videos
                    try:
                        video_id_int = int(video_id)
                        
                        # Находим format_id
                        cursor = conn.execute("""
                            SELECT id FROM video_formats
                            WHERE video_id = ? AND format_code = ?
                        """, (video_id_int, format_code))
                        row = cursor.fetchone()
                        
                        if row:
                            format_id = row[0]
                            # Обновляем запись
                            conn.execute("""
                                UPDATE video_formats
                                SET telegram_file_id = ?,
                                    telegram_file_size = ?,
                                    quality_label = ?,
                                    status = 'completed',
                                    completed_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (file_id, file_size, quality_label, format_id))
                            conn.commit()

                            # Также обновляем метаданные видео если они переданы
                            if title or uploader or duration:
                                conn.execute("""
                                    UPDATE videos
                                    SET title = COALESCE(?, title),
                                        uploader = COALESCE(?, uploader),
                                        duration = COALESCE(?, duration)
                                    WHERE id = ?
                                """, (title, uploader, duration, video_id_int))
                                conn.commit()

                            logger.info(f"Обновлён формат {format_id}: telegram_file_id={file_id[:20]}...")
                            return True
                        else:
                            logger.warning(f"Формат не найден: video_id={video_id_int}, format_code={format_code}")
                            return False
                    except (ValueError, TypeError) as e:
                        logger.error(f"Ошибка конвертации video_id: {e}")
                        return False
                
                logger.error("Таблица video_formats не найдена!")
                return False
        except Exception as e:
            logger.error(f"Error saving to cache: {e}")
            return False
    
    def get_all_for_video(self, video_id: str) -> list:
        """
        Получить все закэшированные форматы для видео.

        Args:
            video_id: ID видео в БД (INTEGER) или YouTube ID (TEXT)

        Returns:
            список (format_code, quality_label)
        """
        with sqlite3.connect(self.db_path) as conn:
            # Проверяем, есть ли таблица video_formats (новая схема)
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='video_formats'
            """)
            if cursor.fetchone():
                # Новая схема: video_id это INTEGER ID из videos
                try:
                    video_id_int = int(video_id)
                    cursor = conn.execute("""
                        SELECT format_code, quality_label
                        FROM video_formats
                        WHERE video_id = ? AND telegram_file_id IS NOT NULL
                    """, (video_id_int,))
                    return [(row[0], row[1]) for row in cursor.fetchall()]
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка конвертации video_id в get_all_for_video(): {e}")
                    return []
            
            logger.error("Таблица video_formats не найдена в get_all_for_video()!")
            return []

    def get_url_for_video(self, video_id: str) -> Optional[str]:
        """
        Получить URL для видео по video_id (новая схема).

        Returns:
            URL или None
        """
        with sqlite3.connect(self.db_path) as conn:
            try:
                video_id_int = int(video_id)
                cursor = conn.execute(
                    "SELECT source_url FROM videos WHERE id = ?",
                    (video_id_int,)
                )
                row = cursor.fetchone()
                return row[0] if row else None
            except (ValueError, TypeError):
                logger.error(f"Ошибка конвертации video_id в get_url_for_video(): {e}")
                return None

    def set_url_for_video(self, video_id: str, url: str) -> bool:
        """
        Сохранить URL для видео (новая схема).

        Returns:
            True если успешно
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                video_id_int = int(video_id)
                conn.execute("""
                    UPDATE videos SET source_url = ? WHERE id = ?
                """, (url, video_id_int))
                conn.commit()
            return True
        except (ValueError, TypeError) as e:
            logger.error(f"Ошибка конвертации video_id в set_url_for_video(): {e}")
            return False
    
    def count(self) -> int:
        """Получить количество записей в кэше (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            return cursor.fetchone()[0]

    def clear(self) -> int:
        """
        Очистить кэш (новая схема).

        Returns:
            количество удалённых записей
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            count = cursor.fetchone()[0]
            conn.execute("UPDATE video_formats SET telegram_file_id = NULL, telegram_file_size = 0 WHERE telegram_file_id IS NOT NULL")
            conn.commit()
            return count
    
    def get_stats(self) -> dict:
        """Получить статистику кэша (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            # Количество закэшированных файлов
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            total_files = cursor.fetchone()[0]

            # Общий размер
            cursor = conn.execute("SELECT COALESCE(SUM(telegram_file_size), 0) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            total_size = cursor.fetchone()[0]

            # Количество уникальных видео
            cursor = conn.execute("SELECT COUNT(DISTINCT video_id) FROM video_formats WHERE telegram_file_id IS NOT NULL")
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
        """Установить предпочтительный язык для пользователя (новая схема)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO users (user_id, language, last_seen)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        language = excluded.language,
                        last_seen = CURRENT_TIMESTAMP
                """, (user_id, language))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error setting user language: {e}")
            return False

    def get_user_language(self, user_id: int) -> str:
        """Получить предпочтительный язык пользователя (новая схема)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT language FROM users
                    WHERE user_id = ?
                """, (user_id,))
                row = cursor.fetchone()
                return row[0] if row else "ru"  # По умолчанию русский
        except Exception as e:
            logger.error(f"Error getting user language: {e}")
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
        """Получить подробную статистику (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            # Пользователи
            cursor = conn.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
            banned_users = cursor.fetchone()[0]

            # Запросы
            cursor = conn.execute("SELECT COUNT(*) FROM download_requests")
            total_requests = cursor.fetchone()[0]

            # Кэш
            cursor = conn.execute("SELECT COUNT(*) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            cached_files = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COALESCE(SUM(telegram_file_size), 0) FROM video_formats WHERE telegram_file_id IS NOT NULL")
            cache_size = cursor.fetchone()[0]

            return {
                "total_users": total_users,
                "banned_users": banned_users,
                "active_users": total_users - banned_users,
                "total_requests": total_requests,
                "cache_hits": 0,  # Больше не отслеживаем
                "cache_misses": total_requests,
                "cached_files": cached_files,
                "cache_size": cache_size
            }

    def get_top_users(self, limit: int = 10) -> List[dict]:
        """Получить топ пользователей по количеству запросов (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT u.user_id, u.username, u.first_name, COUNT(dr.id) as request_count
                FROM users u
                LEFT JOIN download_requests dr ON u.user_id = dr.user_id
                GROUP BY u.user_id
                ORDER BY request_count DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # === Методы для работы с очередью ===
    # Удалены: log_queue_task, update_queue_task_status, get_user_queue, get_queue_stats
    # Очередь теперь хранится в video_formats.status

    def get_queue_stats(self) -> dict:
        """Получить статистику очереди (новая схема)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT status, COUNT(*) FROM video_formats GROUP BY status")
            stats = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                "pending": stats.get("pending", 0),
                "downloading": stats.get("downloading", 0),
                "uploading": stats.get("uploading", 0),
                "completed": stats.get("completed", 0),
                "failed": stats.get("failed", 0),
                "total": sum(stats.values())
            }
