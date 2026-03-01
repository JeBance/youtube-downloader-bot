#!/usr/bin/env python3
"""
Миграция: добавление таблицы video_urls для хранения URL видео.
Запускается один раз при обновлении.
"""
import sqlite3
from pathlib import Path

CACHE_DB_PATH = Path("/root/git/youtube-downloader-bot/cache.db")


def migrate():
    """Добавляет таблицу video_urls если её нет."""
    if not CACHE_DB_PATH.exists():
        print("База данных не найдена. Миграция не требуется.")
        return

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        # Проверяем наличие таблицы
        cursor = conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='video_urls'
        """)
        if cursor.fetchone():
            print("Таблица video_urls уже существует.")
            return

        # Создаём таблицу
        conn.execute("""
            CREATE TABLE video_urls (
                video_id TEXT PRIMARY KEY,
                source_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Таблица video_urls успешно создана.")


if __name__ == "__main__":
    migrate()
