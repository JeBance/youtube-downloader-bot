#!/usr/bin/env python3
"""
Миграция: добавление таблицы queue_stats для отслеживания очереди загрузок.
Запускается один раз при обновлении.
"""
import sqlite3
from pathlib import Path

CACHE_DB_PATH = Path("/root/git/youtube-downloader-bot/cache.db")


def migrate():
    """Добавляет таблицу queue_stats если её нет."""
    if not CACHE_DB_PATH.exists():
        print("База данных не найдена. Миграция не требуется.")
        return

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        # Проверяем наличие таблицы
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='queue_stats'
        """)
        if cursor.fetchone():
            print("Таблица queue_stats уже существует.")
            return

        # Создаём таблицу
        conn.execute("""
            CREATE TABLE queue_stats (
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
        
        # Создаём индексы
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_stats_user
            ON queue_stats(user_id)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_queue_stats_status
            ON queue_stats(status)
        """)
        
        conn.commit()
        print("✅ Таблица queue_stats успешно создана.")


if __name__ == "__main__":
    migrate()
