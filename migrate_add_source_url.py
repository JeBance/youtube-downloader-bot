#!/usr/bin/env python3
"""
Миграция: добавление колонки source_url в таблицу video_cache.
Запускается один раз при обновлении.
"""
import sqlite3
from pathlib import Path

CACHE_DB_PATH = Path("/root/git/youtube-downloader-bot/cache.db")


def migrate():
    """Добавляет колонку source_url если её нет."""
    if not CACHE_DB_PATH.exists():
        print("База данных не найдена. Миграция не требуется.")
        return

    with sqlite3.connect(CACHE_DB_PATH) as conn:
        # Проверяем наличие колонки
        cursor = conn.execute("PRAGMA table_info(video_cache)")
        columns = [row[1] for row in cursor.fetchall()]

        if "source_url" in columns:
            print("Колонка source_url уже существует.")
            return

        # Добавляем колонку
        conn.execute("""
            ALTER TABLE video_cache
            ADD COLUMN source_url TEXT
        """)
        conn.commit()
        print("✅ Колонка source_url успешно добавлена.")


if __name__ == "__main__":
    migrate()
