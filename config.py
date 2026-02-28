"""
Конфигурация бота.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Токен бота
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Максимальный размер файла (50MB — лимит Telegram)
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 52428800))

# Путь для загрузок
DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "/root/git/youtube-downloader-bot/downloads"))

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Таймауты
DOWNLOAD_TIMEOUT = 300  # 5 минут на загрузку
SEND_TIMEOUT = 120  # 2 минуты на отправку
