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

# ID администратора
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# URL локального Bot API Server (если используется)
BOT_API_SERVER_URL = os.getenv("BOT_API_SERVER_URL", "")

# Максимальный размер файла (50MB — лимит Telegram для публичного API)
# При использовании локального Bot API Server можно увеличить до 2000MB
DEFAULT_MAX_FILE_SIZE = 52428800  # 50MB
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", DEFAULT_MAX_FILE_SIZE))

# Путь для загрузок
DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "/root/git/youtube-downloader-bot/downloads"))

# Путь к базе данных кэша
CACHE_DB_PATH = Path(os.getenv("CACHE_DB_PATH", "/root/git/youtube-downloader-bot/cache.db"))

# Уровень логирования
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Таймауты
DOWNLOAD_TIMEOUT = 600  # 10 минут на загрузку
SEND_TIMEOUT = 300  # 5 минут на отправку

# Настройки очереди загрузок
QUEUE_YOUTUBE_RPS = float(os.getenv("QUEUE_YOUTUBE_RPS", "1.0"))  # Запросов к YouTube в секунду
QUEUE_TELEGRAM_UPLOADS_PER_MIN = int(os.getenv("QUEUE_TELEGRAM_UPLOADS_PER_MIN", "15"))  # Загрузок в Telegram в минуту
QUEUE_MAX_CONCURRENT = int(os.getenv("QUEUE_MAX_CONCURRENT", "2"))  # Макс. одновременных загрузок
QUEUE_MAX_PER_USER = int(os.getenv("QUEUE_MAX_PER_USER", "50"))  # Макс. задач в очереди на пользователя
