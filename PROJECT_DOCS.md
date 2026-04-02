# 📚 YouTube Downloader Bot — Полная документация

**Дата обновления:** 2026-04-02  
**Версия:** 2.0 (модульная архитектура)  
**Бот:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)

---

## 🏗 Архитектура проекта

```
youtube-downloader-bot/
├── bot.py                      # Точка входа (124 строки)
├── config.py                   # Конфигурация и переменные окружения
├── database.py                 # Работа с SQLite БД (~900 строк)
├── queue_manager.py            # Менеджер очереди загрузок (~500 строк)
├── requirements.txt            # Зависимости Python
│
├── handlers/                   # Обработчики событий (aiogram routers)
│   ├── __init__.py             # Экспорт роутеров
│   ├── commands.py             # Команды бота (/start, /help, etc.)
│   ├── video.py                # Ссылки и выбор качества
│   ├── search.py               # Поиск видео по запросу
│   └── queue.py                # Обработка очереди загрузок
│
└── services/                   # Бизнес-логика
    ├── __init__.py             # Экспорт сервисов
    ├── video_service.py        # Работа с YouTube (yt-dlp)
    └── download_service.py     # Отправка видео в Telegram
```

---

## 🔄 Поток данных

### 1. Пользователь отправляет ссылку

```
User → handle_text_message() → handle_url()
                              ↓
                        get_video_info() [YouTube API]
                              ↓
                        db.create_video() → videos table
                              ↓
                        db.create_or_get_format() → video_formats table
                              ↓
                        build_quality_keyboard() → InlineKeyboard
                              ↓
                        Показываем кнопки с качествами
```

### 2. Пользователь выбирает качество

```
User → handle_download_queued()
       ↓
Проверка кэша: db.get(video_id, format_code)
       ↓
Если есть в кэше → send_video_from_file_id() ✅
       ↓
Если нет в кэше → DownloadTask → queue_mgr.add_task()
                              ↓
                        FairQueueManager._worker_loop()
                              ↓
                        process_download_task()
                              ↓
                        download_video() [yt-dlp]
                              ↓
                        Отправка в Telegram
                              ↓
                        db.set() → сохраняем file_id
```

### 3. Пользователь отправляет поисковый запрос

```
User → handle_text_message() → process_search_results()
                               ↓
                         Для каждого видео (до 10):
                               ↓
                         db.create_video()
                               ↓
                         get_video_info()
                               ↓
                         db.create_or_get_format()
                               ↓
                         Проверка кэша: db.get_format()
                               ↓
                         Если есть в кэше → send_video() ✅
                         Если нет → DownloadTask → queue_mgr.add_task()
```

### 4. Менеджер очереди загружает

```
FairQueueManager (Fair Queue)
├── RateLimiter (YouTube RPS)
├── RateLimiter (Telegram uploads/min)
└── Semaphore (max concurrent downloads)

process_download_task(task, db):
  1. download_video(url, format_code)
  2. Отправка через Bot API Server
  3. db.set() → сохраняем telegram_file_id
  4. db.update_video_metadata() → обновляем title/uploader
  5. db.log_request() → логируем запрос
  6. db.update_queue_task_status() → статус "completed"
```

---

## 📊 База данных (SQLite)

### Таблица `videos`
| Колонка | Тип | Описание |
|---------|-----|----------|
| id | INTEGER | Внутренний ID (PRIMARY KEY) |
| source_url | TEXT | Ссылка на YouTube |
| youtube_video_id | TEXT | YouTube video ID |
| title | TEXT | Название видео |
| uploader | TEXT | Автор канала |
| duration | INTEGER | Длительность в секундах |
| created_at | TIMESTAMP | Дата создания |

### Таблица `video_formats`
| Колонка | Тип | Описание |
|---------|-----|----------|
| id | INTEGER | ID формата (PRIMARY KEY) |
| video_id | INTEGER | Ссылка на videos.id (FOREIGN KEY) |
| format_code | TEXT | Код формата (напр. "134+bestaudio") |
| quality_label | TEXT | Человекочитаемое качество (напр. "360p") |
| requested_by_user_id | INTEGER | ID пользователя |
| telegram_file_id | TEXT | File ID в Telegram (после загрузки) |
| telegram_file_size | INTEGER | Размер файла в байтах |
| status | TEXT | pending/downloading/completed/failed |
| created_at | TIMESTAMP | Дата создания |
| completed_at | TIMESTAMP | Дата завершения |

### Таблица `users`
| Колонка | Тип | Описание |
|---------|-----|----------|
| id | INTEGER | ID записи (PRIMARY KEY) |
| user_id | INTEGER | Telegram user ID (UNIQUE) |
| username | TEXT | Username в Telegram |
| first_name | TEXT | Имя пользователя |
| created_at | TIMESTAMP | Дата регистрации |
| last_seen | TIMESTAMP | Последняя активность |
| is_banned | INTEGER | 0/1 — забанен или нет |

### Таблица `download_requests`
| Колонка | Тип | Описание |
|---------|-----|----------|
| id | INTEGER | ID запроса (PRIMARY KEY) |
| user_id | INTEGER | Telegram user ID |
| video_format_id | INTEGER | Ссылка на video_formats.id |
| created_at | TIMESTAMP | Дата запроса |

---

## 🔑 Ключевые классы и функции

### `database.py`

#### Класс `VideoDatabase`
Основные методы:

| Метод | Описание |
|-------|----------|
| `create_video(source_url, youtube_video_id, title, uploader, duration)` | Создать/обновить видео |
| `get_video_by_internal_id(video_id)` | Получить видео по ID в БД |
| `get_video_by_youtube_id(youtube_id)` | Получить видео по YouTube ID |
| `update_video_metadata(video_id, title, uploader, duration)` | Обновить метаданные |
| `create_or_get_format(video_id, format_code, quality_label, requested_by_user_id)` | Создать формат |
| `get_format(video_id, format_code)` | Получить формат |
| `get(video_id, format_code)` | Получить файл из кэша |
| `set(video_id, format_code, telegram_file_id, ...)` | Сохранить файл в кэш |
| `get_all_formats_for_video(video_id)` | Получить все форматы видео |
| `update_format_status(format_id, status)` | Обновить статус формата |
| `add_user(user_id, username, first_name)` | Добавить пользователя |
| `is_banned(user_id)` | Проверка бана |
| `get_user_language(user_id)` | Язык пользователя |
| `set_user_language(user_id, language)` | Установить язык |
| `log_request(user_id, video_id, format_code, from_cache)` | Логирование запроса |
| `update_queue_task_status(user_id, video_id, format_code, status)` | Статус задачи |
| `clear()` | Очистка кэша |
| `get_stats()` | Статистика |
| `get_queue_stats()` | Статистика очереди |

### `queue_manager.py`

#### Класс `FairQueueManager`
Управляет очередью загрузок с честным распределением между пользователями.

**Параметры инициализации:**
- `youtube_rps` — лимит запросов к YouTube в секунду (по умолчанию 0.7)
- `telegram_uploads_per_min` — лимит загрузок в Telegram в минуту (по умолчанию 35)
- `max_concurrent_downloads` — макс. одновременных загрузок (по умолчанию 2)
- `max_queue_per_user` — макс. задач в очереди на пользователя (по умолчанию 100)

**Основные методы:**
- `start(process_callback)` — запуск менеджера
- `stop()` — остановка
- `add_task(task)` — добавление задачи
- `cancel_task(user_id, task_id)` — отмена задачи
- `get_user_queue_status(user_id)` — статус очереди пользователя
- `get_stats()` — статистика менеджера

#### Класс `DownloadTask`
Задача на загрузку.

**Поля:**
- `task_id` — уникальный ID задачи
- `user_id` — ID пользователя
- `username` — имя пользователя
- `video_url` — ссылка на YouTube
- `video_id` — ID видео в БД
- `format_code` — код формата
- `quality_label` — описание качества
- `status` — статус (TaskStatus enum)
- `callback_query` — оригинальный callback для ответа

### `handlers/video.py`

#### `handle_text_message(message, db, queue_mgr)`
Обрабатывает текстовые сообщения:
- Если ссылка → `handle_url()`
- Если текст → поиск видео

#### `handle_url(message, db)`
Обрабатывает ссылки YouTube:
1. Получает информацию о видео
2. Создаёт запись в БД
3. Показывает кнопки с качествами

#### `handle_download_queued(callback, db, queue_mgr)`
Обрабатывает выбор качества:
1. Проверяет кэш (`db.get()`)
2. Если есть → отправляет из кэша ✅
3. Если нет → создаёт задачу в очереди

### `handlers/search.py`

#### `search_youtube_videos(query, limit, language)`
Поиск обычных видео (10 шт).

#### `search_youtube_shorts(query, limit, language)`
Поиск Shorts (10 шт).

#### `process_search_results(user_id, chat_id, videos, shorts, query, db, bot, queue_mgr)`
Добавляет найденные видео в очередь БД:
1. Создаёт видео в БД
2. Проверяет кэш
3. Если есть → отправляет сразу ✅
4. Если нет → добавляет в очередь

### `handlers/queue.py`

#### `process_download_task(task, db)`
Обрабатывает задачу из очереди:
1. Скачивает видео через `download_video()`
2. Отправляет в Telegram
3. Сохраняет `file_id` в БД
4. Обновляет метаданные
5. Логирует результат

### `services/video_service.py`

#### `get_video_info(url)`
Получает информацию о видео без загрузки.

#### `get_available_formats(formats, max_size_mb, max_height)`
Извлекает доступные форматы из информации о видео.

#### `download_video(url, format_code)`
Скачивает видео с YouTube через yt-dlp.

#### `search_youtube_videos(query, limit, language)`
Поиск видео по запросу.

#### `search_youtube_shorts(query, limit, language)`
Поиск Shorts по запросу.

### `services/download_service.py`

#### `send_video_from_file_id(bot, chat_id, file_id, title, uploader, duration, quality_desc, source_url)`
Отправляет видео по `file_id` из кэша.

#### `download_and_send_video(bot, chat_id, url, format_code, quality_desc, title, uploader, duration, db, video_db_id)`
Скачивает и отправляет видео.

---

## ⚙️ Конфигурация

### Переменные окружения (.env)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `BOT_TOKEN` | Токен от @BotFather | `1234567890:ABCdef...` |
| `ADMIN_ID` | Telegram ID администратора | `5610580916` |
| `BOT_API_SERVER_URL` | URL локального Bot API Server | `http://localhost:8081/bot...` |
| `MAX_FILE_SIZE` | Макс. размер файла (байты) | `2147483648` (2GB) |
| `DOWNLOAD_PATH` | Путь для загрузок | `/root/git/.../downloads` |
| `CACHE_DB_PATH` | Путь к БД | `/root/git/.../cache.db` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |
| `QUEUE_YOUTUBE_RPS` | Лимит запросов к YouTube | `0.7` |
| `QUEUE_TELEGRAM_UPLOADS_PER_MIN` | Лимит загрузок в Telegram | `35` |
| `QUEUE_MAX_CONCURRENT` | Макс. одновременных загрузок | `2` |
| `QUEUE_MAX_PER_USER` | Макс. задач на пользователя | `100` |

---

## 🎯 Команды бота

### Пользовательские
| Команда | Описание |
|---------|----------|
| `/start` | Запустить бота |
| `/help` | Справка |
| `/ping` | Проверка связи |
| `/status` | Статус кэша |
| `/queue` | Статус очереди загрузок |
| `/lang` | Выбор языка поиска |

### Администратора
| Команда | Описание |
|---------|----------|
| `/admin` | Админ-панель |
| `/clear` | Очистка кэша |
| `/qstat` | Статистика очереди |

---

## 🔧 Middleware

### `globals_middleware` (bot.py)
Добавляет `db` и `queue_mgr` в контекст всех хендлеров:

```python
@dp.update.middleware()
async def globals_middleware(handler, event, data):
    """Добавляет db и queue_mgr в контекст всех хендлеров."""
    data["db"] = db
    data["queue_mgr"] = queue_mgr
    return await handler(event, data)
```

Теперь в хендлерах можно использовать:
```python
@router.message(Command("start"))
async def cmd_start(message: Message, db: VideoDatabase, queue_mgr: FairQueueManager):
    db.add_user(...)
```

---

## 📈 Статистика и мониторинг

### Логи
- Путь: `journalctl -u ytrobot -f`
- Уровень: настраивается через `LOG_LEVEL`

### Статус бота
```bash
systemctl status ytrobot
ytrobot logs
ytrobot status
```

### БД
```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('cache.db')
cursor = conn.execute('SELECT COUNT(*) FROM videos')
print('Всего видео:', cursor.fetchone()[0])
"
```

---

## 🐛 Отладка

### Частые проблемы

#### 1. "Ссылка устарела"
**Причина:** `video_db_id` — это внутренний ID, а не YouTube ID.
**Решение:** Использовать `db.get_video_by_internal_id(video_db_id)`.

#### 2. Видео не сохраняется в кэш
**Причина:** `process_download_task()` не получает `db`.
**Решение:** Использовать wrapper в `bot.py`:
```python
async def process_task_wrapper(task):
    await process_download_task(task, db)
await queue_mgr.start(process_task_wrapper)
```

#### 3. Ошибка Markdown (`can't parse entities`)
**Причина:** Символы `_`, `*`, `` ` `` в названии видео.
**Решение:** Экранировать перед отправкой:
```python
escaped_title = title.replace('_', '\\_').replace('*', '\\*')
```

#### 4. Длительность N/A
**Причина:** `duration` хранится в таблице `videos`, а не в `video_formats`.
**Решение:** Получать из БД:
```python
video_info = db.get_video_by_internal_id(video_db_id)
duration = video_info.get('duration', 0)
```

---

## 📝 Changelog

### v2.0 (2026-04-02)
- ✅ Модульная архитектура (handlers/, services/)
- ✅ Fair Queue — честная очередь загрузок
- ✅ Умное кэширование с индикацией ✅
- ✅ Поиск видео по ключевым словам
- ✅ Выбор языка поиска (RU/EN)
- ✅ Экранирование Markdown в сообщениях
- ✅ Обновление метаданных при загрузке
- ✅ Отправка из кэша без повторной загрузки
- ✅ Исправление длительности N/A

### v1.0 (предыдущая)
- Монолитный bot.py (2000+ строк)
- Кэши в памяти
- Прямая загрузка без очереди

---

## 🚀 Быстрый старт

```bash
# Установка зависимостей
pip install -r requirements.txt

# Копирование .env
cp .env.example .env
nano .env  # Редактирование

# Запуск
python3 bot.py

# Или через systemd
systemctl start ytrobot
systemctl enable ytrobot  # Автозапуск
```

---

## 🔗 Ссылки

**Репозиторий:** https://github.com/JeBance/youtube-downloader-bot  
**Бот:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)  
**Автор:** JeBance (@JeBance)

---

**Лицензия:** MIT License
