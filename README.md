# 🎬 YouTube Downloader Bot

Telegram-бот для загрузки видео из YouTube с поддержкой кэширования, выбора качества и **поиска по ключевым словам**.

**Репозиторий:** https://github.com/JeBance/youtube-downloader-bot  
**Бот:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)

---

## ✨ Возможности

- 🔍 **Поиск видео:** По ключевым словам (10 видео + 10 shorts)
- 🌐 **Выбор языка:** Русский/English через `/lang`
- 📹 **Все качества:** 144p – 4K (2160p)
- 🎵 **Только аудио:** MP3 из видео
- 💾 **Умное кэширование:** Повторные запросы мгновенно из кэша
- ✅ **Индикация кэша:** Закэшированные форматы с галочкой ✅
- 🔗 **Ссылка на источник:** В каждом видео
- 🔄 **Очередь загрузок:** Fair Queue — честное распределение
- ⚡ **Лимиты:** YouTube ~0.7 RPS, Telegram ~35 загрузок/мин
- 👑 **Админ-команды:** Статистика, очистка кэша
- 🔒 **Безопасность:** Запуск от ytrobot, systemd sandboxing
- 📦 **До 2GB:** Локальный Bot API Server

---

## 🔍 Поиск видео

Бот умеет искать видео по ключевым словам! Просто отправьте текстовый запрос:

```
Steam Deck OLED
Python уроки для начинающих
Рецепты пасты карбонара
```

**Что делает бот:**
1. Находит 10 популярных видео по запросу
2. Находит 10 актуальных Shorts
3. Добавляет всё в очередь загрузок
4. Скачивает и отправляет видео (до 720p для поиска)

**Кэширование при поиске:**
- При повторном запросе видео отправляются **мгновенно из кэша**
- Не скачивается заново то, что уже загружено

**Выбор языка:**
Используйте команду `/lang` для выбора языка поиска:
- 🇷 **Русский** — ищет русскоязычные видео
- 🇬 **English** — ищет англоязычные видео

---

## 📦 Требования

- Ubuntu 20.04+ / Debian 11+
- Python 3.9+
- 512 MB RAM, 2 GB disk
- ffmpeg (для слияния видео/аудио)

---

## 🚀 Установка

### 1. Зависимости

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git ffmpeg curl
```

### 2. Клонирование

```bash
sudo mkdir -p /root/git
cd /root/git
git clone https://github.com/JeBance/youtube-downloader-bot.git
cd youtube-downloader-bot
```

### 3. Виртуальное окружение

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Настройка

```bash
cp .env.example .env
nano .env
```

**Обязательные переменные:**

```bash
# Токен от @BotFather
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz

# Твой ID (узнать у @userinfobot)
ADMIN_ID=5610580916

# Локальный Bot API (опционально, для >50MB)
BOT_API_SERVER_URL=http://localhost:8081/bot1234567890:ABCdef...

# Лимит файла (2GB с локальным API)
MAX_FILE_SIZE=2147483648
```

### 5. Пользователь ytrobot

```bash
sudo useradd -r -s /bin/false -d /opt/ytrobot -m ytrobot
sudo chown -R ytrobot:ytrobot /root/git/youtube-downloader-bot
```

---

## ⚙️ Локальный Bot API Server (для >50MB)

### Сборка из TDLib

```bash
sudo apt install -y git cmake g++ libssl-dev zlib1g-dev gperf

cd /opt
git clone --recursive https://github.com/tdlib/telegram-bot-api.git
cd telegram-bot-api
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --target install

telegram-bot-api --version
```

### Настройка службы

```bash
sudo nano /etc/systemd/system/telegram-bot-api.service
```

```ini
[Unit]
Description=Telegram Bot API Server
After=network.target

[Service]
Type=simple
Environment=TELEGRAM_API_ID=YOUR_API_ID
Environment=TELEGRAM_API_HASH=YOUR_API_HASH
ExecStart=/usr/local/bin/telegram-bot-api --local --http-port=8081
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

**API_ID и API_HASH:** https://my.telegram.org/auth

```bash
sudo systemctl daemon-reload
sudo systemctl enable telegram-bot-api
sudo systemctl start telegram-bot-api
```

---

## 💬 Команды

### Пользовательские

| Команда | Описание |
|---------|----------|
| `/start` | Приветствие |
| `/help` | Справка |
| `/ping` | Проверка связи |
| `/status` | Статистика кэша |
| `/queue` | Статус вашей очереди |
| `/lang` | Выбор языка поиска |

### Администратора

| Команда | Описание |
|---------|----------|
| `/admin` | Админ-панель |
| `/clear` | Очистка кэша |
| `/qstat` | Статистика очереди |

---

## 🔧 ytrobot менеджер

```bash
# Статус
ytrobot status

# Запуск
ytrobot start

# Перезапуск
ytrobot restart

# Логи
ytrobot logs

# Автозапуск
ytrobot enable
```

### Автозапуск

```bash
ytrobot enable
sudo systemctl start ytrobot
sudo systemctl status ytrobot
```

---

## 📁 Модульная структура (v2.0)

```
youtube-downloader-bot/
├── bot.py                  # Точка входа (124 строки)
├── config.py               # Конфигурация
├── database.py             # База данных SQLite (~900 строк)
├── queue_manager.py        # Fair Queue Manager (~500 строк)
├── handlers/               # Обработчики событий
│   ├── __init__.py
│   ├── commands.py         # Команды бота (/start, /help, etc.)
│   ├── video.py            # Ссылки и выбор качества
│   ├── search.py           # Поиск видео по запросу
│   └── queue.py            # Обработка очереди загрузок
├── services/               # Бизнес-логика
│   ├── __init__.py
│   ├── video_service.py    # Работа с YouTube (yt-dlp)
│   └── download_service.py # Отправка в Telegram
├── ytrobot                 # Менеджер запуска
├── requirements.txt        # Зависимости
├── .env.example            # Шаблон переменных
├── README.md               # Документация
├── PROJECT_DOCS.md         # Полная документация
└── systemd/                # Службы systemd
```

---

## 🔄 Очередь загрузок (Fair Queue)

### Принцип работы

- **Fair Queue:** Каждый пользователь получает равный приоритет
- Если пользователь A добавил 100 видео, а пользователь B — 10, они обрабатываются равномерно
- Глобальные лимиты соблюдаются для всех пользователей в сумме

### Лимиты

| Сервис | Лимит | Настройка |
|--------|-------|-----------|
| YouTube | ~0.7 запроса/сек | `QUEUE_YOUTUBE_RPS` |
| Telegram | ~35 загрузок/мин | `QUEUE_TELEGRAM_UPLOADS_PER_MIN` |
| Одновременные загрузки | 2 | `QUEUE_MAX_CONCURRENT` |
| Очередь на пользователя | 100 задач | `QUEUE_MAX_PER_USER` |

### Настройка

В `.env`:

```bash
QUEUE_YOUTUBE_RPS=0.7
QUEUE_TELEGRAM_UPLOADS_PER_MIN=35
QUEUE_MAX_CONCURRENT=2
QUEUE_MAX_PER_USER=100
```

### Мониторинг

```bash
# Статус вашей очереди
/queue

# Подробная статистика очереди (админ)
/qstat
```

---

## 🐛 Troubleshooting

**Бот не запускается:**
```bash
ytrobot logs
journalctl -u ytrobot -f
```

**Ошибка токена:**
```bash
cat .env | grep BOT_TOKEN
curl "https://api.telegram.org/bot<TOKEN>/getMe"
```

**Файлы >50MB:**
```bash
systemctl status telegram-bot-api
cat .env | grep MAX_FILE_SIZE
```

**Видео не загружается:**
```bash
ytrobot logs | grep ERROR
```

---

## 🔒 Безопасность

- ✅ Запуск от `ytrobot` (не root)
- ✅ systemd sandboxing
- ✅ `.env` в `.gitignore`
- ✅ Админ-команды защищены
- ✅ Модульная архитектура для удобства поддержки

---

## 📈 Changelog

### v2.0 (2026-04-02)
- ✅ Модульная архитектура (handlers/, services/)
- ✅ Fair Queue — честная очередь загрузок
- ✅ Умное кэширование с индикацией ✅
- ✅ Поиск видео по ключевым словам
- ✅ Выбор языка поиска (RU/EN)
- ✅ Экранирование Markdown в сообщениях
- ✅ Обновление метаданных при загрузке
- ✅ Отправка из кэша без повторной загрузки

### v1.0 (предыдущая)
- Монолитный bot.py (2000+ строк)
- Прямая загрузка без очереди
- Базовое кэширование

---

## 📄 Лицензия

MIT License

---

**Автор:** JeBance  
**GitHub:** https://github.com/JeBance/youtube-downloader-bot  
**Telegram:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)
