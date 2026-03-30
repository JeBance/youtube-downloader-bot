# 🎬 YouTube Downloader Bot

Telegram-бот для загрузки видео из YouTube с поддержкой кэширования и выбора качества.

**Репозиторий:** https://github.com/JeBance/youtube-downloader-bot  
**Бот:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)

---

## ✨ Возможности

- 📹 **Все качества:** 144p – 4K (2160p)
- 🎵 **Только аудио:** MP3 из видео
- 💾 **Кэширование:** Повторные запросы мгновенно
- ✅ **Умные кнопки:** Закэшированные форматы с галочкой
- 🔗 **Ссылка на источник:** В каждом видео
- 🔄 **Очередь загрузок:** Честное распределение между пользователями
- ⚡ **Лимиты:** YouTube ~1 RPS, Telegram ~15 загрузок/мин
- 👑 **Админ-команды:** Статистика, рассылка, бан
- 🔒 **Безопасность:** Запуск от ytrobot, systemd sandboxing
- 📦 **До 2GB:** Локальный Bot API Server

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
| `/ping` | Проверка |
| `/status` | Статистика кэша |
| `/queue` | Статус вашей очереди загрузок |

### Администратора

| Команда | Описание |
|---------|----------|
| `/admin` | Админ-панель |
| `/clear` | Очистка кэша |
| `/stats` | Подробная статистика |
| `/qstat` | Статистика очереди |
| `/users` | Список пользователей |
| `/ban` | Забанить |
| `/unban` | Разбанить |
| `/broadcast` | Рассылка |

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

## 📁 Структура

```
youtube-downloader-bot/
├── bot.py                  # Код бота
├── config.py               # Конфигурация
├── database.py             # Кэш SQLite
├── queue_manager.py        # Менеджер очереди загрузок
├── ytrobot                 # Менеджер запуска
├── requirements.txt        # Зависимости
├── .env.example            # Шаблон переменных
├── README.md               # Документация
├── migrate_*.py            # Миграции БД
└── systemd/                # Службы systemd
```

---

## 🔄 Очередь загрузок

Бот использует систему очереди с честным распределением (Fair Queue):

### Принцип работы

- **Fair Queue:** Каждый пользователь получает равный приоритет
- Если пользователь A добавил 100 видео, а пользователь B — 10, они обрабатываются равномерно
- Глобальные лимиты соблюдаются для всех пользователей в сумме

### Лимиты

| Сервис | Лимит | Настройка |
|--------|-------|-----------|
| YouTube | ~1 запрос/сек | `QUEUE_YOUTUBE_RPS` |
| Telegram | ~15 загрузок/мин | `QUEUE_TELEGRAM_UPLOADS_PER_MIN` |
| Одновременные загрузки | 2 | `QUEUE_MAX_CONCURRENT` |
| Очередь на пользователя | 50 задач | `QUEUE_MAX_PER_USER` |

### Настройка

В `.env`:

```bash
# Лимит запросов к YouTube в секунду (рекомендуется 1-2 для безопасности)
QUEUE_YOUTUBE_RPS=1.0

# Лимит загрузок в Telegram в минуту (рекомендуется 10-20)
QUEUE_TELEGRAM_UPLOADS_PER_MIN=15

# Максимальное количество одновременных загрузок
QUEUE_MAX_CONCURRENT=2

# Максимум задач в очереди на одного пользователя
QUEUE_MAX_PER_USER=50
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

---

## 🔒 Безопасность

- ✅ Запуск от `ytrobot` (не root)
- ✅ systemd sandboxing
- ✅ `.env` в `.gitignore`
- ✅ Админ-команды защищены

---

## 📄 Лицензия

MIT License

---

**Автор:** JeBance  
**GitHub:** https://github.com/JeBance/youtube-downloader-bot  
**Telegram:** [@JBaiYouTubeRobot](https://t.me/JBaiYouTubeRobot)
