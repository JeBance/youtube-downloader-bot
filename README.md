# YouTube Downloader Bot

Telegram-бот для загрузки видео из YouTube.

## Возможности

- Загрузка видео по ссылке
- Выбор формата (видео + аудио или только аудио)
- Отправка файла пользователю
- Автоматическая очистка временных файлов
- Асинхронная обработка запросов

## Установка

1. Клонируйте репозиторий:
```bash
git clone git@github.com:JeBance/youtube-downloader-bot.git
cd youtube-downloader-bot
```

2. Создайте виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Настройте переменные окружения:
```bash
cp .env.example .env
# Отредактируйте .env и добавьте ваш токен бота
```

5. Установите yt-dlp (опционально, последняя версия):
```bash
pip install -U yt-dlp
```

## Запуск

```bash
python bot.py
```

## Запуск как служба (systemd)

1. Скопируйте файл службы:
```bash
sudo cp systemd/youtube-bot.service /etc/systemd/system/
```

2. Включите и запустите службу:
```bash
sudo systemctl daemon-reload
sudo systemctl enable youtube-bot
sudo systemctl start youtube-bot
```

3. Проверьте статус:
```bash
sudo systemctl status youtube-bot
```

## Команды бота

- `/start` — приветствие и инструкция
- `/help` — справка по использованию
- Отправьте ссылку на YouTube видео — бот скачает и отправит файл

## Требования

- Python 3.9+
- Telegram Bot Token (получить у @BotFather)
- Доступ к интернету
