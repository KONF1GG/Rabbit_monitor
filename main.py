import os
import requests
import time
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv('.env')

# Конфигурация
RABBITMQ_API = os.getenv("RABBIT_API")
RABBITMQ_USER = os.getenv("RABBIT_USER")
RABBITMQ_PASSWORD = os.getenv("RABBIT_PASSWORD")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_ID_LEO")

THRESHOLD = 100  # Порог сообщений в очереди
CHECK_INTERVAL = 300  # Интервал проверки (в секундах)
ALERT_THRESHOLD = 3  # Количество подряд превышений для уведомления


def send_telegram_message(chat_id, message, bot_token):
    """Отправляет уведомление в Telegram."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        print(f"[ERROR] Failed to send message: {response.text}")
    return response.json()


def get_queue_message_count():
    """Получает количество сообщений в очереди из RabbitMQ."""
    try:
        response = requests.get(RABBITMQ_API, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD))
        if response.status_code == 200:
            data = response.json()
            return data.get("messages", 0)
        else:
            raise Exception(f"Failed to fetch RabbitMQ data: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[ERROR] {e}")
        return 0


def monitor_queue():
    """Мониторит очередь и отправляет уведомления, если сообщений слишком много."""
    consecutive_alerts = 0

    while True:
        try:
            message_count = get_queue_message_count()
            print(f"[INFO] Сообщений в очереди: {message_count}")

            if message_count > THRESHOLD:
                consecutive_alerts += 1
                print(f"[WARNING] Превышение порога ({consecutive_alerts}/3)")
            else:
                consecutive_alerts = 0  # Сбрасываем счетчик, если меньше порога

            if consecutive_alerts >= ALERT_THRESHOLD:
                send_telegram_message(
                    TELEGRAM_CHAT_ID,
                    f"⚠️ В очереди `to_redis` {message_count} сообщений",
                    TELEGRAM_BOT_TOKEN
                )
                time.sleep(1800)
                consecutive_alerts = 0  # Сброс после отправки уведомления

            time.sleep(CHECK_INTERVAL)  # Ждем перед следующей проверкой
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(CHECK_INTERVAL)  # Подождать перед следующей попыткой


if __name__ == "__main__":
    monitor_queue()
