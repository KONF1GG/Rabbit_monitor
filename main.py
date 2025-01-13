import os
import requests
import time
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv('.env')

# Конфигурация
RABBIT_API_TO_REDIS = os.getenv("RABBIT_API_TO_REDIS")
RABBITMQ_API_HTTP = os.getenv("RABBIT_API_HTTP")
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
        return 0
    return response.json()


def get_queue_to_redis_message_count():
    """Получает количество сообщений в очереди из RabbitMQ (to_redis)."""
    try:
        response = requests.get(RABBIT_API_TO_REDIS, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD))
        if response.status_code == 200:
            data = response.json()
            return data.get("messages", 0)
        else:
            raise Exception(f"Failed to fetch RabbitMQ data: {response.status_code} {response.text}")
    except Exception as e:
        send_telegram_message(
            TELEGRAM_CHAT_ID,
            f"⚠️ Не удалось получить информацию о количестве сообщений в очереди to_redis!",
            TELEGRAM_BOT_TOKEN
        )
        return 0


def get_queue_http_message_count():
    """Получает количество сообщений в очереди из RabbitMQ (http_post_queue)."""
    try:
        response = requests.get(RABBITMQ_API_HTTP, auth=(RABBITMQ_USER, RABBITMQ_PASSWORD))
        if response.status_code == 200:
            data = response.json()
            return data.get("messages", 0)
        else:
            raise Exception(f"Failed to fetch RabbitMQ data: {response.status_code} {response.text}")
    except Exception as e:
        send_telegram_message(
            TELEGRAM_CHAT_ID,
            f"⚠️ Не удалось получить информацию о количестве сообщений в очереди http_post_queue!",
            TELEGRAM_BOT_TOKEN
        )
        return 0


def monitor_queues():
    """Мониторит обе очереди и отправляет уведомления, если сообщений слишком много в любой из них."""
    consecutive_alerts_to_redis = 0
    consecutive_alerts_http = 0

    while True:
        try:
            # Получаем количество сообщений в обеих очередях
            to_redis_message_count = get_queue_to_redis_message_count()
            http_message_count = get_queue_http_message_count()

            # Проверяем очередь to_redis
            if to_redis_message_count > THRESHOLD:
                consecutive_alerts_to_redis += 1
            else:
                consecutive_alerts_to_redis = 0  # Сбрасываем счетчик, если меньше порога

            # Проверяем очередь http_post_queue
            if http_message_count > THRESHOLD:
                consecutive_alerts_http += 1
            else:
                consecutive_alerts_http = 0  # Сбрасываем счетчик, если меньше порога

            # Если превышен порог для одной из очередей
            if consecutive_alerts_to_redis >= ALERT_THRESHOLD:
                send_telegram_message(
                    TELEGRAM_CHAT_ID,
                    f"⚠️ В очереди `to_redis` {to_redis_message_count} сообщений",
                    TELEGRAM_BOT_TOKEN
                )
                consecutive_alerts_to_redis = 0  # Сброс после отправки уведомления

            if consecutive_alerts_http >= ALERT_THRESHOLD:
                send_telegram_message(
                    TELEGRAM_CHAT_ID,
                    f"⚠️ В очереди `http_post_queue` {http_message_count} сообщений",
                    TELEGRAM_BOT_TOKEN
                )
                consecutive_alerts_http = 0  # Сброс после отправки уведомления

            time.sleep(CHECK_INTERVAL)  # Ждем перед следующей проверкой
        except Exception as e:
            time.sleep(CHECK_INTERVAL)  # Подождать перед следующей попыткой


if __name__ == "__main__":
    monitor_queues()
