import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Загружаем датасет о диабете
        X, y = load_diabetes(return_X_y=True)

        # Формируем случайный индекс строки
        random_row = np.random.randint(0, len(X))

        # Генерируем уникальный идентификатор
        message_id = datetime.timestamp(datetime.now())

        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters("rabbitmq"))
        channel = connection.channel()

        # Создаём очередь y_true
        channel.queue_declare(queue="y_true")
        # Создаём очередь features
        channel.queue_declare(queue="features")

        # Формируем сообщения с ID
        message_y_true = {"id": message_id, "body": float(y[random_row])}
        message_features = {"id": message_id, "body": list(X[random_row])}

        # Публикуем сообщение в очередь y_true
        channel.basic_publish(
            exchange="", routing_key="y_true", body=json.dumps(message_y_true)
        )
        print("Сообщение с правильным ответом отправлено в очередь")

        # Публикуем сообщение в очередь features
        channel.basic_publish(
            exchange="", routing_key="features", body=json.dumps(
                message_features)
        )
        print("Сообщение с вектором признаков отправлено в очередь")

        # Закрываем подключение
        connection.close()

        # Добавляем задержку в 5 секунд
        time.sleep(5)

    except Exception as e:
        print(f"Не удалось подключиться к очереди: {e}")
        time.sleep(5)