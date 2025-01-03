import pika
import json
import pandas as pd

# Инициализируем DataFrame для хранения сообщений
messages = pd.DataFrame(columns=["id", "y_true", "y_pred"])

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq"))
    channel = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue="y_true")
    channel.queue_declare(queue="y_pred")

    # Инициализируем CSV файл с заголовками
    with open("./logs/metric_log.csv", "w") as f:
        f.write("id,y_true,y_pred,absolute_error\n")

    def process_complete_row(row):
        # Вычисляем абсолютную ошибку
        absolute_error = abs(row["y_true"] - row["y_pred"])

        # Записываем в CSV
        with open("./logs/metric_log.csv", "a") as f:
            f.write(f"{row['id']},{row['y_true']},{row['y_pred']},{absolute_error}\n")

    def callback_y_true(ch, method, properties, body):
        global messages
        message = json.loads(body)
        message_id = message["id"]
        y_true = message["body"]

        # Добавляем в DataFrame
        messages.loc[len(messages)] = {
            "id": message_id,
            "y_true": y_true,
            "y_pred": None,
        }

        # Проверяем, есть ли соответствующее предсказание
        matching_pred = messages[
            (messages["id"] == message_id) & (messages["y_pred"].notna())
        ]

        if not matching_pred.empty:
            complete_row = messages[messages["id"] == message_id].iloc[0]
            process_complete_row(complete_row)
            # Удаляем обработанную строку
            messages = messages[messages["id"] != message_id]

    def callback_y_pred(ch, method, properties, body):
        global messages
        message = json.loads(body)
        message_id = message["id"]
        y_pred = message["body"]

        # Найти существующую запись или создать новую
        existing_row = messages[messages["id"] == message_id]
        if len(existing_row) > 0:
            # Обновить существующую запись
            messages.loc[existing_row.index[0], "y_pred"] = y_pred
        else:
            # Создать новую запись
            messages.loc[len(messages)] = {"id": message_id, "y_true": None, "y_pred": y_pred}

        # Проверяем, есть ли соответствующее истинное значение
        matching_true = messages[
            (messages["id"] == message_id) & (messages["y_true"].notna())
        ]

        if not matching_true.empty:
            complete_row = messages[messages["id"] == message_id].iloc[0]
            process_complete_row(complete_row)
            # Удаляем обработанную строку
            messages = messages[messages["id"] != message_id]

    # Устанавливаем обработчики сообщений
    channel.basic_consume(
        queue="y_true", on_message_callback=callback_y_true, auto_ack=True
    )

    channel.basic_consume(
        queue="y_pred", on_message_callback=callback_y_pred, auto_ack=True
    )

    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()

except Exception as e:
    print(f"Не удалось подключиться к очереди: {e}")
