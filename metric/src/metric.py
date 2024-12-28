import pika
import json
from pathlib import Path

# Путь к файлу для логирования
log_dir = Path("./logs")
log_file = log_dir / "metric_log.csv"

# Создаём папку для логов, если её не существует
log_dir.mkdir(parents=True, exist_ok=True)

# Инициализируем файл с именами столбцов, если он не существует
if not log_file.exists():
    with open(log_file, "w") as f:
        f.write("id,y_true,y_pred,absolute_error\n")

# Словарь для временного хранения сообщений
message_store = {}

def process_message(message_id, key, value):
    """Обрабатывает сообщение, добавляя его в store и записывая в лог при наличии полного набора."""
    # Если идентификатор уже есть, обновляем данные
    if message_id in message_store:
        message_store[message_id][key] = value
        # Если есть и y_true, и y_pred, вычисляем ошибку
        if "y_true" in message_store[message_id] and "y_pred" in message_store[message_id]:
            y_true = message_store[message_id]["y_true"]
            y_pred = message_store[message_id]["y_pred"]
            absolute_error = abs(y_true - y_pred)
            # Логируем метрику
            with open(log_file, "a") as f:
                f.write(f"{message_id},{y_true},{y_pred},{absolute_error}\n")
            print(f"Записано: id={message_id}, y_true={y_true}, y_pred={y_pred}, absolute_error={absolute_error}")
            # Удаляем записанный идентификатор из store
            del message_store[message_id]
    else:
        # Создаём новую запись для message_id
        message_store[message_id] = {key: value}

try:
    # Создаём подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq", port=5672))
    channel = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue="y_true")
    channel.queue_declare(queue="y_pred")

    def callback(ch, method, properties, body):
        """Обрабатывает сообщение из очереди."""
        message = json.loads(body)
        message_id = message["id"]
        value = message["body"]
        if method.routing_key == "y_true":
            process_message(message_id, "y_true", value)
        elif method.routing_key == "y_pred":
            process_message(message_id, "y_pred", value)

    # Подписываемся на очереди
    channel.basic_consume(queue="y_true", on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue="y_pred", on_message_callback=callback, auto_ack=True)

    # Запускаем режим ожидания сообщений
    print("...Ожидание сообщений, для выхода нажмите CTRL+C")
    channel.start_consuming()
except Exception as e:
    print(f"Не удалось подключиться к очереди: {e}")
