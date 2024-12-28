import pandas as pd
import matplotlib.pyplot as plt
from time import sleep
from pathlib import Path

# Пути к файлам
log_file = Path("./logs/metric_log.csv")
output_image = Path("./logs/error_distribution.png")

# Убедитесь, что папка logs существует
log_file.parent.mkdir(parents=True, exist_ok=True)

print("Сервис plot запущен. Ожидание обновлений...")

while True:
    try:
        if log_file.exists():
            # Читаем данные из CSV
            data = pd.read_csv(log_file)
            if not data.empty:
                # Извлекаем абсолютные ошибки
                absolute_errors = data["absolute_error"]

                # Строим гистограмму
                plt.figure(figsize=(10, 6))
                plt.hist(absolute_errors, bins=20, color='blue', edgecolor='black', alpha=0.7)
                plt.title("Распределение абсолютных ошибок", fontsize=16)
                plt.xlabel("Абсолютная ошибка", fontsize=14)
                plt.ylabel("Частота", fontsize=14)
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.tight_layout()

                # Сохраняем график
                plt.savefig(output_image)
                plt.close()
                print(f"Гистограмма обновлена: {output_image}")
        else:
            print(f"Файл {log_file} не найден. Ожидание...")
    except Exception as e:
        print(f"Ошибка при обработке: {e}")

    # Ожидание перед следующей итерацией
    sleep(5)