import pandas as pd
import matplotlib.pyplot as plt
import time

def plot_error_distribution():
    while True:
        try:
            # Чтение данных
            df = pd.read_csv("logs/metric_log.csv")
            df = df.dropna()

            if len(df) > 0:
                plt.figure(figsize=(10, 6))

                # Построение гистограммы
                n, bins, patches = plt.hist(df["absolute_error"],
                                          bins=6,  # 6 столбцов
                                          range=(0, 140),  # диапазон от 0 до 140
                                          color='moccasin',  # желтый цвет
                                          edgecolor='black')

                # Сглаженная линия через средние точки столбцов
                bin_centers = (bins[:-1] + bins[1:]) / 2
                plt.plot(bin_centers, n, '-', color='orange', linewidth=2)

                # Настройка осей и подписей
                plt.xlabel("absolute_error")
                plt.ylabel("Count")

                # Удаление верхней и правой границы
                plt.gca().spines['top'].set_visible(False)
                plt.gca().spines['right'].set_visible(False)

                # Установка пределов осей
                plt.ylim(0, 6)
                plt.xlim(0, 140)

                # Сохранение графика
                plt.savefig("logs/error_distribution.png")
                plt.close()

            time.sleep(10)

        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(10)

if __name__ == "__main__":
    plot_error_distribution()