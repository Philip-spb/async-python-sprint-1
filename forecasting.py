from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from utils import CITIES
from tasks import (DataFetchingTask, DataAggregationTask, DataCalculationTask, CITY_DATA,
                   DataAnalyzingTask)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    with ThreadPoolExecutor() as pool:
        pool.map(DataFetchingTask().run, CITIES)

    with ProcessPoolExecutor() as pool:
        data = pool.map(DataCalculationTask().run, CITY_DATA)

    # Раскрываем все вложенные кортежи
    data = [item for sublist in data for item in sublist]

    DataAggregationTask(list(data)).run()
    DataAnalyzingTask().perform()


if __name__ == "__main__":
    forecast_weather()
