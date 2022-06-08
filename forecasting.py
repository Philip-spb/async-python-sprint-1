# import logging
# import threading
# import subprocess
# import multiprocessing

from utils import YandexWeatherAPI, CITIES
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    # city_name = "MOSCOW"
    # ywAPI = YandexWeatherAPI()
    # resp = ywAPI.get_forecasting(city_name)
    pass


if __name__ == "__main__":
    forecast_weather()
