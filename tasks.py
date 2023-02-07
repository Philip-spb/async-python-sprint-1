import csv
import logging
from copy import copy
from datetime import date, datetime
from dataclasses import dataclass
from itertools import takewhile
from typing import Tuple, List, Union

from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from api_client import YandexWeatherAPI
from utils import (CITIES, TIME_INTERVAL, GOOD_WEATHER, TEMPERATURE_TASK_NAME,
                   HOURS_WITHOUT_PRECIPITATION_TASK_NAME, FILE_NAME)

logging.basicConfig(
    filename='application-log.log',
    filemode='w',
    format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s'
)

logger = logging.getLogger()
CITY_DATA = []
lock = Lock()


@dataclass
class WeatherData:
    city: str
    data_type: str
    date: date
    value: Union[int, float]


class DataFetchingTask:
    """
    Получение данных через API
    """

    data = None

    def __init__(self):
        self.yw_api = YandexWeatherAPI()
        self.data = None

    def _receive_weather_forecast_data(self, city: str) -> list:
        if not self.data:
            self.data = self.yw_api.get_forecasting(city)
        try:
            forecast_data = self.data['forecasts']
        except Exception as e:
            logger.error(e)
            raise Exception(e)

        return forecast_data

    def run(self, city: str) -> None:
        data = self._receive_weather_forecast_data(city)
        CITY_DATA.append((city, data))


class DataHelperMixin:

    @staticmethod
    def get_hours_and_date(data: dict) -> Tuple[list, date]:

        try:
            hours = data['hours']
            current_date = data['date']
        except KeyError as e:
            logger.error(e)
            raise Exception(e)

        current_date = datetime.strptime(current_date, "%Y-%m-%d").date()
        return hours, current_date


class CalculateDayAverageTemperature(DataHelperMixin):
    """
    Класс с методами для вычисления средней температуры
    """

    def __init__(self, raw_data: list, city: str):
        self.raw_data = raw_data
        self.city = city

    def _get_temperature_day_value(self, data: dict):
        """
        Средняя температура за один день
        data: dict - Словарь данных по погоде за день
        """
        hours, current_date = self.get_hours_and_date(data)

        temperature_data = list()
        hours = list(filter(lambda x: x['hour'] in TIME_INTERVAL, hours))

        if not len(hours):
            return

        for hour in hours:
            temperature_data.append(hour['temp'])

        average_temperature = sum(temperature_data) / len(temperature_data)

        weather_data = WeatherData(self.city, TEMPERATURE_TASK_NAME,
                                   current_date, average_temperature)

        return weather_data

    def calculate(self):
        """
        Отправка в очередь данных по средним дневным температурам
        """
        return [self._get_temperature_day_value(day)
                for day
                in self.raw_data
                if self._get_temperature_day_value(day)]


class CalculateDayHoursWithoutPrecipitation(DataHelperMixin):
    """
    Класс с методами для вычисления количества часов без осадков
    """

    def __init__(self, raw_data: list, city: str):
        self.raw_data = raw_data
        self.city = city

    def _count_hours_without_perceptions(self, data: dict):
        hours, current_date = self.get_hours_and_date(data)

        hours_without_perception = list(
            filter(lambda x: x['hour'] in TIME_INTERVAL, hours))

        if not len(hours_without_perception):
            return

        hours_without_perception = list(
            filter(lambda x: x['condition'] in GOOD_WEATHER,
                   hours_without_perception))

        hours_without_precipitation = len(hours_without_perception)
        weather_data = WeatherData(self.city,
                                   HOURS_WITHOUT_PRECIPITATION_TASK_NAME,
                                   current_date,
                                   hours_without_precipitation)

        return weather_data

    def calculate(self):
        return [self._count_hours_without_perceptions(day)
                for day
                in self.raw_data
                if self._count_hours_without_perceptions(day)]


class DataCalculationTask:
    """
    Вычисление погодных параметров
    """

    @staticmethod
    def run(data: tuple):
        city, city_data = data
        day_temperature = CalculateDayAverageTemperature(city_data, city).calculate()
        hours_without_precipitation = (
            CalculateDayHoursWithoutPrecipitation(city_data, city).calculate()
        )
        return *day_temperature, *hours_without_precipitation


class DataAggregationTask:
    """
    Объединение вычисленных данных

    Процесс по записи полученных вычисленных данных в файл
    """

    def __init__(self, data: list):
        super().__init__()
        self.data = data
        self.header = None
        self.row = None

    def _get_all_dates(self) -> list:
        only_dates = {data.date for data in self.data}
        return sorted(only_dates)

    def _create_header_for_csv(self, dates: list) -> None:
        self.header = ['Город/день', '', *dates, 'Среднее']
        self.row = ['', ] * len(self.header)

    @staticmethod
    def _calculate_average_data(data: List[tuple]) -> float:
        """
        Рассчитываем среднее значение
        """
        values_list = [item[1] for item in data]
        return sum(values_list) / len(values_list)

    def _create_data_list(self) -> List[dict]:
        data_list = []
        for city in CITIES.keys():
            city_weather = list(
                filter(
                    lambda x: x.city == city and x.data_type == TEMPERATURE_TASK_NAME, self.data)
            )
            temperature_list = [(str(data.date), data.value) for data in
                                city_weather]

            city_hours_without_precipitation = list(filter(
                lambda x: x.city == city and x.data_type == HOURS_WITHOUT_PRECIPITATION_TASK_NAME,
                self.data))
            hours_without_precipitation = [(str(data.date), data.value)
                                           for data in city_hours_without_precipitation]
            item = {
                'city': city,
                'temperature': {
                    'date': temperature_list,
                    'average': self._calculate_average_data(temperature_list)
                },
                'days_without_precipitation': {
                    'date': hours_without_precipitation,
                    'average': self._calculate_average_data(
                        hours_without_precipitation)
                }
            }
            data_list.append(item)

        return data_list

    def _write_row_to_file(self, city_data: list, ) -> None:
        lock.acquire()

        with open(FILE_NAME, 'a') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            current_row_1 = copy(self.row)
            current_row_2 = copy(self.row)
            current_row_1[0] = city_data['city']
            current_row_1[1] = 'Температура, среднее'

            current_col = 2
            for _, value in city_data['temperature']['date']:
                current_row_1[current_col] = str(value)[0:5]
                current_col += 1

            current_row_1[-1] = str(city_data['temperature']['average'])[0:5]
            writer.writerow(current_row_1)

            current_row_2[1] = 'Без осадков, часов'

            current_col = 2
            for _, value in city_data['days_without_precipitation']['date']:
                current_row_2[current_col] = str(value)[0:5]
                current_col += 1

            current_row_2[-1] = str(
                city_data['days_without_precipitation']['average'])[0:5]
            writer.writerow(current_row_2)

        lock.release()

    def run(self):
        all_dates = self._get_all_dates()

        data = self._create_data_list()

        self._create_header_for_csv(all_dates)

        with open(FILE_NAME, 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(self.header)

        with ThreadPoolExecutor() as pool:
            pool.map(self._write_row_to_file, data)


class DataAnalyzingTask(Process):
    """
    Финальный анализ и получение результата

    Открываем файл и определяем победителя
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def _get_data_from_file(filename: str) -> List[list]:
        with open(filename, 'r') as csv_file:
            reader = csv.reader(csv_file, delimiter=",", quotechar='"')
            data_read = [[row[0], row[-1]] for row in reader][1:]
        return data_read

    @staticmethod
    def _prepare_data_for_sorting(city_data: List[list]) -> List[tuple]:
        data_for_sorting = []
        while len(city_data) >= 2:
            data_for_sorting.append((city_data[0][0], city_data[0][1], city_data[1][1]))
            city_data = city_data[2:]

        return data_for_sorting

    @staticmethod
    def _get_list_of_top_cities(data_for_sorting: List[tuple]) -> List[tuple]:
        sorted_data = sorted(data_for_sorting, key=lambda x: (x[1], x[2]), reverse=True)
        top_weather = sorted_data[0][1]
        top_days = sorted_data[0][2]
        top_cities = list(takewhile(lambda x:
                                    x[1] == top_weather and x[2] == top_days,
                                    sorted_data)
                          )
        return top_cities

    @staticmethod
    def _get_congratulation_str(city_data: List[tuple]) -> str:
        assert len(city_data) > 0

        city_names = [item[0] for item in city_data]

        if len(city_names) == 1:
            congratulation_str = f'The winner is city: {city_names[0]}'
        else:
            city_names = ', '.join(city_names)
            congratulation_str = f'Winners is cities: {city_names}'
        return congratulation_str

    def perform(self):
        data_read = self._get_data_from_file(FILE_NAME)
        data_for_sorting = self._prepare_data_for_sorting(data_read)
        top_cities = self._get_list_of_top_cities(data_for_sorting)
        congratulation_str = self._get_congratulation_str(top_cities)

        print('-' * 30)
        print(congratulation_str)
        print('-' * 30)
