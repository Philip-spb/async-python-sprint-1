if __name__ == "__main__":
    from utils import YandexWeatherAPI

    CITY_NAME_FOR_TEST = "MOSCOW"

    ywAPI = YandexWeatherAPI()
    resp = ywAPI.get_forecasting(CITY_NAME_FOR_TEST)
    attr = resp.get("info")
    print(attr)
