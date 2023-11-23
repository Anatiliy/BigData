import requests
import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime

# Список городов
cities = ["Berlin", "Moscow"]

# определяем даты запроса
end_date = datetime.datetime.now().date() - datetime.timedelta(2)
start_date = end_date - datetime.timedelta(32)
print(end_date)
print(start_date)


# Создание пустого DataFrame для хранения погодных данных
weather_data = pd.DataFrame(columns=["city", "date", "temperature"])

# Запрос погодных данных для каждого города
for city in cities:
    # запрашиваем коородинаты
    response = requests.get(f'https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1&language=en&format=json').json()
    latitude = response['results'][0]['latitude']
    longitude = response['results'][0]['longitude']

    # определяем параметры API запроса 
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": f"{start_date}",
        "end_date": f"{end_date}",
        "hourly": "temperature_2m"
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    print('11111', hourly)
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature"] = hourly_temperature_2m

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    city_list = [city] * 33 * 24
    hourly_dataframe.insert (loc= 0, column='city', value=city_list)
    print(hourly_dataframe)
    weather_data = weather_data._append(hourly_dataframe, ignore_index=True)

print(weather_data)


# График изменения температуры в разных городах
# plt.figure(figsize=(10, 6))
# for city in cities:
#     city_data = weather_data[weather_data["city"] == city]
#     plt.plot(city_data["date"], city_data["temperature"], label=city)
#     plt.xlabel("Date")
#     plt.ylabel("Temperature (°C)")
#     plt.title("Temperature Change in Different Cities")
#     plt.legend()
#     plt.show()

# График распределения температуры
# plt.figure(figsize=(8, 6))
# plt.hist(weather_data["temperature"], bins=20)
# plt.xlabel("Temperature (°C)")
# plt.ylabel("Frequency")
# plt.title("Temperature Distribution")
# plt.show()

# Сохранение данных в HDFS
hdfs_path = "localhost:9870//hdfs/weather_data.csv"
weather_data.to_csv(hdfs_path, index=False)

# Выгрузка данных из HDFS на локальный компьютер
local_path = "/path/to/local/weather_data.csv"
subprocess.run(["hdfs", "dfs", "-get", hdfs_path, local_path])