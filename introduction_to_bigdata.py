import requests
from bs4 import BeautifulSoup
import pandas as pd
#import matplotlib.pyplot as plt
import subprocess
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime

# Список городов
cities = ["Berlin", "Moscow"]

# определяем даты запроса
end_date = datetime.datetime.now().date()
start_date = end_date - datetime.timedelta(30)
print(end_date)
print(start_date)


# Создание пустого DataFrame для хранения погодных данных
weather_data = pd.DataFrame(columns=["City", "Date", "Temperature"])

# Запрос погодных данных для каждого города
for city in cities:
    # запрашиваем коородинаты
    response = requests.get(f'https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1&language=en&format=json').json()
    latitude = response['results'][0]['latitude']
    longitude = response['results'][0]['longitude']

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
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
    print(hourly)
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature_2m"] = hourly_temperature_2m

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    print(hourly_dataframe)


    
    # Извлечение дат и температур из HTML-страницы
    # dates = []
    # temperatures = []
    
    # date_elements = soup.select(".date")
    # temperature_elements = soup.select(".t_0")
    
    # for date_element, temperature_element in zip(date_elements, temperature_elements):
    #     date = pd.to_datetime(date_element.text, format="%d.%m.%Y")
    #     temperature = float(temperature_element.text.replace("°C", "").replace(",", "."))
        
    #     dates.append(date)
    #     temperatures.append(temperature)
    
    # Добавление данных в DataFrame
#     city_weather_data = pd.DataFrame({"City": city, "Date": dates, "Temperature": temperatures})
#     weather_data = weather_data._append(city_weather_data, ignore_index=True)

# print(list(weather_data["City"]))

# График изменения температуры в разных городах
#plt.figure(figsize=(10, 6))
#for city in cities:
    #city_data = weather_data[weather_data["City"] == city]
    #plt.plot(city_data["Date"], city_data["Temperature"], label=city)
    #plt.xlabel("Date")
    #plt.ylabel("Temperature (°C)")
    #plt.title("Temperature Change in Different Cities")
    #plt.legend()
    #plt.show()

# График распределения температуры
# plt.figure(figsize=(8, 6))
# plt.hist(weather_data["Temperature"], bins=20)
# plt.xlabel("Temperature (°C)")
# plt.ylabel("Frequency")
# plt.title("Temperature Distribution")
# plt.show()

# Сохранение данных в HDFS
#hdfs_path = "/path/to/hdfs/weather_data.csv"
#weather_data.to_csv(hdfs_path, index=False)

# Выгрузка данных из HDFS на локальный компьютер
#local_path = "/path/to/local/weather_data.csv"
#subprocess.run(["hdfs", "dfs", "-get", hdfs_path, local_path])