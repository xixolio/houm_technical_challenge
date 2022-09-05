import os
import sys

sys.path.append('../..')
import pytest
import pandas as pd
import numpy as np
from src.utils.queries import get_data_from_db_or_api

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


def query_weather_api_daily(*args, **kwargs):

    dates = ['2022-01-10', '2022-01-11']
    #locations = ['Valdivia', 'Santiago']
    latitudes = [70.3, 70.3]
    longitudes = [30.1, 30.1]
    temps = [20, 30]
    conditions = ['Rain', 'Snow']
    fake_column = ['fake1', 'fake2']

    data = {'date': dates,
            'latitude': latitudes,
            'longitude': longitudes,
            'temp': temps,
            'condition': conditions,
            'fake_column': fake_column}

    return_values = pd.DataFrame(data)
    return return_values


def select_from_daily_weather_data(*args, **kwargs):
    dates = ['2021-01-11']
    latitudes = [0]
    longitudes = [0]
    temps = [25]
    conditions = ['Rain']

    data = {'date': dates,
            'latitude': latitudes,
            'longitude': longitudes,
            'temp': temps,
            'condition': conditions}

    return_values = pd.DataFrame(data)
    return return_values


def select_from_hourly_weather_data(*args, **kwargs):
    dates = ['2021-01-11']
    hours = ['14:00']
    locations = ['Valdivia']
    latitudes = [0]
    longitudes = [0]
    temps = [25]
    conditions = ['Rain']

    data = {'date': dates,
            'hour': hours,
            'latitude': latitudes,
            'longitude': longitudes,
            'temp': temps,
            'condition': conditions}

    return_values = pd.DataFrame(data)
    return return_values


def query_weather_api_hourly(*args, **kwargs):
    dates = ['2022-01-10', '2022-01-11']
    hours = ['00:00:00', '21:00:00']
    latitudes = [70.3, 70.3]
    longitudes = [30.1, 30.1]
    temps = [20, 30]
    conditions = ['Rain', 'Snow']

    data = {'date': dates,
            'hour': hours,
            'latitude': latitudes,
            'longitude': longitudes,
            'temp': temps,
            'condition': conditions}

    return_values = pd.DataFrame(data)
    return return_values


def test_get_data_from_db_or_api_daily_some_in_db(mocker,):
    dates = ['2022-01-10', '2022-01-11', '2021-01-11']
    #locations = ['Valdivia', 'Santiago', 'Valdivia']
    latitudes = [70.3, 70.3, 0]
    longitudes = [30.1, 30.1, 0]
    columns = ['date', 'latitude', 'longitude', 'temp', 'condition']

    select_from_daily_weather_data_mock = mocker.patch('src.utils.queries.select_from_daily_weather_data',
                                                       side_effect=select_from_daily_weather_data)

    query_weather_api_mock = mocker.patch('src.utils.queries.query_weather_api_multiple_times',
                                          side_effect=query_weather_api_daily)

    df = get_data_from_db_or_api(None, dates, latitudes, longitudes, columns, include='days')

    assert len(df) == 3

