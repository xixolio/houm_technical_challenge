import sys
sys.path.append('../..')
import pytest
import pandas as pd
import numpy as np
from src.utils.api_queries import query_weather_api_multiple_times


def query_weather_api_daily(*args, **kwargs):
    dates = ['2022-01-10']
    names = ['70.3,-30.1']
    temps = [20]
    conditions = ['Rain']
    fake_column = ['fake1']

    data = {'datetime': dates,
            'name': names,
            'temp': temps,
            'conditions': conditions,
            'fake_column': fake_column}

    return_values = pd.DataFrame(data)
    return return_values


def query_weather_api_hourly(*args, **kwargs):
    dates = ['2022-01-10']
    hours = ['20:00:00']
    names = ['70.3,-30.1']
    temps = [20]
    conditions = ['Rain']
    fake_column = ['fake1']

    data = {'datetime': dates,
            'name': names,
            'temp': temps,
            'conditions': conditions,
            'fake_column': fake_column}

    return_values = pd.DataFrame(data)
    return return_values


def test_query_weather_api_multiple_times_daily(mocker,):
    query_weather_api_mock = mocker.patch('src.utils.api_queries.query_weather_api',
                                          side_effect=query_weather_api_daily)
    locations = ['Valdivia', 'Puerto Montt']
    dates = ['2022-01-10', '2022-01-11']
    response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates)
    columns = ['date', 'latitude', 'longitude', 'temperature', 'conditions']
    assert len(response_df) == 2 and all([column in response_df.columns for column in columns])


def test_query_weather_api_multiple_times_hourly(mocker,):
    query_weather_api_mock = mocker.patch('src.utils.api_queries.query_weather_api',
                                          side_effect=query_weather_api_hourly)
    locations = ['Valdivia', 'Valdivia', 'Puerto Montt']
    dates = ['2022-01-10', '2022-01-10', '2022-01-11']
    response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates, include='hours')
    columns = ['date', 'latitude', 'longitude', 'temperature', 'conditions']
    assert len(response_df) == 2 and all([column in response_df.columns for column in columns])


"""
def test_query_weather_api_multiple_times_hourly_real_data(mocker,):
    locations = ['4.870956,-74.058040']
    dates = ['2022-01-10']
    response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates, include='hours', verbose=True)
    columns = ['date', 'hour', 'latitude', 'longitude', 'temp', 'conditions']
    assert len(response_df) == 24 and all([column in response_df.columns for column in columns])
"""


def test_query_weather_api_multiple_times_different_lengths(mocker,):
    query_weather_api_mock = mocker.patch('src.utils.api_queries.query_weather_api',
                                          side_effect=query_weather_api_daily)
    locations = ['Valdivia', 'Puerto Montt']
    dates = ['2022-01-10']

    with pytest.raises(Exception):
        response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates)



