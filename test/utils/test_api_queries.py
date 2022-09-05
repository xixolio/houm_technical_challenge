import sys
sys.path.append('../..')
import pytest
import pandas as pd
import numpy as np
from src.utils.api_queries import query_weather_api_multiple_times


def query_weather_api_random(*args, **kwargs):
    return_values = pd.DataFrame(np.random.normal(0, 1, size=(1, 10)))
    return return_values


def test_query_weather_api_multiple_times_normal(mocker,):
    query_weather_api_mock = mocker.patch('src.utils.api_queries.query_weather_api',
                                          side_effect=query_weather_api_random)
    locations = ['Valdivia', 'Puerto Montt']
    dates = ['2022-01-10', '2022-01-11']
    response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates)
    assert len(response_df) == 2


def test_query_weather_api_multiple_times_different_lengths(mocker,):
    query_weather_api_mock = mocker.patch('src.utils.api_queries.query_weather_api',
                                          side_effect=query_weather_api_random)
    locations = ['Valdivia', 'Puerto Montt']
    dates = ['2022-01-10']

    with pytest.raises(Exception):
        response_df = query_weather_api_multiple_times(locations=locations, start_dates=dates)



