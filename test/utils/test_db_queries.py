import os
import sys
sys.path.append('../..')
import pytest
import pandas as pd
import numpy as np
from src.utils.db_queries import select_from_weather_data, get_connection

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def weather_data_sqlite(tmpdir, scope='session'):
    db_name = 'weather_data.db'
    conn = get_connection(tmpdir, db_name)
    cursor = conn.cursor()
    sql_query = open(f'{BASE_PATH}/../../src/sql_queries/create_table_weather_daily_data.sql').read()
    cursor.execute(sql_query)

    test_data = [
        ('2022-01-10', 'Valdivia', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-10', 'Valdivia', 80.3, 30.1, 'Rain', 30.1),
        ('2022-01-11', 'Santiago', 70.3, 30.1, 'Rain', 30.1),
        ('2022-01-12', 'Calama', 70.3, 30.1, 'Rain', 30.1)
    ]

    cursor.executemany('INSERT INTO weather_daily_data VALUES(?, ?, ?, ?, ?, ?)', test_data)

    yield conn


def test_select_from_weather_data_some_rows(weather_data_sqlite):
    # Third row doesnt exist
    dates = ['2022-01-10', '2022-01-11', '2021-01-11']
    locations = ['Valdivia', 'Santiago', 'Valdivia']
    latitudes = [70.3, 70.3]
    longitudes = [30.1, 30.1]

    db_name = 'weather_data'
    conn = weather_data_sqlite
    df = select_from_weather_data(conn, dates, locations, latitudes, longitudes)
    assert len(df) == 2


