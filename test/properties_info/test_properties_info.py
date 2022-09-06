import sys
import pytest
sys.path.append('../..')
import pandas as pd
from src.properties_info.properties_info import PropertiesInfo


def visits_data_total_visits():
    data = [
        [1, '2022-01-13', 'Visit', 'Cancelled'],
        [2, '2022-01-13', 'Visit', 'Done'],
        [1, '2022-01-13', 'Visit', 'Done'],
        [3, '2022-01-13', 'Else', 'Done']
    ]

    columns = ['property_id', 'begin_date', 'type_visit', 'status']
    return pd.DataFrame(data, columns=columns)


class TestGetTotalVisits(object):
    def test_on_some_visits(self, mocker,):
        properties_visit_information = PropertiesInfo()
        load_visits_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_visits_data',
                                               side_effect=visits_data_total_visits)
        result = properties_visit_information.get_total_visits()
        assert result == 2

    def test_on_no_visits(self, mocker,):
        properties_visit_information = PropertiesInfo()
        columns = ['property_id', 'begin_date', 'type_visit', 'status']

        load_visits_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_visits_data',
                                               return_value=pd.DataFrame([], columns=columns))
        result = properties_visit_information.get_total_visits()
        assert result == 0


def users_data_average_properties_by_user():
    data = [
        [1, 1],
        [2, 1],
        [3, 2],
        [5, 4],
        [6, 4],
        [7, 4]
    ]

    columns = ['property_id', 'user_id']
    return pd.DataFrame(data, columns=columns)


class TestGetAveragePropertiesByUser(object):
    def test_on_all_users_with_properties(self, mocker,):
        properties_info = PropertiesInfo()
        load_users_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_users_data',
                                               side_effect=users_data_average_properties_by_user)
        result = properties_info.get_average_properties_by_user()
        print(result)
        assert result == 2


def input_get_hourly_visits():
    data = [
        [1, '2022-01-13 10:00:00', '2022-01-13 12:00:00'],
        [2, '2022-01-13 23:00:00', '2022-01-14 03:00:00']
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'begin_date', 'end_date'])
    df['begin_date'] = pd.to_datetime(df['begin_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df


def output_get_hourly_visits():
    data = [
        [1, '2022-01-13 10:00:00'],
        [1, '2022-01-13 11:00:00'],
        [2, '2022-01-13 23:00:00'],
        [2, '2022-01-14 00:00:00'],
        [2, '2022-01-14 01:00:00'],
        [2, '2022-01-14 02:00:00']
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'date'])
    df['date'] = pd.to_datetime(df['date'])
    return df


class TestGetHourlyVisits(object):
    def test_on_multiple_dates(self):
        properties_info = PropertiesInfo()
        input_visits = input_get_hourly_visits()

        columns = ['scheduled_id', 'date']

        actual_output = properties_info._get_hourly_visits_df(input_visits)[columns]
        expected_output = output_get_hourly_visits()[columns]

        print(actual_output)
        print(expected_output)

        pd.testing.assert_frame_equal(actual_output.reset_index(drop=True),
                                      expected_output.reset_index(drop=True),
                                      check_index_type=False)


def input_add_hourly_temperatures_to_visits():
    data = [
        [1, '2022-01-13 10:00:00', '70', '-40'],
        [1, '2022-01-13 11:00:00', '70', '-40'],
        [2, '2022-01-13 23:00:00', '80', '-30'],
        [2, '2022-01-14 00:00:00', '80', '-30'],
        [2, '2022-01-14 01:00:00', '80', '-30'],
        [2, '2022-01-14 02:00:00', '80', '-30']
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'date', 'latitude', 'longitude'])
    df['date'] = pd.to_datetime(df['date'])
    return df


def query_weather_api_hourly(*args, **kwargs):

    data = [
        ['2022-01-13 09:00:00', '70', '-40', 45],
        ['2022-01-13 10:00:00', '70', '-40', 49],
        ['2022-01-13 11:00:00', '70', '-40', 51],
        ['2022-01-13 12:00:00', '70', '-40', 52],
        ['2022-01-13 23:00:00', '80', '-30', 0],
        ['2022-01-14 00:00:00', '80', '-30', 0],
        ['2022-01-14 01:00:00', '80', '-30', 1],
        ['2022-01-14 02:00:00', '80', '-30', 1],
        ['2022-01-14 03:00:00', '80', '-30', 1]
    ]

    columns = ['date', 'latitude', 'longitude', 'temp']

    return_values = pd.DataFrame(data, columns=columns)
    return_values['date'] = pd.to_datetime(return_values['date'])
    return return_values


def output_add_hourly_temperatures_to_visits():
    data = [
        [1, '2022-01-13 10:00:00', '70', '-40', 49],
        [1, '2022-01-13 11:00:00', '70', '-40', 51],
        [2, '2022-01-13 23:00:00', '80', '-30', 0],
        [2, '2022-01-14 00:00:00', '80', '-30', 0],
        [2, '2022-01-14 01:00:00', '80', '-30', 1],
        [2, '2022-01-14 02:00:00', '80', '-30', 1]
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'date', 'latitude', 'longitude', 'temp'])
    df['date'] = pd.to_datetime(df['date'])
    return df


class TestAddHourlyTemperaturesToVisits(object):
    def test_on_multiple_dates(self, mocker,):
        properties_info = PropertiesInfo()
        mocked_get_data_from_db_or_api = mocker.patch('src.properties_info.properties_info.get_data_from_db_or_api',
                                                      side_effect=query_weather_api_hourly)
        hourly_visits = input_add_hourly_temperatures_to_visits()

        actual_output = properties_info._add_hourly_temperatures_to_visits(hourly_visits)
        expected_output = output_add_hourly_temperatures_to_visits()

        pd.testing.assert_frame_equal(actual_output.reset_index(drop=True),
                                      expected_output.reset_index(drop=True),
                                      check_index_type=False)


def add_hourly_temperatures_to_visits(*args):
    data = [
        [1, 1, '2022-01-13 10:00:00', '70', '-40', 49],
        [1, 1, '2022-01-13 11:00:00', '70', '-40', 51],
        [2, 1, '2022-01-13 23:00:00', '70', '-40', 0],
        [2, 1, '2022-01-14 00:00:00', '70', '-40', 0],
        [2, 1, '2022-01-14 01:00:00', '70', '-40', 1],
        [2, 1, '2022-01-14 02:00:00', '70', '-40', 1],
        [3, 2, '2022-01-15 02:00:00', '40', '-30', 1],
        [3, 2, '2022-01-15 03:00:00', '40', '-30', 1],
        [3, 2, '2022-01-15 04:00:00', '40', '-30', 1],
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'property_id', 'date', 'latitude', 'longitude', 'temperature'])
    df['date'] = pd.to_datetime(df['date'])
    return df


class TestGetAverageTemperatureInVisitsByUser(object):
    def test_on_multiple_visits_multiple_properties(self, mocker,):
        properties_info = PropertiesInfo()

        mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_users_data',
                     return_value=pd.DataFrame([], columns=['user_id', 'property_id']))
        mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_properties_data',
                     return_value=pd.DataFrame([], columns=['property_id', 'latitude', 'longitude']))

        visits_columns = ['scheduled_id', 'property_id', 'begin_date', 'end_date', 'status', 'type_visit']
        mocker.patch('src.properties_info.properties_info.PropertiesInfo._load_visits_data',
                     return_value=pd.DataFrame([], columns=visits_columns))

        mocker.patch('src.properties_info.properties_info.PropertiesInfo._visits_preprocessing')
        mocker.patch('src.properties_info.properties_info.PropertiesInfo.merge_data')
        #mocker.patch('src.properties_info.properties_info._get_hourly_visits_data')
        mocker.patch('src.properties_info.properties_info.PropertiesInfo._get_hourly_visits_df')
        mocker.patch('src.properties_info.properties_info.PropertiesInfo._add_hourly_temperatures_to_visits',
                     side_effect=add_hourly_temperatures_to_visits)

        avg_temperature = properties_info.get_average_temperature_in_visits_by_user(1)
        print(avg_temperature)
        assert pytest.approx(avg_temperature) == pytest.approx((50 + 0.5 + 1)/3)


def input_add_daily_conditions_to_visits():
    data = [
        [1, '2022-01-13 10:00:00', '70', '-40'],
        [2, '2022-01-13 23:00:00', '80', '-30']
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'begin_date', 'latitude', 'longitude'])
    df['begin_date'] = pd.to_datetime(df['begin_date'])
    return df


def query_weather_api_daily(*args, **kwargs):

    data = [
        ['2022-01-13', '70', '-40', 'Rain', 50],
        ['2022-01-13', '80', '-30', 'Snow', 50]
    ]

    columns = ['date', 'latitude', 'longitude', 'conditions', 'temperature']

    return_values = pd.DataFrame(data, columns=columns)
    return_values['date'] = pd.to_datetime(return_values['date'])
    return return_values


def output_add_daily_conditions_to_visits():
    data = [
        [1, '2022-01-13 10:00:00', '70', '-40', 'Rain', 50],
        [2, '2022-01-13 23:00:00', '80', '-30', 'Snow', 50]
    ]
    df = pd.DataFrame(data, columns=['scheduled_id', 'begin_date', 'latitude', 'longitude', 'conditions', 'temperature'])
    df['begin_date'] = pd.to_datetime(df['begin_date'])
    return df


class TestAddDailyConditionsToVisits(object):
    def test_on_multiple_dates(self, mocker,):
        properties_info = PropertiesInfo()
        mocked_get_data_from_db_or_api = mocker.patch('src.properties_info.properties_info.get_data_from_db_or_api',
                                                      side_effect=query_weather_api_daily)
        visits = input_add_daily_conditions_to_visits()

        actual_output = properties_info._add_daily_conditions_to_visits(visits)
        expected_output = output_add_daily_conditions_to_visits()

        print(actual_output)
        print(expected_output)

        pd.testing.assert_frame_equal(actual_output.reset_index(drop=True),
                                      expected_output.reset_index(drop=True),
                                      check_index_type=False)









