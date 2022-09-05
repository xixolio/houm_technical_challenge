import sys
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
        load_visits_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo.load_visits_data',
                                               side_effect=visits_data_total_visits)
        result = properties_visit_information.get_total_visits()
        assert result == 2

    def test_on_no_visits(self, mocker,):
        properties_visit_information = PropertiesInfo()
        columns = ['property_id', 'begin_date', 'type_visit', 'status']

        load_visits_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo.load_visits_data',
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
        load_visits_data_mocker = mocker.patch('src.properties_info.properties_info.PropertiesInfo.load_users_data',
                                               side_effect=users_data_average_properties_by_user)
        result = properties_info.get_average_properties_by_user()
        print(result)
        assert result == 2


