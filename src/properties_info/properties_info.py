import os
import sys
sys.path.append('../..')
from datetime import timedelta
import pandas as pd
import numpy as np
from src.utils.connections import get_connection
from src.utils.queries import get_data_from_db_or_api

BASE_PATH = os.path.abspath(os.path.dirname(__file__))


class PropertiesInfo:
    def __init__(self):
        # TODO: put these in config file
        self.visits_filename = 'visits.csv'
        self.properties_filename = 'properties.csv'
        self.users_filename = 'users.csv'
        self.conn = get_connection(f'{BASE_PATH}/../', 'weather_api.db')

    def load_visits_data(self):
        visits = pd.read_csv(f'{BASE_PATH}/../data/{self.visits_filename}')
        return visits

    def load_properties_data(self):
        properties = pd.read_csv(f'{BASE_PATH}/../data/{self.properties_filename}')
        return properties

    def load_users_data(self):
        users = pd.read_csv(f'{BASE_PATH}/../data/{self.users_filename}')
        return users

    @staticmethod
    def _visits_preprocessing(visits):
        """
        Preprocess visits DataFrame

        Takes the visits DataFrame and transform its begin_date and end_date columns to UTC dates, flooring
        down the time to the hourly value: e.g. 13:15:04 to 13:00:00.

        Parameters
        ----------
        :param DataFrame visits: pandas DataFrame with at least begin_date and end_date
        :return:  pandas DataFrame with begin_date and end_date transformed
        """
        visits['begin_date'] = pd.to_datetime(visits['begin_date']).dt.tz_convert(None).dt.floor('h')
        visits['end_date'] = pd.to_datetime(visits['end_date']).dt.tz_convert(None).dt.floor('h')
        return visits

    def get_total_visits(self):
        visits = self.load_visits_data()[['type_visit', 'status']]
        completed_visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        return len(completed_visits)

    def get_average_properties_by_user(self):
        users = self.load_users_data()
        print('ACAAAAAA', users)
        result = users.groupby('user_id').count()['property_id'].mean()
        return result

    def get_average_temperature_in_visits_by_user(self, user_id):
        # Load and preprocess visits information
        visits = self.load_visits_data()
        visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        visits = visits[['scheduled_id', 'property_id', 'begin_date', 'end_date']]
        visits = self._visits_preprocessing(visits)

        # Load user information
        users = self.load_users_data()
        users = users[users['user_id'] == user_id][['user_id', 'property_id']]

        # Load properties information
        properties = self.load_properties_data()
        properties = properties[['property_id', 'latitude', 'longitude']]

        # Merge all dfs and get hourly data
        visits = self.merge_data(visits, properties, users)
        hourly_visits = self._get_hourly_visits_df(visits)
        hourly_visits = self._add_hourly_temperatures_to_visits(hourly_visits)

        # Compute the average temperature
        print(hourly_visits)
        mean_temp_visits = hourly_visits[['scheduled_id', 'temperature']].groupby('scheduled_id').mean()
        return mean_temp_visits.mean()['temperature']

    @staticmethod
    def merge_data(visits, properties, users=None):
        if users is not None:
            properties = properties.merge(users, on='property_id')
        visits = properties.merge(visits, on='property_id')
        return visits

    @staticmethod
    def _get_hourly_visits_df(visits):
        """
        Expands a df with visits information addind the visit hours to the rows.

        It looks at begin_date and start_date of each row, and adds as many rows as hours are in between
        those two dates, replicating all the extra column information for the original row. For example,
        if begin_date = '2020-01-01 13:00:00' and end_date = '2020-01-01 16:00:00', the new dataframe
        would have 3 entries coming from this row, corresponding to the 13h, 14h and 15h. Also, the end_date
        is no longer relevant, so it is deleted.

        Parameters
        ----------
        :param DataFrame visits: pandas DataFrame with at least latitude, longitude, scheduled_id, property_id,
                                 begin_date and end_date
        :return: DataFrame with added rows and deleted column 'end_date'
        """
        other_columns = [col for col in visits.columns if col not in ('begin_date', 'end_date')]

        new_visits_list = []

        for row in visits.iterrows():
            begin_date = row[1]['begin_date']
            end_date = row[1]['end_date']

            if begin_date != end_date:
                end_date = end_date - timedelta(hours=1)

            dates = pd.date_range(start=begin_date, end=end_date, freq='h')
            new_df = pd.DataFrame(dates, columns=['date'])
            # All other columns are just replicated for all the new dates
            new_df[other_columns] = row[1][other_columns]
            new_visits_list.append(new_df)

        return pd.concat(new_visits_list)

    def _add_hourly_temperatures_to_visits(self, hourly_visits):
        """
        Adds the temperature value for each row of hourly visits.

        Receives the visits DataFrame having been added the hourly temperatures using the _get_hourly_visits_df
        and appends the temperature for each row, calling the weather API taking the latitude, longitude, and
        date values of the row.

        Parameters
        ----------
        :param DataFrame hourly_visits: pandas DataFrame with at least latitude, longitude, scheduled_id, property_id
                                 and date
        :return: DataFrame hourly_visits DataFrame with temp row (temperature)
        """

        dates = list(hourly_visits['date'].dt.strftime('%Y-%m-%d').values)
        hours = list(hourly_visits['date'].dt.strftime('%H:%M:%S').values)
        latitudes = list(hourly_visits['latitude'].values)
        longitudes = list(hourly_visits['longitude'].values)

        columns = ['date', 'latitude', 'longitude', 'conditions', 'temperature']

        weather_df = get_data_from_db_or_api(self.conn, dates, latitudes, longitudes, columns, include='hours',
                                             hours=hours)

        return hourly_visits.merge(weather_df, on=['date', 'latitude', 'longitude'])














