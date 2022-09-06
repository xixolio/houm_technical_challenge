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

    def get_total_visits(self):
        """
        Get the total amount of completed visits (Question 1)

        Loads the visits.csv data, gets only rows corresponding to completed visits based on the type_visit
        and status columns, and then just counts the total amount of performed visits considering each row
        as an independent one (different scheduled_id).

        :return: int, total amount of completed visits in the visits.csv data
        """
        visits = self._load_visits_data()[['type_visit', 'status']]
        completed_visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        return len(completed_visits)

    def get_average_properties_by_user(self):
        """
        Get average amount of properties across all users (Question 2)

        It calculates the amount of properties each user has according to the users data (users.csv) and
        then averages accross users to know whats the average number of properties the users have.

        :return: float, average numbers of properties users have
        """
        users = self._load_users_data()
        result = users.groupby('user_id').count()['property_id'].mean()
        return result

    def get_average_temperature_in_visits_by_user(self, user_id):
        """
        Get the average temperature accross all visits done to all properties of an user (Question 3)

        Loads visits, users and properties data (visits.csv, users.csv, properties.csv) and merges the data to get
        all visits done and their properties information for a specific user. Then, the average temperature of each
        individual visit is calculated by getting the temperature information for each visiting hour from the DB or
        the API. For example, if one of the visits at a property started at 2020-01-02 10:00:00 and ended at
        2020-01-02 14:00:00, we get the temperature for each hour in between 10:00:00 and 14:00:00 (non inclusive)
        and average that. Then, the average temperature across all visits is calculated by taking advantage of the
        fact that each visit is differentiated by its scheduled_id value.

        Parameters
        ----------
        :param int user_id: a valid user_id from the users.csv data
        :return: float average temperature across all visits performed to all properties of the selected user
        """
        # Load and preprocess visits information
        visits = self._load_visits_data()
        visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        visits = visits[['scheduled_id', 'property_id', 'begin_date', 'end_date']]
        visits = self._visits_preprocessing(visits)

        # Load user information
        users = self._load_users_data()
        users = users[users['user_id'] == user_id][['user_id', 'property_id']]

        if len(users) == 0:
            raise Exception(f'user_id: {user_id} not found in users data. Please provide a valid user_id.')

        # Load properties information
        properties = self._load_properties_data()
        properties = properties[['property_id', 'latitude', 'longitude']]

        # Merge all dfs and get hourly data
        visits = self.merge_data(visits, properties, users)
        hourly_visits = self._get_hourly_visits_df(visits)
        hourly_visits = self._add_hourly_temperatures_to_visits(hourly_visits)

        # Compute the average temperature
        return self.compute_average_temperature_by_visit(hourly_visits)

    def get_average_temperature_in_visits_by_weather_condition(self, weather_condition):
        """
        Get the average temperature accross all visits done in days having a specific weather_condition (Question 4)

        Loads visits and properties data (visits.csv, properties.csv) and merges the data to get
        all visits and its properties information. Then we add weather condition information from the DB or API to
        each visit, taking the begin_date column as the day the visit was performed. Giving the new condition
        column added to the data, we filter all rows having the weather_condition present in the condition string.
        For example, if the weather_condition was 'Rain', then it would select visits having conditions such as
        'Rain', 'Heavy Rain', 'Light Rain', and so on.
        (Refer to https://github.com/visualcrossing/WeatherApi/blob/master/lang/en.txt for all weather types coming
        from the API).
        Then, the average temperature of each individual visit is calculated by getting the temperature information
        for each visiting hour from the DB or the API. For example, if one of the visits at a property started at
        2020-01-02 10:00:00 and ended at 2020-01-02 14:00:00, we get the temperature for each hour in between 10:00:00
        and 14:00:00 (non inclusive) and average that. Then, the average temperature across all visits is calculated
        by taking advantage of the fact that each visit is differentiated by its scheduled_id value.

        Parameters
        ----------
        :param str weather_condition: a valid weather condition or key word weather condition
        :return: float average temperature across all visits performed on days with the weather_condition
        """
        # Load and preprocess visits information
        visits = self._load_visits_data()
        visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        visits = visits[['scheduled_id', 'property_id', 'begin_date', 'end_date']]
        visits = self._visits_preprocessing(visits)

        # Load properties information
        properties = self._load_properties_data()
        properties = properties[['property_id', 'latitude', 'longitude']]

        # Merge all dfs and get daily conditions data
        visits = self.merge_data(visits, properties)
        visits = self._add_daily_conditions_to_visits(visits)

        # Filter by Rain keyword and drop conditions column
        visits = visits[visits['conditions'].str.contains(weather_condition)]
        visits = visits.drop(columns=['conditions', 'temperature'])

        if len(visits) == 0:
            raise Exception(f'Visits performed on days with weather_condition {weather_condition} not found in the data. '
                            f'Are you sure you provided a valid weather_condition value?.')

        # Get hourly visits with temperatures and calculate mean temperatures for each visit
        hourly_visits = self._get_hourly_visits_df(visits)
        hourly_visits = self._add_hourly_temperatures_to_visits(hourly_visits)

        return self.compute_average_temperature_by_visit(hourly_visits)

    def get_average_temperature_in_visits_by_location(self, location):
        """
        Get the average temperature accross all visits done to all properties at a particular location (Question 5)

        Loads visits and properties data (visits.csv, properties.csv), filters the properties by the given location,
        and merges the data to get all visits done and their properties information. Then, the average temperature of
        each individual visit is calculated by getting the temperature information for each visiting hour from the DB
        or the API. For example, if one of the visits at a property started at 2020-01-02 10:00:00 and ended at
        2020-01-02 14:00:00, we get the temperature for each hour in between 10:00:00 and 14:00:00 (non inclusive)
        and average that. Then, the average temperature across all visits is calculated by taking advantage of the
        fact that each visit is differentiated by its scheduled_id value.

        Parameters
        ----------
        :param str location: a value from the localidad column in properties.csv
        :return: float average temperature across all visits performed at the particular location
        """

        # Load and preprocess visits information
        visits = self._load_visits_data()
        visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        visits = visits[['scheduled_id', 'property_id', 'begin_date', 'end_date']]
        visits = self._visits_preprocessing(visits)

        # Load properties information and filter by location
        properties = self._load_properties_data()
        properties = properties[['property_id', 'latitude', 'longitude', 'localidad']]
        properties = properties[properties['localidad'] == location]

        if len(properties) == 0:
            raise Exception(f'properties with localidad: {location} not found in properties data. Please provide a '
                            f'valid location.')

        # Merge all dfs
        visits = self.merge_data(visits, properties)

        # Get hourly visits with temperatures and calculate mean temperatures for each visit
        hourly_visits = self._get_hourly_visits_df(visits)
        hourly_visits = self._add_hourly_temperatures_to_visits(hourly_visits)

        return self.compute_average_temperature_by_visit(hourly_visits)

    def _load_visits_data(self):
        """
        Loads visits data
        :return: DataFrame with visit data information, columns ('scheduled_id', 'property_id', 'begin_date',
                'end_date', 'type_visit', 'status',)
        """
        visits = pd.read_csv(f'{BASE_PATH}/../data/{self.visits_filename}')
        return visits

    def _load_properties_data(self):
        """
        Loads properties data
        :return: DataFrame with property data information, columns ('property_id', 'type_house', 'business_type',
                'bedrooms', 'bathrooms', 'parking_lots', 'services', 'balcony', 'pool', 'latitude', 'longitude',
                'localidad', 'city', 'region', 'country')
        """
        properties = pd.read_csv(f'{BASE_PATH}/../data/{self.properties_filename}')
        return properties

    def _load_users_data(self):
        """
        Loads users data
        :return: DataFrame with users data information, columns ('property_id', 'user_id', 'first_name', 'last_name',
                'address')
        """
        users = pd.read_csv(f'{BASE_PATH}/../data/{self.users_filename}')
        return users

    @staticmethod
    def _visits_preprocessing(visits):
        """
        Preprocess visits DataFrame

        Takes the visits DataFrame and transform its begin_date and end_date columns to UTC dates, flooring
        down the time to the hourly value as well: e.g. 13:15:04 to 13:00:00.

        Parameters
        ----------
        :param DataFrame visits: pandas DataFrame with at least begin_date and end_date
        :return: DataFrame with begin_date and end_date transformed to UTC and floored to hourly value
        """
        visits['begin_date'] = pd.to_datetime(visits['begin_date']).dt.tz_convert(None).dt.floor('h')
        visits['end_date'] = pd.to_datetime(visits['end_date']).dt.tz_convert(None).dt.floor('h')
        return visits

    @staticmethod
    def merge_data(visits, properties, users=None):
        """
        Merges all data sources provided on the property_id column

        Parameters
        ----------
        :param visits: visits DataFrame with at least property_id column
        :param properties: properties DataFrame with at least property_id column
        :param users: (optional) users DataFrame with at least property_id column
        :return: DataFrame, results of merge
        """
        if users is not None:
            properties = properties.merge(users, on='property_id')
        visits = properties.merge(visits, on='property_id')
        return visits

    @staticmethod
    def compute_average_temperature_by_visit(hourly_visits):
        """
        Gets the average temperature across all visits in the DataFrame

        Since each visit is characterized by a sheduled_id value, the temperatures for all the hours of each
        visits is first averaged to get the average temperature of each visit. Then, the average of that is computed
        to get the total average across all visits.

        Parameters
        ----------
        :param DataFrame hourly_visits: visits dataframe with at least scheduled_id and temperature columns.
        :return: float, average temperature across all visits in the DataFrame
        """
        mean_temp_visits = hourly_visits[['scheduled_id', 'temperature']].groupby('scheduled_id').mean()
        return mean_temp_visits.mean()['temperature']

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

    def _add_daily_conditions_to_visits(self, visits):
        """
        Adds the conditions value for each row of daily visits.

        Receives the visits DataFrame and adds the column Conditions to each row, calling the weather API taking the
        latitude, longitude, and date values of the row. We assume that the day corresponds to the begin_date day,
        ignoring the end_date.

        Parameters
        ----------
        :param DataFrame visits: pandas DataFrame with at least latitude, longitude, scheduled_id, property_id
                                 and date
        :return: DataFrame visits DataFrame with conditions column (conditions)
        """

        dates = list(visits['begin_date'].dt.strftime('%Y-%m-%d').values)
        latitudes = list(visits['latitude'].values)
        longitudes = list(visits['longitude'].values)

        columns = ['date', 'latitude', 'longitude', 'conditions', 'temperature']

        weather_df = get_data_from_db_or_api(self.conn, dates, latitudes, longitudes, columns, include='days')
        visits['date'] = pd.to_datetime(visits['begin_date'].dt.strftime('%Y-%m-%d'))
        visits = visits.merge(weather_df, on=['date', 'latitude', 'longitude'])
        visits = visits.drop(columns='date')
        return visits














