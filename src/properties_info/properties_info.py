import os
import sys
sys.path.append('../..')
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

    def load_visits_data(self):
        visits = pd.read_csv(f'{BASE_PATH}/../data/{self.visits_filename}')
        return visits

    def load_properties_data(self):
        properties = pd.read_csv(f'{BASE_PATH}/../data/{self.properties_filename}')
        return properties

    def load_users_data(self):
        users = pd.read_csv(f'{BASE_PATH}/../data/{self.users_filename}')
        return users

    def get_total_visits(self):
        visits = self.load_visits_data()[['type_visit', 'status']]
        completed_visits = visits[(visits['type_visit'] == 'Visit') & (visits['status'] == 'Done')]
        return len(completed_visits)

    def get_average_properties_by_user(self):
        users = self.load_users_data()
        result = users.groupby('user_id').count()['property_id'].mean()
        return result





