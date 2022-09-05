import sqlite3


def get_connection(path, db_name):
    conn = sqlite3.connect(f'{path}{db_name}')
    return conn
