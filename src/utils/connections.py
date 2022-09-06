import sqlite3


def get_connection(path, db_name):
    """
    Gets the connector object connected to the sqlite DB

    Parameters
    ----------
    :param path: DB folder path
    :param db_name: DB name
    :return: sqlite3 connector object connected to the DB
    """
    conn = sqlite3.connect(f'{path}{db_name}')
    return conn
