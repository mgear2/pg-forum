# pylint: disable=import-error
import psycopg2

def disconnect(connection):
    if connection is not None:
        connection.close()
        print('Database connection closed.')
    else:
        print('Connection was already closed!')