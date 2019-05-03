# pylint: disable=import-error
import psycopg2
from config import config
 
def connect():
    """ Connect to the PostgreSQL database server """
    connection = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    return connection