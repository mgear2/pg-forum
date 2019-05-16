# pylint: disable=import-error
import psycopg2
from configparser import ConfigParser

class Connector: 
    def __init__(self):
        self.params = self.config()

    def config(self, filename='app\database.ini', section='postgresql'):
        # create a ConfigParser instance
        parser = ConfigParser()
        # read config file
        parser.read(filename)
        # if the file/section is found, parse the contents into a dictionary variable
        fileConfig = {}
        if parser.has_section(section):
            # each line of the .ini file can be used as a key-value pair
            params = parser.items(section)
            for param in params:
                fileConfig[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return fileConfig

    def connect(self):
        # Connect to the PostgreSQL database server
        self.connection = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.connection = psycopg2.connect(**self.params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        # create a cursor instance to use for the duration of this connection
        self.cursor = self.connection.cursor()

    def disconnect(self):
        self.cursor.close()
        if self.connection is not None:
            self.connection.close()
            print('Database connection closed.')
        else:
            print('Connection was already closed!')

    def operate(self, string):
        try: 
            # attempt to execute user command
            self.cursor.execute(string)
            print(string + ' returns:')
            # display the results of the statement
            returnval = self.cursor.fetchall()
            print(returnval)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)