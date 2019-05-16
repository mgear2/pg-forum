# pylint: disable=import-error
import psycopg2
from configparser import ConfigParser

class Connector: 
    def __init__(self):
        self.filename = 'app/database.ini'
        self.section = 'postgresql'
        self.config()

    def config(self):
        # create a ConfigParser instance
        parser = ConfigParser()
        # read config file
        parser.read(self.filename)
        # if the file/section is found, parse the contents into a dictionary variable
        self.params = {}
        if parser.has_section(self.section):
            # each line of the .ini file can be used as a key-value pair
            pairs = parser.items(self.section)
            for pair in pairs:
                self.params[pair[0]] = pair[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(self.section, self.filename))

    def connect(self):
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