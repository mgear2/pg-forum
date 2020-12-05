# pylint: disable=import-error
import psycopg2
from configparser import ConfigParser
from pathlib import Path

class Connector:
    def __init__(self, section):
        self.datafolder = Path("src/")
        self.filename = Path("database.ini")
        self.section = section
        self.configparse()

    def configparse(self):
        # create a ConfigParser instance
        parser = ConfigParser()
        # read config file
        parser.read(self.datafolder / self.filename)
        # parse the config file contents into a dictionary variable
        self.config = {}
        if parser.has_section(self.section):
            # each line of the .ini file can be used as a key-value pair
            pairs = parser.items(self.section)
            for pair in pairs:
                if pair[0] == "schema":
                    self.schema = pair[1]
                    continue
                self.config[pair[0]] = pair[1]
        else:
            raise Exception(
                "Section {0} not found in the {1} file".format(
                    self.section, self.filename
                )
            )

    def connect(self):
        self.connection = None
        # connect to the PostgreSQL server
        try:
            print("Connecting to the PostgreSQL database...")
            self.connection = psycopg2.connect(**self.config)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        # create a cursor instance to use for the duration of this connection
        self.cursor = self.connection.cursor()
        self.cursor.execute("SET search_path TO " + self.schema)
        print(self.connection.encoding)

    def disconnect(self):
        self.cursor.close()
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed.")
        else:
            print("Connection was already closed!")

    def operate(self, string, builder):
        try:
            # Some commands have a builder variable (tuples for format string)
            if builder == None:
                self.cursor.execute(string)
            else:
                self.cursor.execute(string, builder)
            self.connection.commit()
            returnval = self.cursor.fetchall()
            return returnval
        except (Exception, psycopg2.DatabaseError) as error:
            if str(error) == "no results to fetch":
                return
            print("Caught: {0}".format(str(error)))
            self.connection.rollback()
            return error
