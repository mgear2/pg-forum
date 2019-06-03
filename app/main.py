# pylint: disable=import-error
from connector import Connector
from browser import Browser
import psycopg2

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()
    # take user input and send it to the database
    browser = Browser(connector)
    browser.sqlrunner()
    # disconnect from the database
    connector.disconnect()