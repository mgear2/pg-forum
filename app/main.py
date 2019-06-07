# pylint: disable=import-error
import sys
import psycopg2
from connector import Connector
from browser import Browser

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()
    # take user input and send it to the database
    browser = Browser(connector)
    browser.commandrunner()
    # disconnect from the database
    connector.disconnect()