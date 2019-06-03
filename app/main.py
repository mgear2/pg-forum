# pylint: disable=import-error
from connector import Connector
from browser import Browser
import psycopg2
import sys

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()
    # take user input and send it to the database
    browser = Browser(connector)
    browser.commandrunner()
    # disconnect from the database
    connector.disconnect()