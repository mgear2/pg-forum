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

    while True:
        userstring = input("Enter Command: ")
        if userstring == "exit": 
            break
        if userstring == "sqlrunner()":
            runnerval = browser.sqlrunner()
            if runnerval == "exit":
                break
            else:
                continue
        else:
            print("Command not recognized")
    # disconnect from the database
    connector.disconnect()