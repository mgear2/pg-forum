# pylint: disable=import-error
from connector import Connector
import psycopg2

if __name__ == '__main__':
    # create a Connector instance and connect to the database
    connector = Connector()
    connector.connect()
    # take user input and send it to the database
    userstring = ""
    while True:
        userstring = input("Enter Command: ")
        if userstring == "exit": 
            break
        returnval = connector.operate(userstring, None)
        print(userstring + ' returns:')
        print(type(returnval))
        if(isinstance(returnval, list)):
            for val in returnval:
                print(val)
        if(isinstance(returnval, Exception)):
            print(returnval)
    # disconnect from the database
    connector.disconnect()