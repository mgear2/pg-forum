from connector import Connector

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
        connector.operate(userstring)
    # disconnect from the database
    connector.disconnect()