from connector import Connector

if __name__ == '__main__':
    connector = Connector()

    userstring = ""

    while True:
        userstring = input("Enter Command: ")
        if userstring == "exit": 
            break
        connector.operate(userstring)

    connector.disconnect()