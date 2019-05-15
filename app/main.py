from connect import connect
from disconnect import disconnect
from cursor import cursor

if __name__ == '__main__':

    connection = connect()
    userstring = ""

    while True:
        userstring = input("Enter command: ")
        if userstring == "exit":
            break
        cursor(connection, userstring)

    disconnect(connection)