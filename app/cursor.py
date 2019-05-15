# pylint: disable=import-error
import psycopg2

def cursor(connection, string):
    try: 
        # create a cursor
        cursor = connection.cursor()

        # execute a statement
        print(string + ' returns:')
        cursor.execute(string)

        returnval = cursor.fetchall()
        print(returnval)

        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)