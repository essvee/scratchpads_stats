from sshtunnel import SSHTunnelForwarder
import pymysql


def connect():
    # TODO - can/should global vars go somewhere else?
    global databases

    # Get auth details + date
    with open('server-permissions.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    sql = "SHOW DATABASES"

    # Connect and get a list of available databases
    with pymysql.connect(host=host, user=user, password=password) as cursor:

            try:
                cursor.execute(sql)
                databases = [row[0] for row in cursor.fetchall()]
            except pymysql.Error as e:
                print(f"Error: {e} \nQuery: '{sql}'")


if __name__ == '__main__':
    connect()