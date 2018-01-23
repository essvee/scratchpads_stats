from sshtunnel import SSHTunnelForwarder
import pymysql


def connect():
    # Get auth details + date
    with open('server-permissions.txt', 'r') as f:
        keys = f.read().splitlines()
        host, user, password, database = keys

    sql = "SHOW DATABASES;"
    databases = []

    # Connect and get a list of available databases
    with pymysql.connect(host=host, user=user, password=password) as cursor:

            try:
                cursor.execute(sql)
                for n in cursor.fetchall():
                    databases.append(n[0])
            except pymysql.Error as e:
                print(sql)
                print(e)


if __name__ == '__main__':
    connect()