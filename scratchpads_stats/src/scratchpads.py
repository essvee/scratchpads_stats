from sshtunnel import SSHTunnelForwarder
import pymysql


def connect():
    # TODO - can/should global vars go somewhere else?
    global databases

    sql = "SHOW DATABASES"

    # Connect and get a list of available databases
    with pymysql.connect(host=host, user=user, password=password) as cursor:
            try:
                cursor.execute(sql)
                databases = [row[0] for row in cursor.fetchall()]
            except pymysql.Error as e:
                print(f"Error: {e} \nQuery: '{sql}'")

    check_scratchpad_db()


# Removes non-scratchpad dbs from list
def check_scratchpad_db():
    scratch_dbs = []

    with pymysql.connect(host=host, user=user, password=password) as cursor:
        for db in databases:
            try:
                # Connect to database
                sql = f"USE {db}"
                cursor.execute(sql)

                # check it's a scratchpad - if not, remove from list
                sql = "SHOW TABLES LIKE 'node_counter'"
                cursor.execute(sql)

                # Add scratchpads to scratch_dbs list
                # TODO - is the else clause the best way to do 'ignore this one'?
                scratch_dbs.append(db) if len(cursor.fetchall()) is not 0 else None

            except pymysql.Error as e:
                print(f"Error: {e} \nQuery: '{sql}'")

    print(scratch_dbs)

    # try:
    #     for i in cursor.fetchall():
    #         s = len(i)
    #         print(f"{db} returns result of length {s}")
    #     # if cursor.fetchall() is None:
    #     #     print(f"{db} is not a scratchpad")
    #     # else:
    #     #     print(f"{db} is a scratchpad!")
    # except pymysql.Error as e:
    #     print(f"Error: {e} \nQuery: '{sql}'")


# Get auth details + date
with open('server-permissions.txt', 'r') as f:
    keys = f.read().splitlines()
    host, user, password, database = keys

if __name__ == '__main__':
    connect()
