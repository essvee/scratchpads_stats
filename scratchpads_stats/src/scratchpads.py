import pymysql


def connect():
    sql = "SHOW DATABASES"

    # Connect and get a list of available databases
    with pymysql.connect(host=host, user=user, password=password) as cursor:
            try:
                cursor.execute(sql)
                databases = [row[0] for row in cursor.fetchall()]

                for db in databases:
                    # Connect to database
                    sql = f"USE {db}"
                    cursor.execute(sql)

                    # check it's a scratchpad and add to new list if so
                    sql = "SHOW TABLES LIKE 'node_counter'"
                    cursor.execute(sql)

                    # Add scratchpads to scratch_dbs list
                    # TODO - is the else None clause the best way to do 'ignore this one'? else continue doesn't work...
                    scratch_dbs.append(db) if len(cursor.fetchall()) is not 0 else None

            except pymysql.Error as e:
                print(f"Error: {e} \nQuery: '{sql}'")

    print(scratch_dbs)

scratch_dbs = []

# Get auth details + date
with open('server-permissions.txt', 'r') as f:
    keys = f.read().splitlines()
    host, user, password, database = keys

if __name__ == '__main__':
    connect()
