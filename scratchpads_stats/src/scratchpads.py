import pymysql
import datetime
import os
import json


def load():
    # Check to see if there's an existing json file we need to load, or make a dict to populate the new one
    if os.path.exists("scratch_stats.json"):
        with open("scratch_stats.json") as archive_file:
            scratch_stats = json.load(archive_file)
    else:
        scratch_stats = {}

    # Date last run
    scratch_stats['last_run'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    with pymysql.connect(host=host, user=user, password=password) as cursor:
        scratch_dbs = get_databases(cursor)

        for db in scratch_dbs:
            site_month = {}
            node_counts = node_type_counts(cursor, db)
            node_total_count = sum(node_counts.values())


def get_databases(cursor):
    sql = "SHOW DATABASES"
    scratch_dbs = []
    # Get a list of all available databases
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
            # TODO - is the 'else None' clause the best way to do 'ignore this one'? 'else continue' doesn't work...
            scratch_dbs.append(db) if len(cursor.fetchall()) is not 0 else None

    except pymysql.Error as e:
        print(f"Error: {e} \nQuery: '{sql}'")

    return scratch_dbs


def node_type_counts(cursor, db):
    node_counts = {}
    try:
        sql = f"USE {db}"
        cursor.execute(sql)

        # Get node counts - total and breakdown per type
        sql = "SELECT type, COUNT(*) FROM node GROUP BY type"
        cursor.execute(sql)

        # Load node types and counts into dict and return
        for node_type in cursor.fetchall():
            node_counts[node_type[0]] = node_type[1]

        return node_counts

    except pymysql.Error as e:
        print(f"Error: {e} \nQuery: '{sql}' \nDatabase: {db}")


# Get auth details + date
with open('server-permissions.txt', 'r') as f:
    keys = f.read().splitlines()
    host, user, password, database = keys

if __name__ == '__main__':
    load()
