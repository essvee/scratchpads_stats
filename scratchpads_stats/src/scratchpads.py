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
            cursor.execute(f"USE {db}")
            # TODO - add each site_month dict to a list when finished, then add complete list to month dict?
            site_stats = dict(name=db,
                              nodes=node_type_counts(cursor, db),
                              total_nodes=query(cursor, total_nodes),
                              active_users=query(cursor, active_users),
                              recent_users=query(cursor, recent_users),
                              total_views=int(query(cursor, total_views)),
                              month_views=int(query(cursor, month_views)),
                              last_update=query(cursor, last_update),
                              dwca_output=query(cursor, dwca_output))
            print(site_stats)


def get_databases(cursor):
    scratch_dbs = []
    # Get a list of all available databases
    try:
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]

        for db in databases:
            # Connect to database
            cursor.execute(f"USE {db}")
            # check it's a scratchpad and add to new list if so
            cursor.execute("SHOW TABLES LIKE 'node_counter'")
            # Add scratchpads to scratch_dbs list
            scratch_dbs.append(db) if len(cursor.fetchall()) is not 0 else None

    except pymysql.Error as e:
        print(e)

    return scratch_dbs


def node_type_counts(cursor, db):
    node_counts = {}
    try:
        # Get node counts - total and breakdown per type
        sql = "SELECT type, COUNT(*) FROM node GROUP BY type"
        cursor.execute(sql)

        # Load node types and counts into dict and return
        for node_type in cursor.fetchall():
            node_counts[node_type[0]] = node_type[1]

        return node_counts

    except pymysql.Error as e:
        print(f"Error: {e} \nQuery: '{sql}' \nDatabase: {db}")


def query(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchone()[0]

    except pymysql.Error as e:
        print(f"Error: {e} \nQuery: '{sql}'")


# SQL statements
total_nodes = "SELECT COUNT(*) FROM node"
active_users = "SELECT COUNT(*) FROM users WHERE name <> 'Scratchpad Team' and login <> 0"
recent_users = "SELECT COUNT(*) FROM users WHERE FROM_UNIXTIME(login) > DATE(NOW()) + INTERVAL -1 MONTH"
total_views = "SELECT SUM(totalcount) FROM node_counter"
month_views = "SELECT SUM(totalcount) FROM node_counter WHERE FROM_UNIXTIME(timestamp) > DATE(NOW()) + INTERVAL -1 MONTH"
last_update = "SELECT MAX(FROM_UNIXTIME(changed, '%Y-%m-%d')) FROM node"
dwca_output = "SELECT COUNT(*) FROM system WHERE name IN ('dwca_export', 'dwcarchiver') AND status = 1"

# Get auth details + date
with open('server-permissions.txt', 'r') as f:
    keys = f.read().splitlines()
    host, user, password, database = keys


if __name__ == '__main__':
    load()
