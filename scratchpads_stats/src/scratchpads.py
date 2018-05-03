import pymysql
import datetime
import os
import json


def load():
    if os.path.exists("scratch_stats.json"):
        with open("scratch_stats.json", 'r') as archive_file:
            scratch_stats = json.load(archive_file)
    else:
        scratch_stats = {}

    # Date last run
    scratch_stats['last_run'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    # Make bucket for current month (assuming it's run near the end of the month...)
    scratch_stats[datetime.datetime.now().strftime("%Y-%m")] = {}

    with pymysql.connect(host=host, user=user, password=password) as cursor:
        scratch_dbs = get_databases(cursor)

        for db in scratch_dbs:
            cursor.execute(f"USE {db}")
            site_stats = dict(name=db,
                              nodes=query(cursor, nodes),
                              total_nodes=query(cursor, total_nodes),
                              active_users=query(cursor, active_users),
                              recent_users=query(cursor, recent_users),
                              total_views=int(query(cursor, total_views or 0)),
                              month_views=int(query(cursor, month_views) or 0),
                              last_update=query(cursor, last_update),
                              dwca_output=query(cursor, dwca_output),
                              created=query(cursor, created))

            # Add to month list of site stats
            scratch_stats[datetime.datetime.now().strftime("%Y-%m")][site_stats['name']] = site_stats

        with open('scratch_stats.json', 'w') as outfile:
            json.dump(scratch_stats, outfile)


# Get a list of all available databases and filter out the non-scratchpads
def get_databases(cursor):
    scratch_dbs = []
    try:
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]
        for db in databases:
            cursor.execute(f"USE {db}")
            # presence of node_counter indicates scratchpad db
            scratch_dbs.append(db) if cursor.execute("SHOW TABLES LIKE 'node_counter'") is not 0 else None
        return scratch_dbs

    except pymysql.Error as e:
        print(e)


# Runs each query and returns single/list value
def query(cursor, sql):
    try:
        return cursor.fetchone()[0] if cursor.execute(sql) == 1 else dict(cursor.fetchall())
    except pymysql.Error as e:
        print(f"Error: {e} \nQuery: '{sql}'")


# SQL statements
nodes = "SELECT type, COUNT(*) FROM node GROUP BY type"
total_nodes = "SELECT COUNT(*) FROM node"
active_users = "SELECT COUNT(*) FROM users WHERE name <> 'Scratchpad Team' and login <> 0"
recent_users = "SELECT COUNT(*) FROM users WHERE FROM_UNIXTIME(login) > DATE(NOW()) + INTERVAL -1 MONTH"
total_views = "SELECT SUM(totalcount) FROM node_counter"
month_views = "SELECT SUM(totalcount) FROM node_counter WHERE FROM_UNIXTIME(timestamp) > DATE(NOW()) + INTERVAL -1 MONTH"
last_update = "SELECT MAX(FROM_UNIXTIME(changed, '%Y-%m-%d')) FROM node"
dwca_output = "SELECT COUNT(*) FROM system WHERE name IN ('dwca_export', 'dwcarchiver') AND status = 1"
created = "SELECT MIN(FROM_UNIXTIME(created, '%Y-%m-%d')) FROM node;"

# Get auth details
with open('server-permissions.txt', 'r') as f:
    keys = f.read().splitlines()
    host, user, password = keys

if __name__ == '__main__':
    load()
