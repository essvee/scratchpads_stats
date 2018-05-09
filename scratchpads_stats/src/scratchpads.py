import pymysql
import datetime
import os
import json


def load():
    if os.path.exists("scratch_stats_sites.json"):
        with open("scratch_stats_sites.json", 'r') as archive_file:
            scratch_stats = json.load(archive_file)
    else:
        scratch_stats = {'sites': {}}

    # Date last run
    scratch_stats['last_run'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    with pymysql.connect(host=host, user=user, password=password) as cursor:
        scratch_dbs = get_databases(cursor)

        for db in scratch_dbs:
            cursor.execute(f"USE {db}")

            # Add key for new site + populate with header data
            if db not in scratch_stats['sites']:
                scratch_stats['sites'][db] = {'name': db, 'created': query(cursor, created),
                                              'updated': "", 'total_views': 0, 'dwca': "", 'results': {}}

            # Update dynamic header stats
            scratch_stats['sites'][db]['updated'] = query(cursor, last_update)
            scratch_stats['sites'][db]['total_views'] = int(query(cursor, total_views) or 0)
            scratch_stats['sites'][db]['dwca'] = query(cursor, dwca_output)

            # Get monthly stats
            site_stats = dict(nodes=query(cursor, nodes),
                              total_nodes=query(cursor, total_nodes),
                              new_nodes=query(cursor, new_nodes) or 0,
                              active_users=query(cursor, active_users),
                              recent_users=query(cursor, recent_users),
                              month_views=int(query(cursor, month_views) or 0))

            # Stash in current month bucket
            scratch_stats['sites'][db]['results'][datetime.datetime.now().strftime("%Y-%m")] = site_stats

        with open('scratch_stats_sites.json', 'w') as outfile:
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
nodes = "SELECT type, COUNT(*) FROM node WHERE status = 1 GROUP BY type"
total_nodes = "SELECT COUNT(*) FROM node WHERE status = 1"
new_nodes = "SELECT COUNT(*) FROM node WHERE FROM_UNIXTIME(created) > DATE(NOW()) + INTERVAL -1 MONTH"
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
