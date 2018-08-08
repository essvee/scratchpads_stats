import pymysql
import json


def load():
    scratch_stats = {}

    with pymysql.connect(host=host, user=user, password=password) as cursor:
        # Get list of scratchpads dbs on server
        scratch_dbs = get_databases(cursor)

        for db in scratch_dbs:
            if db == "scratchpadseu":
                continue
            db_name = "USE " + db
            cursor.execute(db_name)

            # Add key for new site + populate with header data
            if db not in scratch_stats:
                scratch_stats[db] = {'created': query(cursor, created),
                                     'updated': query(cursor, last_update),
                                     'total_views': int(query(cursor, total_views) or 0),
                                     'month_views': int(query(cursor, month_views) or 0),
                                     'dwca': query(cursor, dwca_output),
                                     'node_count': query(cursor, total_nodes),
                                     'newest_node': query(cursor, newest_node),
                                     'active_users': query(cursor, active_users),
                                     'recent_users': query(cursor, recent_users)
                                     }

        with open('scratch_stats_snapshot.json', 'w') as outfile:
            json.dump(scratch_stats, outfile)


# Get a list of all available databases and filter out the non-scratchpads
def get_databases(cursor):
    scratch_dbs = []
    try:
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]
        for db in databases:

            db_name = "USE " + db
            cursor.execute(db_name)
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
        error_message = "Error: " + str(e) + ". " + sql
        print(error_message)


# SQL statements
nodes = "SELECT type, COUNT(*) FROM node WHERE status = 1 GROUP BY type"
total_nodes = "SELECT COUNT(*) FROM node WHERE status = 1"
newest_node = "SELECT MAX(FROM_UNIXTIME(created, '%Y-%m-%d')) FROM node;"
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
