import collections
from datetime import date, datetime

import pymysql
import json
import platform


def main():
    with get_cursor('server-permissions.txt') as cursor:
        # todo write count of scratch pads to json
        scratchpads = get_databases(cursor)
        nodes_emails_views(cursor, scratchpads)

def nodes_emails_views(cursor, db_list):
    server_name = platform.node()
    types = {"biblio", "ecological_interactions", "location", "page", "specimen_observation", "spm"}
    results_node = []
    results_email = []
    results_views = []
    results_users = []
    month = date.today().strftime('%Y-%m')

    for db in db_list:

        if db == "scratchpadseu" or db == "cephbaseeolorg" or db == "statsscratchpads" or db == "vbranteu":
            continue

        # SQL queries
        node_sql = "SELECT type, DATE_FORMAT(FROM_UNIXTIME(created), '%%Y-%%m') " \
                   "FROM %s.node WHERE status = 1;" % db
        email_sql = "SELECT mail FROM %s.users" \
                    " WHERE mail <> '' AND mail <> 'scratchpad@nhm.ac.uk' AND login <> 0;" % db
        views_sql = "SELECT SUM(totalcount), FROM_UNIXTIME(timestamp, '%%Y-%%m') " \
                    "FROM %s.node_counter GROUP BY FROM_UNIXTIME(timestamp, '%%Y-%%m')" % db
        user_sql = "SELECT COUNT(*) FROM %s.users WHERE name <> 'Scratchpad Team' and login <> 0" % db

        nodes = query(cursor, node_sql)
        emails = query(cursor, email_sql)
        views = query(cursor, views_sql)
        users = query(cursor, user_sql)

        for e in emails:
            results_email.append((db, e[0]))

        for v in views:
            count, month = v
            results_views.append((db, int(count), month))

        for n in nodes:
            node_type, created = n
            if node_type in types:
                results_node.append(node_type + " " + created)
            else:
                results_node.append("other" + " " + created)

        for u in users:
            results_users.append((month, db, u[0]))

    counter = collections.Counter(results_node)

    with open(f"scratch_stats_nodes_{server_name}.json", 'w') as outfile:
        json.dump(counter, outfile)

    with open(f"scratch_stats_email_{server_name}.json", 'w') as outfile:
        json.dump(results_email, outfile)

    with open(f"scratch_stats_views_{server_name}.json", 'w') as outfile:
        json.dump(results_views, outfile)

    with open(f"scratch_stats_users_{server_name}.json", 'w') as outfile:
        json.dump(results_users, outfile)



def auth(filename):
    # Get auth details
    with open(filename, 'r') as f:
        keys = f.read().splitlines()
    return keys


def get_cursor(filename):
    host, user, password = auth(filename)
    return pymysql.connect(host=host, user=user, password=password)


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


def query(cursor, sql):
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    except pymysql.Error as e:
        error_message = "Error: " + str(e) + ". " + sql
        print(error_message)


if __name__ == '__main__':
    main()
