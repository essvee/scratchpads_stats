import collections

import pymysql
import json


def main():

    with get_cursor('server-permissions.txt') as cursor:
        # todo write count of scratch pads to json
        scratchpads = get_databases(cursor)
        count_nodes(cursor, scratchpads)
        get_emails(cursor, scratchpads)


def get_emails(cursor, db_list):
    results_email = []

    for db in db_list:
        if db == "scratchpadseu" or db == "cephbaseeolorg" or db == "statsscratchpads" or db == "vbranteu":
            continue

        email_sql = "SELECT mail FROM %s.users" \
                    " WHERE mail <> '' AND mail <> 'scratchpad@nhm.ac.uk' AND login <> 0;" % db

        cursor.execute(email_sql)
        emails = cursor.fetchall()
        result_list = []

        for n in emails:
            result_list.append((db, n[0]))

        results_email.extend(result_list)

    with open('scratch_stats_email.json', 'w') as outfile:
        json.dump(results_email, outfile)


def count_nodes(cursor, db_list):
    types = {"biblio", "ecological_interactions", "location", "page", "specimen_observation", "spm"}
    results = []

    for db in db_list:
        if db == "scratchpadseu" or db == "cephbaseeolorg" or db == "statsscratchpads" or db == "vbranteu":
            continue

        database_sql = "SELECT type, DATE_FORMAT(FROM_UNIXTIME(created), '%%Y-%%m') " \
                       "FROM %s.node WHERE status = 1;" % db

        cursor.execute(database_sql)
        node_results = cursor.fetchall()

        for n in node_results:
            node_type, created = n
            if node_type in types:
                results.append(node_type + " " + created)
            else:
                results.append("other" + " " + created)

    counter = collections.Counter(results)

    with open('scratch_stats_nodes_1.json', 'w') as outfile:
        json.dump(counter, outfile)


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

        print(scratch_dbs)
        return scratch_dbs

    except pymysql.Error as e:
        print(e)


if __name__ == '__main__':
    main()
