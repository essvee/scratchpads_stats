import pymysql
import json
import collections

def main():
    results = []
    types = {"biblio", "ecological_interactions", "location", "page", "specimen_observation", "spm"}

    with get_cursor('server-permissions.txt') as cursor:
        results = []
        scratchpads = get_databases(cursor)
        for db in scratchpads:
            if db == "scratchpadseu":
                continue
            db_name = "USE " + db
            cursor.execute(db_name)

            cursor.execute("""SELECT type, DATE_FORMAT(FROM_UNIXTIME(created), '%m-%Y') FROM node WHERE status = 1;""")
            node_results = cursor.fetchall()
            for n in node_results:
                node_type, created = n
                if node_type in types:
                    results.append(node_type + " " + created)
                else:
                    results.append("other" + " " + created)

    counter = collections.Counter(results)

    with open('scratch_stats_node_counter.json', 'w') as outfile:
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
        return scratch_dbs

    except pymysql.Error as e:
        print(e)


if __name__ == '__main__':
    main()
