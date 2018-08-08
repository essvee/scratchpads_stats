import pymysql
import json

def main():

    results = []

    with get_cursor('server-permissions.txt') as cursor:
        results = []
        scratchpads = get_databases(cursor)
        for db in scratchpads:
            if db == "scratchpadseu":
                continue
            db_name = "USE " + db
            cursor.execute(db_name)

            cursor.execute("""SELECT nid, type, status, FROM_UNIXTIME(created, '%Y-%m-%d'), FROM_UNIXTIME(changed, '%Y-%m-%d') FROM node""")
            node_results = cursor.fetchall()
            for n in node_results:
                nid, type, status, created, changed = n
                node_list = [db, nid, type, status, created, changed]
                results.append(node_list)

    with open('scratch_stats_nodes.json', 'w') as outfile:
        json.dump(results, outfile)


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
