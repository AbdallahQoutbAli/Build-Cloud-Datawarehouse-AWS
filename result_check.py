import configparser
import psycopg2
from sql_queries import get_counts_queries


def get_counts(cur, conn):
    """ Get Total Counts in Each DWH Table"""
    for query in get_counts_queries:
        cur.execute(query)
        results = cur.fetchall()
        for row in results:
            print(row)





def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Get Counts Funcation ")
    get_counts(cur, conn)
 

    conn.close()


if __name__ == "__main__":
    main()