import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drop Tables IF EXISTS"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Create Stage Tables : (staging_events) ,(staging_songs) 
        Create DWH   Tables : 
         1-songplay
         2-user_table
         3-song_table
         4-artist
         5-time
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Drop table Tables IF EXISTS ")
    drop_tables(cur, conn)
    
    print ("Create STG AND DWH Tables")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()