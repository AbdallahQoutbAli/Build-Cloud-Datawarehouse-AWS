import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


  
""" 
Loading data From S3 to Stage Tables inside Redshift
 Data will be load in tow tables (staging_events) ,(staging_songs)
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
""" 
Loading data From Stg Tables  to Data   Warehouse  Tables inside Redshift
Data will be load in 5 as show below : 
1-songplay
2-user_table
3-song_table
4-artist
5-time
"""
def insert_tables(cur, conn):
   
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading Stage Tables')

    load_staging_tables(cur, conn)
    
    print('insert into  DWH Tables')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()