import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    '''
    Executes the queries that copies data from S3 bucket in to staging tables.
    '''
    print("Copy to Staging tables started")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Copy to Staging tables completed") 

def insert_tables(cur, conn):
    '''
    Executes the queries that inserts data from staging in to analytical tables.
    '''
    print("Insert into Dimensional Tables started")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Insert into Dimensional tables completed")

def main():
    '''
    Reads the Configuration file for the database details.
    Connects to the database and executes the Copy and insert queries for the staging and analytical tables.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_DB_HOST            = config.get("DWH","DWH_DB_HOST")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_DB_PORT")
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_DB_HOST,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
   

    conn.close()


if __name__ == "__main__":
    main()