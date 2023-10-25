import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    '''
    Executes the queries for droping the staging and analytical tables if any exists.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print ("Dropped Tables Successfully")


def create_tables(cur, conn):
    '''
    Executes the queries for creating the staging and analytical tables.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print ("Created Tables Successfully")


def main():
    '''
    Reads the configuration file for the database connection details.
    Connects to the database in the redshift cluster.
    Executes the drop and create table statements for staging and analytical tables.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()