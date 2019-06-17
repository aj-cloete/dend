import configparser
import psycopg2
import infrastructure_as_code as iac
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    '''Drops all the tables for a fresh reconstruction of the database
    The SQL scripts are imported from sql_queries.py
    cur: cursor object associated with the connection
    conn: psycopg2.connection object with access to the database
    '''
    for query in drop_table_queries:
        print('Running statements starting with: ')
        print('\n'.join(query.split('\n')[1:3]))
        cur.execute(query)
        conn.commit()
        print()

def create_tables(cur, conn):
    '''Creates all the tables for a fresh reconstruction of the database
    The SQL scripts are imported from sql_queries.py
    cur: cursor object associated with the connection
    conn: psycopg2.connection object with access to the database
    '''
    for query in create_table_queries:
        print('Running statements starting with: ')
        print('\n'.join(query.split('\n')[1:3]))
        cur.execute(query)
        conn.commit()
        print()

def main():
    '''Runs the entire delete and create process afresh
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn_string = "host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}"
    
    try:
        conn = psycopg2.connect(conn_string.format(**config['CLUSTER']))
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        iac.create_redshift_cluster()
        config.read('dwh.cfg')
        conn = psycopg2.connect(conn_string.format(**config['CLUSTER']))
        cur = conn.cursor()
        
    print('Dropping tables')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    
    print('Create Tables completed')

if __name__ == "__main__":
    main()