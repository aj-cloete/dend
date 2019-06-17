import configparser
import psycopg2
import infrastructure_as_code as iac
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Copy the staging tables from S3 buckets
    The SQL scripts are imported from sql_queries.py
    cur: cursor object associated with the connection
    conn: psycopg2.connection object with access to the database
    '''
    for query in copy_table_queries:
        print('Running statements starting with: ')
        print('\n'.join(query.split('\n')[1:3]))
        cur.execute(query)
        conn.commit()
        print()

def insert_tables(cur, conn):
    '''Insert the staging data into the Star Schema tables
    The SQL scripts are imported from sql_queries.py
    cur: cursor object associated with the connection
    conn: psycopg2.connection object with access to the database
    '''
    for query in insert_table_queries:
        print('Running statements starting with: ')
        print('\n'.join(query.split('\n')[1:3]))
        cur.execute(query)
        conn.commit()
        print()

def main():
    '''Run the entire etl process.
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
    
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    print('Inserting into tables tables')
    insert_tables(cur, conn)

    conn.close()
    
    print('ETL completed')
    
    if iac.load_credentials():
        print('The cluster and all of the data will be deleted unless you cancel this programme within 30 seconds')
        print('Pressing ctl+c or ctl+z will shut down the programme.')
        try:
            for i in range(30):
                iac.time.sleep(1)
            print('Programme not interrupted - commencing self-destruct')
            iac.cleanup_resources()
        except KeyboardInterrupt:
            print('\nSelf destruct cancelled.  The cluster remains up and the data is still available')
            print('If you did not intend to cancel and would like to delete the resources, run the following command')
            print('  python infrastructure_as_code.py clean')

if __name__ == "__main__":
    main()