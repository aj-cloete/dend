import os
import glob
import pandas as pd
from cassandra.cluster import Cluster
from sql_queries import *


# Helper funtions

def get_all_data(filenames):
    """
    Read all the data from all the files
    filenames = list of filenames from which to read the data
    Returns: pandas df containing all the unfiltered data including the source column
    """
    dfs = []
    for fn in filenames:
        df = pd.read_csv(fn)
        df['source'] = fn
        dfs.append(df)
    data = pd.concat(dfs)
    return data

def insert_data(df, target_table, cols, session):
    """
    Use the df to insert data into the target table using the provided session
    df (pandas DataFrame): rows must be filtered (where applicable)
    target_table (string): name of table into which data must be inserted
    cols (list): a list of the columns (in order) matching the target_table
    session (cassandra.cluster.Session): Cassandra session to be used - keyspace must be set
    """
    print(f'Inserting data into {target_table}')
    d = df[cols].copy()
    query = f"INSERT INTO {target_table} ({', '.join(cols)})"
    query = query + f" VALUES ({', '.join(['%s']*len(cols))})"
    try:
        for row in d.values:
            session.execute(query,row.tolist())
    except Exception as e:
        print(e)
    print(f'Data inserted into {target_table}\n')

def drop_table(table_name, session):
    """
    Drop the table from the database if it exists
    table_name (string): name of the table to be dropped
    session (cassandra.cluster.Session): Cassandra session to be used - keyspace must be set
    """
    try:
        session.execute(f'DROP TABLE IF EXISTS {table_name}')
        return f'{table_name} dropped!'
    except Exception as e:
        print(e)
        return f'{table_name} not dropped!'
        

def drop_tables(table_list, session):
    """
    Drop multiple tables from the keyspace
    table_list (list): A list of table names (string) to drop.
    session (cassandra.cluster.Session): Cassandra session to be used - keyspace must be set
    """
    ret = []
    if isinstance(table_list, str):
        ret.append(drop_table(table_list, session))
        
    else:
        for table in table_list:
            ret.append(drop_table(table, session))
    return ret

# Project steps

def preprocess_data():
    """
    Run the preprocessing steps
    Return: all_data and data pandas dfs.  The data df is the preprocessed data
    """
    print("Running preprocessing steps")
    # Get the data directory
    data_dir = os.path.join(os.getcwd(),'event_data')

    # Get all the filenames to be processed
    filenames = glob.glob(os.path.join(data_dir,'*'), recursive=True)

    # get all the data in memory
    all_data = get_all_data(filenames)

    # filter all the data to generate the events dataset where the artist is not null
    cols = ['artist','firstName','gender','itemInSession','lastName',
            'length','level','location','sessionId','song','userId']
    data = all_data[all_data.artist.notna()][cols].copy()
    data['userId'] = data['userId'].astype(int)

    # Backup the filtered data to a file in the current working directory
    data.to_csv('event_datafile_new.csv', index=False)
    print("Completed preprocessing")
    return all_data, data

def set_up_database():
    """
    Set up the database
    Return: cluster, session for Cassandra
    """
    print('Setting up the database')
    
    # Establish connection to Cassandra
    cluster = Cluster()
    session = cluster.connect()

    # Create the keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkifydb 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    except Exception as e:
        print(e)

    # set the keyspace on the session
    try:
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)
    print('Completed setting up')
    return cluster, session

def create_table(table_name, create_statement, session):
    """
    Create the table from the create_statement using the session provided
    table_name (string): name of table to be created
    create_statement (string): the create statement for the table
    session (cassandra.cluster.Session): Cassandra session to be used - keyspace must be set
    """
    print(f'Creating table {table_name} using statement\n{create_statement}')
    drop_table(table_name, session)
    try:
        session.execute(create_statement)
    except Exception as e:
        print(f'Error in creating {table_name}!')
        print(e)
    print(f'Creation of table {table_name} successful\n')


# Run the main programme if run from shell    

if __name__=='__main__':
    all_data, data = preprocess_data()
    cluster, session = set_up_database()
    
    create_table('song_play_session_items', create_song_play_session_items, session)
    create_table('artist_song_session_plays', create_artist_song_session_plays, session)
    create_table('user_song_plays', create_user_song_plays, session)
    
    insert_data(data, target_table='song_play_session_items', session=session,
                cols=['sessionId','itemInSession','artist','song','length'])
    insert_data(data, target_table='artist_song_session_plays', session=session,
                cols=['sessionId','userId','itemInSession','artist','song','firstName','lastName'])
    insert_data(data, target_table='user_song_plays', session=session,
                cols=['song', 'userId', 'firstName', 'lastName'])

    # Conclude
    print('='*50)
    print('The tables have been created and the data inserted')
    print('='*50)
