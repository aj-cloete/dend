import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

def _clean(s):
    """
    Removes any quotes from s (string)
    returns cleaned string
    """
    try:
        return s.replace('"','').replace("'","")
    except AttributeError:
        return s

def _credentials(clean=True):
    """
    Get AWS credentials from environment (default) or from config file
    clean(bool): remove the AWS credentials from dl.cfg
    """
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    if not _clean(config['AWS']['AWS_ACCESS_KEY_ID']) or not _clean(config['AWS']['AWS_SECRET_ACCESS_KEY']):
        print('AWS config not stored in dl.cfg - getting it from the environment (AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY)')
        awsKey = _clean(os.environ.get('AWS_ACCESS_KEY_ID'))
        awsSecret = _clean(os.environ.get('AWS_SECRET_ACCESS_KEY'))
        config.set('AWS','AWS_ACCESS_KEY_ID',awsKey)
        config.set('AWS','AWS_SECRET_ACCESS_KEY',awsSecret)
        
        if not awsKey or not awsSecret:
            m = 'AWS credentials not found in environment nor dl.cfg.  Cannot continue.'
            m += '\nPlease provide values for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in either of:'
            m += '\n(1) environment variables or (2) under the [AWS] section in dl.cfg'
            raise Exception(m)
    else:
        awsKey = _clean(config['AWS']['AWS_ACCESS_KEY_ID'])
        awsSecret = _clean(config['AWS']['AWS_SECRET_ACCESS_KEY'])
        os.environ['AWS_ACCESS_KEY_ID'] = awsKey
        os.environ['AWS_SECRET_ACCESS_KEY'] = awsSecret
    
    if clean:
        config.set('AWS','AWS_ACCESS_KEY_ID','')
        config.set('AWS','AWS_SECRET_ACCESS_KEY','')
        
    with open('dl.cfg','w') as f:
            config.write(f)
            

_credentials(clean=True)


def create_spark_session():
    """
    Create and return a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song_data file using spark
    spark(spark session object): The spark session to use
    input_data(string): the location of the input data (local address or s3 allowed)
    output_data(string): the desired location of the output data (local address or s3 allowed)
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = ''
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = ''

    # extract columns to create artists table
    artists_table = '' 
    
    # write artists table to parquet files
    artists_table = ''


def process_log_data(spark, input_data, output_data):
    """
    Process the log_data file using spark
    spark(spark session object): The spark session to use
    input_data(string): the location of the input data (local address or s3 allowed)
    output_data(string): the desired location of the output data (local address or s3 allowed)
    """
    # get filepath to log data file
    log_data = 'log-data/*.json'

    # read log data file
    df = ''
    
    # filter by actions for song plays
    df = ''

    # extract columns for users table    
    artists_table = ''
    
    # write users table to parquet files
    artists_table = ''

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = ''
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = ''
    
    # extract columns to create time table
    time_table = ''
    
    # write time table to parquet files partitioned by year and month
    time_table = ''

    # read in song data to use for songplays table
    song_df = ''

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ''

    # write songplays table to parquet files partitioned by year and month
    songplays_table = ''


def main():
    """
    Run the ETL to process the song_data and the log_data files
    """
    spark = create_spark_session()
    input_data = 'data/' #"s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
