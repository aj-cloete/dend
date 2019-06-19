import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions as F, Window, types as T

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
    spark = SparkSession.builder.getOrCreate()
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song_data file using spark
    spark(spark session object): The spark session to use
    input_data(string): the location of the input data
    output_data(string): the desired location of the output data
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/'+'songs_table.parquet',
                              partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']) \
                    .withColumnRenamed('artist_name','artist') \
                    .withColumnRenamed('artist_location','location') \
                    .withColumnRenamed('artist_longitude','longitude') \
                    .withColumnRenamed('artist_latitude','latitude').distinct()

    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists/'+'artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    """
    Process the log_data file using spark
    spark(spark session object): The spark session to use
    input_data(string): the location of the input data
    output_data(string): the desired location of the output data
    """
    # get filepath to log data file
    log_data = input_data+'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where('page="NextSong"')

    # extract columns for users table    
    w_user_ts = Window.partitionBy('userId').orderBy(F.desc('ts'))
    users_table = df.withColumn('rn',F.row_number().over(w_user_ts)) \
                    .filter('rn=1') \
                    .withColumnRenamed('userId','user_id') \
                    .withColumnRenamed('firstName','first_name') \
                    .withColumnRenamed('lastName','last_name') \
                    .select(['user_id','first_name','last_name','gender','level'])

    # write users table to parquet files
    # users_table.write.parquet(output_data+'users/'+'users.parquet')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', (F.col('ts')/1000).cast(dataType=T.TimestampType()))

    # extract columns to create time table
    time_cols = ['hour','day','week','month','year']
    exprs = ['timestamp as start_time']+\
            ['extract({col} from timestamp) as {col}'.format(col=col) for col in time_cols]
    weekDay = F.udf(lambda x: x.strftime('%w'))
    time_table = df.selectExpr(*exprs).withColumn('weekday',weekDay('start_time')).distinct()

    # write time table to parquet files partitioned by year and month
    # time_table.write.parquet(output_data+'time/'+'time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    joined = df.join(song_df,(df.artist==song_df.artist_name)&(df.song==song_df.title),
                     how='inner')
    songplay_selects = ['songplay_id',
                        'timestamp as start_time',
                        'extract(month from timestamp) as month',
                        'extract(year from timestamp) as year',
                        'userId as user_id',
                        'level','song_id','artist_id',
                        'sessionId as session_id',
                        'location',
                        'userAgent as user_agent']
    songplays_table = joined.distinct().withColumn('songplay_id', F.monotonically_increasing_id()) \
                        .selectExpr(*songplay_selects)

    # write songplays table to parquet files partitioned by year and month
    # songplays_table.write.parquet(output_data+'songplays/'+'songplays.parquet', partitionBy=['year','month'])


def main():
    """
    Run the ETL to process the song_data and the log_data files
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "todo"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
