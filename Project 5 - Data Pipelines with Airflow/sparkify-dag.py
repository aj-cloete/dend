from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'aj-cloete',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12, 0, 0, 0, 0),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG('sparkify-dag',
          default_args=default_args,
          description='Load and transform Sparkify data in Redshift with Airflow',
          schedule_interval='@hourly'
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')
    
    # Read the create_tables.sql helper for use in task
    with open('/home/workspace/airflow/create_tables.sql') as f:
        create_tables_query = f.read()
    create_tables_task = PostgresOperator(
        task_id='Create_tables',
        sql = create_tables_query,
        postgres_conn_id='redshift'
    )  

    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_key="log_data",
        json_option='s3://udacity-dend/log_json_path.json',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_key='song_data',
        json_option='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = DummyOperator(task_id='Stop_execution')

# Collect all loads into a list
load_dims = [load_user_dimension_table,
             load_song_dimension_table,
             load_artist_dimension_table,
             load_time_dimension_table] 

start_operator \
    >> create_tables_task \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> load_dims \
    >> run_quality_checks \
    >> end_operator

