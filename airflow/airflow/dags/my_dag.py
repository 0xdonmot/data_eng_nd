from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                              PostgresOperator, PythonOperator)
from helpers import SqlQueries


# define default arguments
default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

# define DAG
with DAG('sparkify',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        ) as dag:

    # specify operators
    start_operator = DummyOperator(task_id='Begin_execution')


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        json_path='log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        destination_table='songplays',
        insert_sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        destination_table='users',
        insert_sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        destination_table='songs',
        insert_sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        destination_table='artists',
        insert_sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        destination_table='time',
        insert_sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time'],
        min_rows=1
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    # define dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                             load_artist_dimension_table, load_time_dimension_table]

    [load_user_dimension_table, load_song_dimension_table,
    load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator