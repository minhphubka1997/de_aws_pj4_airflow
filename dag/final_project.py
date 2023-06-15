from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


s3_bucket = "udacity-minfu-sparksong"
events_s3_key = "log-data"
songs_s3_key = "song-data/A"
log_json_file = 'log_json_path.json'


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    drop_redshift_tables1 = PostgresOperator(
        task_id='drop_tables_staging_events',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_staging_events
    )

    create_redshift_tables1 = PostgresOperator(
        task_id='Create_tables_staging_events',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_staging_events
    )

    drop_redshift_tables2 = PostgresOperator(
        task_id='drop_tables_staging_songs',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_staging_songs
    )

    create_redshift_tables2 = PostgresOperator(
        task_id='Create_tables_staging_songs',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_staging_songs
    )

    drop_redshift_tables3 = PostgresOperator(
        task_id='drop_tables_songplays',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_songplays
    )

    create_redshift_tables3 = PostgresOperator(
        task_id='Create_tables_songplays',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_songplays
    )

    drop_redshift_tables4 = PostgresOperator(
        task_id='drop_tables_users',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_users
    )

    create_redshift_tables4 = PostgresOperator(
        task_id='Create_tables_users',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_users
    )

    drop_redshift_tables5 = PostgresOperator(
        task_id='drop_tables_songs',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_songs
    )

    create_redshift_tables5 = PostgresOperator(
        task_id='Create_tables_songs',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_songs
    )

    drop_redshift_tables6 = PostgresOperator(
        task_id='drop_tables_artists',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_artists
    )

    create_redshift_tables6 = PostgresOperator(
        task_id='Create_tables_artists',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_artists
    )

    drop_redshift_tables7 = PostgresOperator(
        task_id='drop_tables_time',
        postgres_conn_id="redshift",
        sql= SqlQueries.drop_table_time
    )

    create_redshift_tables7 = PostgresOperator(
        task_id='Create_tables_time',
        postgres_conn_id="redshift",
        sql= SqlQueries.Create_table_time
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=events_s3_key,
        sql=SqlQueries.Create_table_staging_events
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=songs_s3_key,
        sql=SqlQueries.Create_table_staging_songs
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        sql = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id = "redshift",
        table_name = ["staging_events","staging_songs","songplays", "users", "songs", "artists", "time"]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> \
    drop_redshift_tables1 >> create_redshift_tables1 >> \
    drop_redshift_tables2 >> create_redshift_tables2 >> \
    drop_redshift_tables3 >> create_redshift_tables3 >> \
    drop_redshift_tables4 >> create_redshift_tables4 >> \
    drop_redshift_tables5 >> create_redshift_tables5 >> \
    drop_redshift_tables6 >> create_redshift_tables6 >> \
    drop_redshift_tables7 >> create_redshift_tables7 >> \
    stage_events_to_redshift >> stage_songs_to_redshift >> \
    load_songplays_table >> \
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> \
    end_operator
    

final_project_dag = final_project()