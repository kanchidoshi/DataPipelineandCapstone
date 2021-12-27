from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator1, LoadFactOperator1,
                                LoadDimensionOperator1, DataQualityOperator1)
from helpers import SqlQueries1

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 12, 12),
    'max_active_runs': 1,
    'depends_on_past':False,
    'email_on_retry':False,
    #'retries':3,
    'catchup': False
    #'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          #schedule_interval='@daily'
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_demographics_to_redshift = StageToRedshiftOperator1(
    task_id='Stage_demographics',
    dag=dag,
    table="staging_demographics",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    #aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend-capstone-kd",
    s3_key="Data_model",
    region="us-west-2",
    parquet_path="s3a://udacity-dend-capstone-kd/dim_demographics/",
    file_type="parquet",
    provide_context=True
)

stage_airports_to_redshift = StageToRedshiftOperator1(
    task_id='Stage_airports',
    dag=dag,
    table="staging_airports",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    #aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend-capstone-kd",
    #s3_key="song_data",
    s3_key='Data_model',
    region="us-west-2",
    parquet_path="s3a://udacity-dend-capstone-kd/dim_airports/",
    #parquet_path="auto",
    file_type="parquet",
    provide_context=True
)

stage_time_to_redshift = StageToRedshiftOperator1(
    task_id='Stage_time',
    dag=dag,
    table="staging_time",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    #aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend-capstone-kd",
    s3_key="Data_model",
    region="us-west-2",
    parquet_path="s3a://udacity-dend-capstone-kd/dim_time_table/",
    file_type="parquet",
    provide_context=True
)

stage_immig_to_redshift = StageToRedshiftOperator1(
    task_id='Stage_immig',
    dag=dag,
    table="staging_immig",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    #aws_conn_id="aws_credentials",
    s3_bucket="udacity-dend-capstone-kd",
    s3_key="Data_model",
    region="us-west-2",
    parquet_path="s3a://udacity-dend-capstone-kd/fact_immigration_table/",
    file_type="parquet",
    provide_context=True
)

load_immig_table = LoadFactOperator1(
    task_id='Load_immig_fact_table',
    dag=dag,
    table="immigration",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.immig_table_insert
)

load_demog_dimension_table = LoadDimensionOperator1(
    task_id='Load_demog_dim_table',
    dag=dag,
    table="demographics",
    redshift_conn_id='redshift',
    truncate_table=True,
    load_sql_stmt=SqlQueries.demog_table_insert
)

load_airports_dimension_table = LoadDimensionOperator1(
    task_id='Load_airports_dim_table',
    dag=dag,
    table='dim_airports',
    redshift_conn_id='redshift',
    truncate_table=True,
    load_sql_stmt=SqlQueries.airports_table_insert
)

load_time_dimension_table = LoadDimensionOperator1(
    task_id='Load_time_dim_table',
    dag=dag,
    table='dim_time_table',
    redshift_conn_id='redshift',
    truncate_table=True,
    load_sql_stmt=SqlQueries.time_table_insert
)

load_time_dimension_table = LoadDimensionOperator1(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    truncate_table=True,
    load_sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator1(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.fact_immigration WHERE cicid  IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "visatype") FROM public.fact_immigration', 'expected_result': 17 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.dim_time_table', 'expected_result': 192 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.dim_demographics', 'expected_result': 2891 },
       
    ],
    #tables = ['songplays', 'users', 'songs', 'artists', 'time'],
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_demographics_to_redshift
start_operator >> stage_airports_to_redshift
start_operator >> stage_time_to_redshift
start_operator >> stage_immig_to_redshift

stage_demographics_to_redshift >> load_immig_table
stage_airports_to_redshift >> load_immig_table
stage_time_to_redshift >> load_immig_table
stage_airports_to_redshift >> load_immig_table

load_immig_table >> load_demog_dimension_table       
load_immig_table >> load_artist_dimension_table
load_immig_table >> load_time_dimension_table

load_demog_dimension_table >> run_quality_checks       
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks        

run_quality_checks >>  end_operator   
