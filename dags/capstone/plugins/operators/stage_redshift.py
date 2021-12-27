from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
#from airflow.hooks.S3_hook import S3Hook

class StageToRedshiftOperator1(BaseOperator):
    ui_color = '#358140'
    sql_statement  = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION '{}'
                    PARQUET '{}'
                   """
    # templated_fields = ("s3_key", )
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 parquet_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator1, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.parquet_path = parquet_path
        self.file_type = file_type
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info("Get Credentials from Airflow connection")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        #self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=False)
        #aws_credentials = self.s3.get_credentials()
        redshift=PostgresHook(self.redshift_conn_id)
        
        self.log.info("Cleaning the data from table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Data copy from S3 to Redshift")
        rendered_key=self.s3_key.format(**context)
        s3_path="s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("s3_path = ", s3_path)                          
        sql_stmt=StageToRedshiftOperator.sql_statement.format (self.table,
                                                               s3_path, 
                                                               credentials.access_key, 
                                                               credentials.secret_key,
                                                               self.region,
                                                               self.parquet_path
                                                              )

        redshift.run(sql_stmt)
        self.log.info("Copy operation complete")




