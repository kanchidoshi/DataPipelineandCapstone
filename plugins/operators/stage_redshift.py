from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_statement  = """
                    copy {}
                    from {}
                    aws_access_key_id {}
                    aws_secret_access_key {}
                    region {}
                    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook=AwsHook(self.aws_credentials)
        aws_credentials=aws_hook.get_credentials()
        redshift=PostgresHook(self.redshift_conn_id)
        
        self.log.info("Cleaning the data from table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Data copy from S3 to Redshift")
        #rendered_key=self.s3_key.format(**context)
        s3_path="s3://{}/{}".format(self.s3_bucket, self.s3_key)
                                    #rendered_key)
        sql_stmt=StageToRedshiftOperator.sql_statement.format (self.table,
                                                       s3_path, 
                                                       aws_credentials.access_key, 
                                                       aws_credentials.secret_key)

        redshift.run(sql_stmt)





