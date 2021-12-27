from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 # tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks=dq_checks
        #self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        if len(self.dq_checks) <= 0:
            self.log.info("No data quality checks provided")
            return
        
        redshift=PostgresHook(self.redshift_conn_id)
        error_count = 0
        failing_tests = []  
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info("All data quality checks passed")  