from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 expected_expression="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.expected_expression = expected_expression

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql)
        
        try:
            num_records = records[0][0]
        except IndexError:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
            
        if not self.expected_expression(num_records):
            raise ValueError(f"Data quality check failed. {self.table} contained a result different than expected")
        self.log.info(f"Data quality on table {self.table} check passed with {num_records} records")
