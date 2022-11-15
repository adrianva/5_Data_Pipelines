from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    :param redshift_conn_id: Redshift connection defined in Airflow
    :param sql: SQL query which is going to be used as the quality check
    :param table: Redshift table name where we are going run the sql query
    :param expected_expression: Function used to test the output of the sql query
    """

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
        """
        Runs the query and validates the output using the expected_expression. 
        If the result of running the expected expression against the output is true, it passes the test.
        
        :param context: Airflow context variables
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql)
        
        try:
            num_records = records[0][0]
        except IndexError:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
            
        if not self.expected_expression(num_records):
            raise ValueError(f"Data quality check failed. {self.table} contained a result different than expected")
        self.log.info(f"Data quality on table {self.table} check passed with {num_records} records")
