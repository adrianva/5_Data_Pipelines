from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads data into some dimension table
    
    :param table: Redshift table name where we are going to load the data
    :param sql: SQL query for getting the data to load into the target table
    :param truncate: Whether truncate the table before load the data
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self, redshift_conn_id="", sql="", table="", truncate=True, *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        """
        Insert data into the facts table using some SQL query.
        If truncate is True, the table is truncated before inserting the data
        
        :param context: Context variables from Airflow
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            redshift.run(f'truncate {self.table}')

        self.log.info(f'Load dimension table {self.table}')
        redshift.run(f'insert into {self.table} {self.sql}')
