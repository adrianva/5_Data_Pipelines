from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table(s)
    
    :param redshift_conn_id: Redshift connection in Airflow
    :param table: Redshift table name where we are going to load the data
    :param sql: SQL query for getting the data to load into the target table
    """
    
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into fact table in Redshift")
        redshift.run(f"insert into {self.table} {self.sql}")
