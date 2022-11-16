from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Load data from a s3 bucket to Redshift
    
    :param redshift_conn_id: Redshift connection defined in Airflow
    :param aws_credentials_id: AWS Credentials connection defined in Airflow
    :param table: Redshift table name where we are going to load the data
    :param s3_bucket: S3 Bucket name
    :param s3_key: S3 file key
    :param copy_json_option: Copy JSON option (auto by default)
    :param region: AWS Region where Redshift is located
    """
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """


    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        copy_json_option="auto",
        region="",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region

    def execute(self, context):
        """
        Load the data from S3 to Redshift. First, it truncates the table in order to ensure that we don't load data twice
            
        :param context: Context variables from Airflow
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"truncate {self.table}") 

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
                           
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )
        redshift.run(formatted_sql)
        self.log.info("Data successfully copied into Redshift!")
