from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        COMPUPDATE OFF
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id=None,
                 aws_credentials_id=None,
                 table=None,
                 s3_bucket=None,
                 s3_key=None,
                 json_path=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f"DELETE FROM {self.table}")
        
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        s3_json_path = f"s3://{self.s3_bucket}/{self.json_path}" if self.json_path else 'auto'
            
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_json_path
        )
        
        redshift.run(formatted_sql)
        
        self.log.info('Copied data from S3 to Redshift')





