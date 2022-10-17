from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 redshift_conn_id=None,
                 tables=None,
                 min_rows=1,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.min_rows = min_rows
        
    def execute(self, context):
        redshift_hook = PostgresHook('redshift')
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < self.min_rows or len(records[0]) < self.min_rows:
                raise ValueError(f"Data quality check failed. {table} returned fewer than {self.min_rows} rows")
            num_records = records[0][0]
            if num_records < self.min_rows:
                raise ValueError(f"Data quality check failed. {table} contained fewer than {self.min_rows} rows")            
            
            self.log.info(f'Data quality on {table} passed with {num_records} records')