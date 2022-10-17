from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 redshift_conn_id=None,
                 insert_sql=None,
                 destination_table=None,
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql
        self.destination_table = destination_table
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Establishing Redshift Hook')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Inserting data into table')
        
        truncate_statement = f"TRUNCATE {self.destination_table};" if self.truncate else ""
        formatted_sql = f"""
            {truncate_statement}
            INSERT INTO {self.destination_table}
            
            {self.insert_sql}        
        """
        redshift_hook.run(formatted_sql)
