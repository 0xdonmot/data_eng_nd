from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 redshift_conn_id=None,
                 insert_sql=None,
                 destination_table=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql = insert_sql
        self.destination_table = destination_table
        self.log.info(f'destination table: {self.destination_table}')

    def execute(self, context):
        self.log.info('Establishing Redshift Hook')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Inserting data into table')
        
        formatted_sql = f"""
            INSERT INTO {self.destination_table}
            {self.insert_sql}
        """
        redshift_hook.run(formatted_sql)
