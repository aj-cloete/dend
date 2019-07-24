from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''
    sql_statement (string): [required] The SQL statement to run on the target database
    target_db_conn_id (string): [required] Airflow conn_id to the database on which the sql_statement should run
    target_table (string): [required] The name of the table into which the results from sql_statement should be loaded
    loading_type (string): The 'delete-load' option will drop-and-swap load the data.  The 'append' option (default) will append new data to the existing data
    '''
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_statement,
                 target_db_conn_id,
                 target_table,
                 loading_type='append'
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.target_db_conn_id = target_db_conn_id
        assert len(target_table) > 0, "The target_table name cannot be an empty string"
        self.target_table = target_table
        assert loading_type in ('delete-load','append'), "loading_type must be one of ('delete-load','append')"
        self.loading_type = loading_type
        self.log.info(f'Loading type: {self.loading_type}')

    def execute(self, context):
        hook = PostgresHook(self.target_db_conn_id)
        if self.loading_type=='delete':
            self.log.info(f"Clearing the data from {self.target_table}")
            truncate_statement = f"TRUNCATE {self.target_table}"
            hook.run(truncate_statement)
        
        self.log.info(f"Loading the fact table into the database at connection '{self.target_db_conn_id}'")
        insert_statement = f"INSERT INTO {self.target_table} \n{self.sql_statement}"
        self.log.info(f"Running sql: \n{insert_statement}")
        hook.run(insert_statement)
        self.log.info(f"Successfully completed insert into {self.target_table}")
        

