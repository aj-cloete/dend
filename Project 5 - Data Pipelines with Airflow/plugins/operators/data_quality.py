from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    '''
    Run tests using the provided sql_test and compare the results against the provided expected_result. 
    Raise an error if the result is not equal to the expected result
    conn_id (string): [required] Airflow conn_id to the database on which the tests should be run
    sql_test (string or list of strings): [required] SQL statement(s) to run as tests
    expected_result (string or list of strings): [required] Expected result(s) against which to compare the sql_test result(s)
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 sql_test,
                 expected_result,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if isinstance(sql_test,str):
            self.sql_test = [sql_test]
            self.expected_result = [expected_result]
        else:
            assert len(sql_test)==len(expected_result), "Must provide the same number of tests and results"
            self.sql_test = sql_test
            self.expected_result = expected_result
        
    def execute(self, context):
        self.log.info(f'Running Data Quality {'tests' if len(self.sql_test)>1 else 'test'}')
        hook = PostgresHook(self.conn_id)
        for (test_sql, expectation) in zip(self.sql_test,self.expected_result):
            result = hook.get_first(test_sql)[0]
            if str(result)!=expectation:
                self.log.info(f"Running test SQL: \n{test_sql}")
                raise ValueError(f'This test did not pass \n{test_sql}')
        self.log.info("All tests passed!")
