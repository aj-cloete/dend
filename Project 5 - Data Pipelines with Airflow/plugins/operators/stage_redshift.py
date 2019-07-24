from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Operator loads any JSON formatted files from S3 to Amazon Redshift.
    table (string): [required] The name of the Amazon Redshift table into which the data should be loaded
    s3_key (string): [required] The S3 key (within the s3_bucket) where the files to be copied to Amazon Redshift is located
    redshift_conn_id (string): [required] Airflow conn_id of Redshift connection
    s3_bucket (string): The bucket where the files to be copied is located (Default: udacity-dend)
    json_option (string): The json option specified in the copy statement.  If left on the default, CSV format assumed. (Default: '')
    aws_credentials_id (string): Airflow conn_id of the aws_credentials granting access to s3 (Default: 'aws_credentials')
    delimiter (string): delimiter character used in copy statement - ignored if json_object is not empty (Default: ',')
    ignore_headers (int): ignore headers in copy statement - ignored if json_object is not empty (1 -> true, 0 -> false) (Default: 1)
    '''
    ui_color = '#358140'
    template_fields = ("s3_key","json_option",)
    copy_sql = {}
    copy_sql['csv'] = """
COPY {target_table}
FROM '{s3_path}'
ACCESS_KEY_ID '{access_key}'
SECRET_ACCESS_KEY '{secret_key}'
IGNOREHEADER {ignore_header}
DELIMITER '{delimiter}'
"""
    copy_sql['json'] = """
COPY {target_table}
FROM {s3_path}
ACCESS_KEY_ID '{access_key}'
SECRET_ACCESS_KEY '{secret_key}'
JSON '{json_option}'
"""
    
    @apply_defaults
    def __init__(self,
                 table,
                 s3_key,
                 redshift_conn_id,
                 s3_bucket="udacity-dend",
                 json_option='',
                 aws_credentials_id="aws_credentials",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json_option = json_option

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE {}".format(self.table))

        self.log.info(f"Copying data from S3 to Redshift table '{self.table}'")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        copy_sql = StageToRedshiftOperator.copy_sql['csv'] if not self.json_option \
                    else StageToRedshiftOperator.copy_sql['json']
        copy_params = {
            'target_table':self.target_table,
            's3_path':s3_path,
            'access_key':credentials.access_key,
            'secret_key':credentials.secret_key,
            'json_option':self.json_option,
            'ignore_headers':self.ignore_headers,
            'delimiter':self.delimiter
        }
        formatted_sql = copy_sql.format(**copy_params)
        redshift.run(formatted_sql)
        
        self.log.info(f"Copying data from S3 to Redshift table '{self.table}' successful")

