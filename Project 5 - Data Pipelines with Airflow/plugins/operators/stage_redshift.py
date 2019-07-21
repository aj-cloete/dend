from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
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
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="udacity-dend",
                 delimiter=",",
                 ignore_headers=1,
                 json_option='',
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
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
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
