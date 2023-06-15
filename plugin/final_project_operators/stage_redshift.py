from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#CD5C5C'

    template_fields = ("s3_key",)

    copy_songsql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            REGION 'us-east-1'
            FORMAT AS JSON 'auto'
        """

    copy_eventsql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-east-1'
            FORMAT AS JSON 's3://udacity-minfu-sparksong/log_json_path.json'
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.sql = sql

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Create table in Redshift")
        redshift.run(format(self.sql))

        self.log.info(f"Delete all data inside table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copy all data under S3 folder")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        if self.table == "staging_songs":
            transformed_sql = StageToRedshiftOperator.copy_songsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        else:
            transformed_sql = StageToRedshiftOperator.copy_eventsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        redshift.run(transformed_sql)