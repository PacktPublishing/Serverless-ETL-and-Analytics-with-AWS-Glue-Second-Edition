from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago


default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

with DAG(
    default_args=default_args,
    dag_id='ch10_3_example_workflow_mwaa',
    schedule_interval='0 3 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(hours=2)
):
    crawl_sales_data = GlueCrawlerOperator(
            task_id='sales_crawl',
            config={"Name": "ch10_3_example_workflow"})
    gen_report = GlueJobOperator(  
                task_id='gen_report',
                job_name='ch10_3_example_workflow_gen_report',
                script_args={
                    '--datalake_location': '<s3://<your-bucket-and-path>',
                    '--database': '<your-database>',
                    '--table': 'example_workflow_mwaa_sales',
                    '--report_year': '2024',
                    '--extra-jars': 's3://crawler-public/json/serde/json-serde.jar'
                })
    crawl_sales_data >> gen_report
