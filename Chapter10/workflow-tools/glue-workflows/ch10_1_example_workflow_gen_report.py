import sys
import boto3
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions


def set_s3_path(bucket_and_path: str) -> str:
    if bucket_and_path.endswith("/"):
        return bucket_and_path[:-1]
    else:
        return bucket_and_path

def get_workflow_props(client, prop_name: str, args: dict) -> str:
    run_props = client.get_workflow_run_properties(
            Name=args['WORKFLOW_NAME'],
            RunId=args['WORKFLOW_RUN_ID'])['RunProperties']
    return run_props[prop_name]

glue = boto3.client("glue")
args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])

DATALAKE_LOCATION = set_s3_path(bucket_and_path=get_workflow_props(client=glue, prop_name="datalake_location", args=args))
DATABASE = get_workflow_props(client=glue, prop_name="database", args=args)
TABLE = get_workflow_props(client=glue, prop_name="table", args=args)
REPORT_YEAR = get_workflow_props(client=glue, prop_name="report_year", args=args)
GEN_REPORT_QUERY = f"""
CREATE TABLE {DATABASE}.{TABLE}_report
USING parquet
LOCATION '{DATALAKE_LOCATION}/serverless-etl-and-analysis-w-glue-second-ed/chapter10/example-workflow/report/'
PARTITIONED BY (report_year)
OPTIONS ('compression'='snappy')
AS SELECT
    category,
    CAST(sum(price) as long) as total_sales,
    year(to_timestamp(datetime)) as report_year
FROM {DATABASE}.{TABLE}
WHERE year(to_timestamp(datetime)) = {REPORT_YEAR}
GROUP BY category, report_year
ORDER BY category, report_year DESC
"""


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    
    spark.sql(GEN_REPORT_QUERY)
    spark.sql(f"SELECT * FROM {DATABASE}.{TABLE}_report").show(truncate=False) # show the report result
