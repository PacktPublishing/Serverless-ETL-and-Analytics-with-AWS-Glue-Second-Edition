import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions


def set_s3_path(bucket_and_path: str) -> str:
    if bucket_and_path.endswith("/"):
        return bucket_and_path[:-1]
    else:
        return bucket_and_path

args = getResolvedOptions(sys.argv, ['JOB_NAME','datalake_location', 'database', 'table', 'report_year'])
DATALAKE_LOCATION = set_s3_path(bucket_and_path=args['datalake_location'])
DATABASE = args['database']
TABLE = args['table']
REPORT_YEAR = args['report_year']
GEN_REPORT_QUERY = f"""
CREATE TABLE {DATABASE}.{TABLE}_report
USING parquet
LOCATION '{DATALAKE_LOCATION}/serverless-etl-and-analysis-w-glue-second-ed/chapter10/example-workflow-sfn/report/'
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
