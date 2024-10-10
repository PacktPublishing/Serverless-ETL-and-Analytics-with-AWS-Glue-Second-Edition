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
    SELECT
        category,
        CAST(sum(price) as long) as total_sales,
        year(to_timestamp(datetime)) as report_year
    FROM {DATABASE}.{TABLE}
    WHERE year(to_timestamp(datetime)) = {REPORT_YEAR}
    GROUP BY category, report_year
    ORDER BY category, report_year DESC
"""
CTAS_REPORT_QUERY = f"""
    CREATE TABLE {DATABASE}.{TABLE}_report
    USING parquet
    PARTITIONED BY (report_year)
    LOCATION '{DATALAKE_LOCATION}/serverless-etl-and-analysis-w-glue/chapter10/example-workflow-mwaa/report/'
    SELECT * FROM tmp
"""


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.sql(GEN_REPORT_QUERY)\
                .repartition("report_year")\
                .createOrReplaceTempView('tmp')
    df.show(n=df.count(), truncate=False)  # show the report result in the output

    # Create a report table
    spark.sql(CTAS_REPORT_QUERY)
