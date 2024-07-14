import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

inputDyf = glueContext.create_dynamic_frame_from_catalog(database='chapter_data_analysis_glue_database', table_name='employees')

glueContext.write_dynamic_frame.from_options(frame=inputDyf,
    connection_type="opensearch",
    connection_options={
        "connectionName": "opensearch-connection",
        "opensearch.resource": "employees",
    },
)

job.commit()
