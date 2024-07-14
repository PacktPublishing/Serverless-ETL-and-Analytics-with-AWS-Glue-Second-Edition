import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet', 'false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

employees_schema = StructType([StructField("emp_no", IntegerType()),
    StructField("name", StringType()),
    StructField("department", StringType()),
    StructField("city", StringType()),
    StructField("salary", IntegerType())])

updated_data = [[3, 'Jeff', 'Finance', ' Cincinnati', 75000]]

updated_df = spark.createDataFrame(updated_data, schema = employees_schema).withColumn('ts', lit(datetime.now()))

config = {'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'hoodie.datasource.write.recordkey.field': 'emp_no',
    'hoodie.table.name': 'employees_cow',
    'hoodie.consistency.check.enabled': 'true',
    'hoodie.datasource.hive_sync.database': 'chapter_data_analysis_glue_database',
    'hoodie.datasource.hive_sync.table': 'employees_cow',
    'hoodie.datasource.hive_sync.enable': 'true',
    'path': 's3://'+args['TARGET_BUCKET']+'/hudi/employees_cow_data',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': 3}

updated_df.write.format('hudi') \
  .options(**config) \
  .mode('append') \
  .save()

job.commit()
