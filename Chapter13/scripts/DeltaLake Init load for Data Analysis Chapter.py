import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from delta.tables import *

args = getResolvedOptions(sys.argv, ['JOB_NAME','TARGET_BUCKET'])

spark = SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
tableLocation = 's3://'+args['TARGET_BUCKET']+'/deltalake_employees/'
inputDf = glueContext.create_data_frame.from_catalog(database='chapter_data_analysis_glue_database', table_name='employees')

inputDf.write.format('delta') \
  .mode('append') \
  .save(tableLocation)

deltaTable = DeltaTable.forPath(spark, tableLocation)
deltaTable.generate("symlink_format_manifest")

spark.sql("CREATE TABLE `chapter_data_analysis_glue_database`.employees_deltalake (emp_no int, name string, department string, city string, salary int) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '"+tableLocation+"_symlink_format_manifest/'")

job.commit()