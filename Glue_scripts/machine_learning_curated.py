import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1755346644493 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1755346644493")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1755346641591 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1755346641591")

# Script generated for node SQL Query
SqlQuery7293 = '''
SELECT
  CAST(at.user AS STRING) AS user,
  CAST(at.x AS FLOAT) AS x,
  CAST(at.y AS FLOAT) AS y,
  CAST(at.z AS FLOAT) AS z,
  CAST(at.timestamp AS BIGINT) AS timestamp,
  CAST(stt.serialnumber AS STRING) AS serialNumber,
  CAST(stt.distancefromobject AS INT) AS distanceFromObject
FROM at
INNER JOIN stt
ON at.timestamp = stt.sensorreadingtime
'''
SQLQuery_node1755347078516 = sparkSqlQuery(glueContext, query = SqlQuery7293, mapping = {"stt":StepTrainerTrusted_node1755346641591, "at":AccelerometerTrusted_node1755346644493}, transformation_ctx = "SQLQuery_node1755347078516")

# Script generated for node Step Trainer Curated
StepTrainerCurated_node1755347689300 = glueContext.getSink(path="s3://eligero-stedi-data-lakehouse/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerCurated_node1755347689300")
StepTrainerCurated_node1755347689300.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
StepTrainerCurated_node1755347689300.setFormat("json")
StepTrainerCurated_node1755347689300.writeFrame(SQLQuery_node1755347078516)
job.commit()