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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1755342518114 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://eligero-stedi-data-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1755342518114")

# Script generated for node Customer Curated
CustomerCurated_node1755342944408 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1755342944408")

# Script generated for node SQL Query
SqlQuery6980 = '''
SELECT
  CAST(stl.sensorreadingtime AS BIGINT) AS sensorReadingTime,
  CAST(stl.serialnumber AS STRING) AS serialNumber,
  CAST(stl.distancefromobject AS int) AS distanceFromObject 
FROM stl
INNER JOIN cc
ON stl.serialnumber = cc.serialnumber
'''
SQLQuery_node1755343068919 = sparkSqlQuery(glueContext, query = SqlQuery6980, mapping = {"stl":StepTrainerLanding_node1755342518114, "cc":CustomerCurated_node1755342944408}, transformation_ctx = "SQLQuery_node1755343068919")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1755343708454 = glueContext.getSink(path="s3://eligero-stedi-data-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1755343708454")
StepTrainerTrusted_node1755343708454.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1755343708454.setFormat("json")
StepTrainerTrusted_node1755343708454.writeFrame(SQLQuery_node1755343068919)
job.commit()