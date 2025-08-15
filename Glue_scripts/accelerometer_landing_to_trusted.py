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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1755276393220 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://eligero-stedi-data-lakehouse/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1755276393220")

# Script generated for node Customer Trusted
CustomerTrusted_node1755276959318 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1755276959318")

# Script generated for node SQL Query
SqlQuery7206 = '''
SELECT
  CAST(al.user AS STRING) AS user,
  CAST(al.timeStamp AS BIGINT) AS timeStamp,
  CAST(al.x AS FLOAT) AS x,
  CAST(al.y AS FLOAT) AS y,
  CAST(al.z AS FLOAT) AS z
FROM al
INNER JOIN ct ON al.user = ct.email
'''
SQLQuery_node1755276954893 = sparkSqlQuery(glueContext, query = SqlQuery7206, mapping = {"al":AccelerometerLanding_node1755276393220, "ct":CustomerTrusted_node1755276959318}, transformation_ctx = "SQLQuery_node1755276954893")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1755278524697 = glueContext.getSink(path="s3://eligero-stedi-data-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1755278524697")
AccelerometerTrusted_node1755278524697.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1755278524697.setFormat("json")
AccelerometerTrusted_node1755278524697.writeFrame(SQLQuery_node1755276954893)
job.commit()