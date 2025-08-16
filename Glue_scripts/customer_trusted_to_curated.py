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

# Script generated for node Customer Trusted
CustomerTrusted_node1755284954562 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1755284954562")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1755284957451 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1755284957451")

# Script generated for node SQL Query
SqlQuery7733 = '''
SELECT DISTINCT
    CAST(ct.customername AS STRING) AS customername,
    CAST(ct.email AS STRING) AS email,
    CAST(ct.phone AS STRING) AS phone,
    CAST(ct.birthday AS STRING) AS birthday,
    CAST(ct.serialnumber AS STRING) AS serialnumber,
    CAST(ct.registrationdate AS BIGINT) AS registrationdate,
    CAST(ct.lastupdatedate AS BIGINT) AS lastupdate,
    CAST(ct.sharewithresearchasofdate AS BIGINT) AS sharewithresearchasofdate,
    CAST(ct.sharewithpublicasofdate AS BIGINT) AS sharewithpublicasofdate,
    CAST(ct.sharewithfriendsasofdate AS BIGINT) AS sharewithfriendsasofdate
FROM ct
INNER JOIN at
ON ct.email = at.user
'''
SQLQuery_node1755285118019 = sparkSqlQuery(glueContext, query = SqlQuery7733, mapping = {"ct":CustomerTrusted_node1755284954562, "at":AccelerometerTrusted_node1755284957451}, transformation_ctx = "SQLQuery_node1755285118019")

# Script generated for node Customer Curated
CustomerCurated_node1755285821219 = glueContext.getSink(path="s3://eligero-stedi-data-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1755285821219")
CustomerCurated_node1755285821219.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1755285821219.setFormat("json")
CustomerCurated_node1755285821219.writeFrame(SQLQuery_node1755285118019)
job.commit()