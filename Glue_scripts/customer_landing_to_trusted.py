import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Landing
CustomerLanding_node1755271702868 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://eligero-stedi-data-lakehouse/customer/landing/"]}, transformation_ctx="CustomerLanding_node1755271702868")

# Script generated for node SQL Query
SqlQuery7646 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1755272540510 = sparkSqlQuery(glueContext, query = SqlQuery7646, mapping = {"myDataSource":CustomerLanding_node1755271702868}, transformation_ctx = "SQLQuery_node1755272540510")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755272540510, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755271673371", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1755272894996 = glueContext.getSink(path="s3://eligero-stedi-data-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1755272894996")
CustomerTrusted_node1755272894996.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1755272894996.setFormat("json")
CustomerTrusted_node1755272894996.writeFrame(SQLQuery_node1755272540510)
job.commit()