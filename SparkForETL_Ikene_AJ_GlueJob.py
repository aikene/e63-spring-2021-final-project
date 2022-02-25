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
## @type: DataSource
## @args: [format_options = {"jsonPath":"$[*]","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://aws-demo-ingestion-bucket/indiana-schools-covid/2021/5/9/ffe6e850-b0e2-11eb-90ed-f96bcacfb12c.json"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"$[*]","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://aws-demo-ingestion-bucket/indiana-schools-covid/2021/5/9/ffe6e850-b0e2-11eb-90ed-f96bcacfb12c.json"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: DropFields
## @args: [paths = ["_full_text", "_id"], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = DropFields.apply(frame = DataSource0, paths = ["_full_text", "_id"], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("county_fips", "string", "county_fips", "double"), ("school_address", "string", "school_address", "string"), ("school_county", "string", "school_county", "string"), ("student_total_cases", "string", "student_total_cases", "string"), ("teacher_total_cases", "string", "teacher_total_cases", "string"), ("staff_total_cases", "string", "staff_total_cases", "string"), ("school_name", "string", "school_name", "string"), ("longitude", "string", "longitude", "decimal"), ("submission_status", "string", "submission_status", "string"), ("school_city", "string", "school_city", "string"), ("latitude", "string", "latitude", "decimal"), ("school_zip", "string", "school_zip", "string"), ("school_id", "string", "school_id", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform1]
Transform0 = ApplyMapping.apply(frame = Transform1, mappings = [("county_fips", "string", "county_fips", "double"), ("school_address", "string", "school_address", "string"), ("school_county", "string", "school_county", "string"), ("student_total_cases", "string", "student_total_cases", "string"), ("teacher_total_cases", "string", "teacher_total_cases", "string"), ("staff_total_cases", "string", "staff_total_cases", "string"), ("school_name", "string", "school_name", "string"), ("longitude", "string", "longitude", "decimal"), ("submission_status", "string", "submission_status", "string"), ("school_city", "string", "school_city", "string"), ("latitude", "string", "latitude", "decimal"), ("school_zip", "string", "school_zip", "string"), ("school_id", "string", "school_id", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "demo-schools", format = "glueparquet", connection_options = {"path": "s3://aws-demo-refined-bucket/indiana-schools-covid/", "compression": "snappy", "partitionKeys": [], "enableUpdateCatalog":true, "updateBehavior":"LOG"}, catalog_table_name = "indianaschools", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.getSink(path = "s3://aws-demo-refined-bucket/indiana-schools-covid/", connection_type = "s3", updateBehavior = "LOG", partitionKeys = [], compression = "snappy", enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "demo-schools",catalogTableName = "indianaschools")
DataSink0.setFormat("glueparquet")
DataSink0.writeFrame(Transform0)

job.commit()
