import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from itertools import chain
from pyspark.sql.functions import create_map, lit
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "<AWS_GLUE_DATABASE_NAME>", table_name = "<AWS_GLUE_TABLE_NAME>", transformation_ctx = "datasource0")

# Here is the custom gender mapping transformation 
df = datasource0.toDF()
gender_dict = { 'male': 1, 'female':0 }
mapping_expr = create_map([lit(x) for x in chain(*gender_dict.items())])
df = df.withColumn('gender', mapping_expr[df['gender']])
datasource_transformed = DynamicFrame.fromDF(df, glueContext, "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource_transformed, mappings = [("first", "string", "first", "string"), ("last", "string", "last", "string"), ("age", "int", "age", "int"), ("gender", "string", "gender", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double")], transformation_ctx = "applymapping1")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://<S3_BUCKET_NAME>"}, format = "csv", transformation_ctx = "datasink2")
job.commit()