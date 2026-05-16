import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from datetime import datetime
from pyspark.sql.types import StringType,IntegerType,TimestampType,StructType , StructField

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.sql("CREATE DATABASE IF NOT EXISTS dev")

data = [
            ('12059-49504', 'INDIA', 'INR', 1, 1, datetime.now()),
            ('18594-07904', 'INDIA', 'INR', 1, 1, datetime.now()),
            ('06932-99420', 'USA', 'USD', 1, 1, datetime.now()),
            ('07942-19393', 'USA', 'USD', 1, 1, datetime.now()),
            ('97940-69402', 'UK', 'GBP', 1, 1, datetime.now())
    ]
    
schema = StructType(
                [
                StructField("account_number" , StringType() , False),
                StructField("account_country" , StringType() , False),
                StructField("account_currency" , StringType() , False),
                StructField("account_status" , IntegerType() , False),
                StructField("account_ver" , IntegerType() , False),
                StructField("account_dml" , TimestampType() , False),
                ]
    )

account_df = spark.createDataFrame(data = data , schema = schema)
account_df = account_df.withColumn("account_id" , expr("uuid()"))

account_df.write \
    .format("delta") \
    .mode("append") \
    .option("path", "s3://**") \
    .save()
                

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
