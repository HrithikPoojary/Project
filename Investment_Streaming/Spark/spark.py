from pyspark.sql import SparkSession
import logging

logging.basicConfig(
        filename = '/Workspace/Users/hrithikpoojary29@gmail.com/Investment_Banking/Logs/Spark.log',
        level = logging.INFO,
        format = "Saprk | %(asctime)s | %(levelname)s | %(message)s"
)

logging.info("Starting Sparksession")

spark = SparkSession.builder\
                    .appName("Investment_Streaming_Spark")\
                    .config("spark.sql.extensions" , "io.delta.sql.DeltaSparkSessionExtension")\
                    .config("spark.sql.catalog.spark_catalog" , "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                    .getOrCreate()

logging.info("SparkSession has been created successfully")
