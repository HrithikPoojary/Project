%python
class OrderRawData:
    def __init__(self):
        self.spark = spark
        self.checkpointlocation_kafka = '/Volumes/dev/spark_db/data/checkpoint/Order_Msg/'
        self.checkpointlocation_order = '/Volumes/dev/spark_db/data/checkpoint/Order/'

    def read_order_data(self):

        return (
            self.spark.readStream.format("kafka")
                                 .option("kafka.servers" , "")
                                 .option("subscribe" , self.topic)
                                 .load()
         )
        
    def get_schema(self):
        import pyspark.sql.types import StringType,DecimalType , StructField , StructType

        return (
            StructType(
                [
                    StructField("unique_no" , StringType() , False),
                    StructField("account_number" , StringType() , False),
                    StructField("asset_external_id" , StringType() , False),
                    StructField("order_type" , StringType() , False),
                    StructField("payment_method" , StringType() , False),
                    StructField("currency" , StringType() , False),
                    StructField("quantity" , DecimalType(18,2) , False),
                    StructField("created_dt" , StringType() , False)
                ]
            )
        )

    def schema_fix(self, kafka_order_df, order ):
        import pyspark.sql.functions import from_json

        
            df =  (
                kafka_order_df.select(col("key").cast("string").alias("key"),
                                    from_json(col("value"),self.get_schema()).alias("value"),
                                    "topic",
                                    "timestamp"
                                    )
            )

            if order == 1:
                return df
            
            else:
                return(

                    df.selectExpr("value.unique_no",
                                  "value.account_number as account_number",
                                  "value.order_type as order_type",
                                  "value.payment_method as payment_method",
                                  "value.curreny as currency",
                                  "value.quantity as quantity"
                                  "value.created_dt as created_dt"
                                    )
                )

            
    def wrtie_to_order_msg(self , schema_fix):

        return (
            schema_fix.writeSchema.format("delta")
                                  .queryName("OrderKafkaStreamingQuery")
                                  .outputMode("append")
                                  .option("checkpointLocation" , self.checkpointlocation_kafka)
                                  .toTable("dev.spark_db.ORDER_MSG")
        )

    def wrtie_to_order(self , schema_fix):

        return (
            schema_fix.writeSchema.format("delta")
                                  .queryName("OrderStreamingQuery")
                                  .outputMode("append")
                                  .option("checkpointLocation" , self.checkpointlocation_order)
                                  .toTable("dev.spark_db.ORDER")
        )

    def main(self):

        read_data = self.read_order_data()
        schema_fixed_df = self.schema_fix(read_data , 1)
        

    

    




    

        
