%python
class OrderRawData:
    def __init__(self):
        self.spark = spark
        self.checkpointlocation_kafka = '/Volumes/dev/spark_db/data/checkpoint/Order_Msg/'
        self.checkpointlocation_order = '/Volumes/dev/spark_db/data/checkpoint/Order/'

        self.bootstrap_server = '**'
        self.api_key = '**'
        self.api_secret_key = '**'
        self.jaas_module = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule'
        self.topic = 'DEV_ORDERS'

    def read_order_data(self):

        return (
            self.spark.readStream.format("kafka")
                                 .option("kafka.bootstrap.servers" , self.bootstrap_server)
                                 .option("kafka.security.protocol" , "SASL_SSL")
                                 .option("kafka.sasl.mechanism" , "PLAIN")
                                 .option("kafka.sasl.jaas.config", f"{self.jaas_module} required username='{self.api_key}' password='{self.api_secret_key}';")
                                 .option("subscribe" , self.topic)
                                 .option("startingOffsets" , "earliest")
                                 .load()
         )
        
    def get_schema(self):
        from pyspark.sql.types import StringType,DecimalType , StructField , StructType

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

    def schema_fix_order_msg(self, kafka_order_df):
        from pyspark.sql.functions import from_json,col,to_timestamp

        return (
            kafka_order_df.select(col("key").cast("string").alias("key"),
                                from_json(col("value").cast("string"),self.get_schema()).alias("value"),
                                "topic",
                                "partition",
                                "offset",
                                to_timestamp(col("timestamp")).alias("kafka_timestamp")
                                )
        )

            
    def schema_fix_order(self , schema_fix_order_msg ):
                return(

                    schema_fix_order_msg.selectExpr("value.unique_no as unique_no",
                                  "value.account_number as account_number",
                                  "value.asset_external_id as asset_external_id",
                                  "value.order_type as order_type",
                                  "value.payment_method as payment_method",
                                  "value.currency as currency",
                                  "value.quantity as quantity",
                                  "to_timestamp(value.created_dt) as created_dt"
                                    )
                )

            
    def wrtie_to_order_msg(self , schema_fix_order_msg):

        return (
            schema_fix_order_msg.writeStream.format("delta")
                                  .queryName("OrderKafkaStreamingQuery")
                                  .outputMode("append")
                                  .option("checkpointLocation" , self.checkpointlocation_kafka)
                                  .trigger(availableNow = True)
                                  .toTable("dev.spark_db.ORDER_MSG")
        )

    def wrtie_to_order(self , schema_fix_order):

        return (
            schema_fix_order.writeStream.format("delta")
                                  .queryName("OrderStreamingQuery")
                                  .outputMode("append")
                                  .option("checkpointLocation" , self.checkpointlocation_order)
                                  .trigger(availableNow = True)
                                  .toTable("dev.spark_db.ORDER")
        )

    def main(self):

        read_data = self.read_order_data()
        
        schema_fixed_msg = self.schema_fix_order_msg(read_data)

        order_msg_query = self.wrtie_to_order_msg(schema_fixed_msg)

        schema_fixed_ord = self.schema_fix_order(schema_fixed_msg)

        order_query = self.wrtie_to_order(schema_fixed_ord )

if __name__=='__main__':
    opr = OrderRawData()
    opr.main()

