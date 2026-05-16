%python
from pyspark.sql.functions import expr

class OrderProcessor:
    def __init__(self):
        self.spark = None

    def read_staging_df(self):

        return (
            self.spark.readStream.table("dev.spark_db.order_stg")
        )

    def transaction(self,stg_df):

        account = self.spark.read.table("dev.spark_db.account")
        asset = self.spark.read.table("dev.spark_db.asset")


        txn_df = (
            stg_df.alias("sd").join(account.alias("ac"). expr("sd.account_number = ac.account_number"), "inner")
                              .join(asset.alias("as") , expr("sd.asset_external_id = as.asset_external_id"),"inner")
                              .select("ac.account_number",
                                      "as.asset_external_id",
                                      "sd.order_type",
                                      "as.asset_currency",
                                      "sd.asset_rate",
                                      "sd.quantity",
                                      "sd.asset_rate",
                                      "sd.order_amt",
                                      expr(" 1 as transaction_ver"),
                                      current_timestamp().alias("transaction_ver")
                                      )
        )

        return txn_df
    
    def write_to_transaction(self,stg_df):

        return (
            stg_df.writeStream.format("delta")
                              .queryName("Transaction_Query")
                              .outputMode("append")
                              .option("checkpointLocation" ,self.checkpointlocation_txn)
                              .trigger("availableNow" = True)
                              .toTable("dev.spark_db.transaction")
        )


    def quantity_process(self,order_stg):

        from delta.tables import DeltaTable
        from pyspark.sql.functions import broadcast

        quantity = DeltaTable.forPath(
            self.spark,
            "s3://....."
        )

        account_list = order_stg.select("account").distinct()

        txn_qty = transaction.alias("t").join(broadcast(account_list).alias("a"),
                                                expr("t.account_number = a.account_number"),
                                                "inner")
                .groupBy("a.account_number","t.asset_external_id").agg(sum(col("quantity")).alias("quantity"))

        asset_rate = self.spark.table("dev.spark_db.asset")

        quantity_val = (
        txn_qty.alias("tq").join(asset_rate.alias("ar"),expr('''
                                                             tq.asset_external_id = ar.asset_external_id
                                                             and ar.as_of_dt = current_date()
                                                             '''),
                                "inner")
                            .selectExpr("tq.account_number",
                                        "tq.asset_external_id",
                                        "tq.quanity as total_qty",
                                        "as.asset_rate",
                                        "tq.quantity*as.asset_rate as current_mkt_val",
                                        current_date().alias("as_of_dt") )
        )

        quantity.alias("qty").merge(order_stg.alias("stg"),expr('''
                                                                qty.account_number=stg.account_number
                                                                and 
                                                                qty.asset_external_id=stg.asset_external_id
                                                                '''))
                                .whenMatchedUpdate(
                                    {
                                        "quantity" : expr("stg.quantity + qty.quantity"),
                                        "available_balance" : expr("qty.available_amount - stg.order_amt")
                                     }
                                ).whenNotMatchedInsert(
                                    {
                                        "account_number" : "stg.account_number",
                                        "asset_external_id" : "stg.asset_external_id",
                                        "quantity" : "stg.quantity",
                                        "available_balance" : lit(100000) - col("order_amt") 
                                    }
                                ).execute()

    return quantity_val


    def write_to_quantity_dynamodb(self, batch_df , batch_id):

        def update_partition(rows):
            import boto3 

            dynamodb = boto3.resoure("dynamodb")

            quantity = dynamodb.Table("quantity")

            for row in rows:
                quantity.update_item(
                    key = {
                        "account_number" : row["account_number"],
                        "asset_external_id" : row["asset_external_id"]
                    },
                    UpdateExpression = '''
                    set #qu = :quantity,
                        #ab = :available_balance
                    ''',
                    ExpressionAttributeNames = {
                        "#qu" : "quantity",
                        "#av" : "available_balance"
                    },
                    ExpressionAttributeValues = {
                        ":quantity" : row['quantity'],
                        ":available_balance" : row['available_balance']
                    }
                    # default upsert
                    #if only insert
                        #ConditionExpression = 'attribute_not_exists(account_number,asset_external_id)'
                    #if only update
                        #ConditionExpression = 'attribute_exists(account_number,asset_external_id)'

                )




        batch_df.foreachPartition(update_partition)

    def write_to_quantity(self , order_stg):

        quantity_updated = self.spark.read.table("quantity")

        quantity_df = quantity_updated.alias("qu").join(order_stg.alias("os"),
                                                        expr('''
                                                            qu.account_number=os.account_number
                                                            and 
                                                            qu.asset_external_id = os.external_id
                                                                                   '''),
                                                        "inner"
                                                        )
                                    .select("qu.account_number",
                                            "qu.asset_external_id",
                                            "qu.quantity",
                                            "qu.avaiable_balance")
                                    
        quantity_df.writeStream.format("delta")
                                .queryName("quantity_query")
                                .foreachBatch(self.write_to_quantity_dynamodb)
                                .option("checkpointLocation" , self.checkpointLocation_quantity)
                                .trigger(processingTime = '5 seconds')
                                .start()



