%python
class OrderProcessor:
    def __init__(self):
        self.spark = spark
        self.checkpointlocation_order_stg = None
        self.self.checkpointlocation_order_error = None



    def read_raw_table(self):
        return (
            self.spark.read.table("dev.spark_db.order")
        )

    def account_validation(self,order_raw_df):
        from pyspark.sql.functions import lit , expr ,broadcast,col

        acct_df = self.spark.read.table("dev.spark_db.account").alias("ad")
        
        inactive_account = (
            order_raw_df.alias("or").join(broadcast(acct_df),expr("ad.account_number = or.account_number"),"left")
                                    .withColumn("error_code_id" , expr('''
                                                                       case when ad.account_status = 2
                                                                                    or 
                                                                                 ad.account_id is null 
                                                                                    then    1
                                                                            else
                                                                                           2
                                                                            end
                                                                       
                                                                       '''))
                                    .where(col("error_code_id") == 1)
                                    .withColumn("error_msg_code" , lit(100))
                                    .select(
                                        "or.order_id",
                                        "error_code_id",
                                        "error_msg_code"
                                    )
        )
        return inactive_account

    def asset_validation(self,order_raw_df):
        from pyspark.sql.functions import lit , expr ,broadcast,col

        asst_df = self.spark.read.table("dev.spark_db.asset").alias("as")
        
        inactive_asset = (
            order_raw_df.alias("or").join(broadcast(asst_df),expr("as.asset_external_id = or.asset_external_id"),"left")
                                    .withColumn("error_code_id" , expr('''
                                                                       case when as.asset_status = 2
                                                                                    or 
                                                                                 as.asset_id is null
                                                                                    then    1
                                                                            else    
                                                                                            2
                                                                            end
                                                                       
                                                                       '''))
                                    .where(col("error_code_id") == 1)
                                    .withColumn("error_msg_code" , lit(101))
                                    .select(
                                        "or.order_id",
                                        "error_code_id",
                                        "error_msg_code"
                                    )
        )
        return inactive_asset

    def invalid_error_detail(self,order_raw_df,invalid_df):
        from pyspark.sql.functions import lit , expr ,broadcast,col

        txn_des = self.spark.read.table("dev.spark_db.transaction_error_detail").alias("td")

        txn_msg =(
         invalid_df.alias("id").join(broadcast(txn_des),expr("td.transaction_error_detail_id = id. error_msg_code")
                                     ,"inner")
                                .select(
                                    "id.order_id",
                                    "id.error_msg_code",
                                    "td.transaction_error_description"
                                )
        )

        return(
            order_raw_df.alias("or").join(txn_msg.alias("tm"),expr("or.order_id = tm.order_id"), "inner")
                                    .selectExpr("or.order_id as order_id",
                                                "or.account_number as account_number",
                                                "or.asset_external_id as asset_external_id",
                                                "or.order_type as order_type",
                                                "tm.transaction_error_description",
                                                "tm.error_msg_code",
                                                "or.created_dt as created_dt"
                                                )
        )

    def write_to_order_error_dynamo(self, batch_df , batch_id):

        def write_partition(rows):

            import boto3
            from datetime import datetime

            dynamodb = boto3.resource(
                        "dynamodb"
            )

            order_error = dynamodb.Table("order_error")

            with order_error.batch_writer() as batch:
                for row in rows:
                    batch.put_item(
                        Item = {
                            "order_id" : row("order_id"),
                            "account_number" : row("account_number"),
                            "asset_external_id" : row("asset_external_id"),
                            "order_type":row("order_type"),
                            "transaction_error_description" : row("transaction_error_description"),
                            "error_msg_code " : row("error_msg_code"),
                            "order_error_dml" : str(datetime.now()) 
                        }
                    )

        batch_df.foreachPartition(write_patition)
	   


    def write_to_order_error(self,invalid_df):

        return (
                invalid_df.writeStream.format("delta")
                                      .queryName("Order_error_query")
                                      .foreachBatch(self.write_to_order_error_dynamo)
                                      .outputMode("update")
                                      .option("checkpointLocation" , self.checkpointlocation_order_error)
                                      .trigger(availableNow = True)
                                      .start()
        )


    def asset_rate_result(self ,asset_list):

        asset_rate = self.spark.spark_db.table("asset_rate").alias("ar")

        return (
            asset_list.alias("al").join(asset_rate,expr('''ar.asset_external_id = al.asset_external_id 
                                                            and ar.as_of_dt  = to_date(created_dt)
                                                        '''), 
                                                        "inner")
                                  .select(
                                      "ar.asset_id",
                                      "ar.asset_name",
                                      "ar.asset_rate",
								 “ar.as_of_dt”
                                  )
        )

	def account_available_balance(self,acount_list):
		
		account = self.spark.read.table(“account”).alias(“s”)
		
		return (
				account.alias(“a”).join(account_list , expr(“s.account_number = a.account_number”) , “”inner)
								.select(“a.account_id”,
										“a.account_number”,
										“a.avaiable_balance”
										)
				)

	def order_computation(self, order_raw ,invalid_df):
		
		asset_list = order_raw.select(“asset_external_id”,
								     to_date(col(“created_at”))
							        .distinct()
								)
		asset_rate = self.asset_rate_result(asset_list)
  
        order_comp = (
                            order_raw.alias("or").join(invalid_df.alias("id"),expr("or.order_id = id.order_id"), "left")
                                    .where("id.error_msg_id <> 1 or id.order_id is null")                           
                            .join(asser_rate.alias("ar")
                                    ,expr("or.asset_external_id = ar.asset_external_id")
                                    ,"inner" )
                            .selectExpr(
                                "or.order_id",
                                "or.account_number",
                                "ar.account_external_id",
                                "or.order_type",
                                "or.quantity * ar.asset_rate as order_amt"
                            )
        return order_comp

    def order_computation_validation(self,order_raw,comp_df):

        account_list = order_raw.select(“account_number”).distinct()

        account_bal = self.account_available_balance(account_list)

        balance_validation = comp_df.alias("cd").join(account_bal.alias("ab"),
                                                      expr("ab.account_number = cd.account_number"),
                                                      "inner")
            .withColumn("error_msg_code"  , expr('''
                                                case when cd.order_amt > ab.available_balance 
                                                            then 1
                                                        else 
                                                            null
                                                end
                                                    '''))
            .where(col("error_msg_id = 1") == 1)
            .withColumn("error_msg_code" , lit(103))
            .select("order_id",
                    "error_msg_id",
                    "error_msg_code")
            )
    
        return balance_validation
    
    def order_valid_process(self , order_computaion,order_error_df):
        return (
            order_computation.alias("oc").join(order_error_df.alias("oe"), expr("oc.order_id = oe.order_id"),"left")
                             .where("oe.error_msg_id <> 1 or oe.order_id is null")
                             .select("oc.order_id",
                                     "oc.account_number",
                                     "oc.asset_external_id",
                                     "oc.order_type",
                                     "oc.order_type",
                                     current_timestamp().alias("order_stg_dml"))
        )

    def write_to_order_stg(self,valid_df):
        return (
            valid_df.writeStream.format("delta")
                                .queryName("Order_Stg_Query")
                                .outputMode("append")
                                .option("checkpointLocation" , self.checkpointlocation_order_stg)
                                .toTable("dev.spark_db.order_stg")
        )



    def main(self):

        raw_df = self.read_raw_table()
        account_validation = self.account_validation(raw_df)
        asset_validation = self.asset_validation(raw_df) 
        account_asset_invalid_df = account_validation.union(asset_validation)

        order_computaion = self.order_computation(raw_df , account_asset_invalid_df)

        order_balance_validation = self.order_computation_validation(order_computation , raw_df)

        invalid_df = account_asset_invalid_df.union(order_balance_validation)

        order_error_df = self.invalid_error_detail(raw_df,invalid_df)

        order_error_query = self.write_to_order_error(order_error_df)

        order_stg_df =  self.order_valid_process(order_computaion,order_error_df)

        order_stg_query = self.write_to_order_stg(order_stg_df)

        return(order_error_df , order_stg_query)



if __name__=='__main__':
    op = OrderProcessor()
    op.main()


