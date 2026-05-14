%python
class OrderProcessor:
    def __init__(self):
        self.spark = spark


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

    def main(self):

        raw_df = self.read_raw_table()
        invalid_df = self.account_validation(raw_df).union(self.asset_validation(raw_df))
        self.invalid_error_detail(raw_df , invalid_df).display()


if __name__=='__main__':
    op = OrderProcessor()
    op.main()













