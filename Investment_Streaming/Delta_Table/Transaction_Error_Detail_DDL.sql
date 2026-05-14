CREATE OR REPLACE TABLE dev.spark_db.transaction_error_detail(
    transaction_error_detail_id bigint primary key,
    transaction_error_description string,
    transaction_error_detail_dml timestamp
)
