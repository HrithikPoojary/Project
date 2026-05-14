CREATE OR REPLACE TABLE dev.spark_db.asset(
        asset_id BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 100000 INCREMENT BY 1),
        asset_external_id STRING NOT NULL,
        asset_name STRING,
        asset_currency STRING,
        asset_status SMALLINT,
        asset_var SMALLINT,
        asset_dml TIMESTAMP
)
USING DELTA;