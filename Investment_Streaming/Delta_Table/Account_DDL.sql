CREATE OR REPLACE TABLE dev.spark_db.account(
        account_id BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 100000 INCREMENT BY 1),
        account_number STRING NOT NULL,
        account_country STRING,
        account_currency STRING,
        account_status SMALLINT,
        account_var SMALLINT,
        account_dml TIMESTAMP,
        CONSTRAINT account_pk PRIMARY KEY(account_id)
)
USING DELTA;