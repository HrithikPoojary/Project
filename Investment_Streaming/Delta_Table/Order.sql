CREATE OR REPLACE TABLE dev.spark_db.order(
                order_id BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 100000 INCREMENT BY 1),
                unique_no string,
                account_number string,
                asset_external_id string,
                order_type string,
                payment_method string,
                currency string,
                quantity decimal(18,2),
                created_dt timestamp,
                CONSTRAINT order_pk PRIMARY KEY(order_id)
)
using delta;
