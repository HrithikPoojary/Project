CREATE OR REPLACE TABLE dev.spark_db.order_msg(
    order_msg_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10000 INCREMENT BY 1),
    key STRING,
    value STRUCT<unique_no:string,
                account_number:string,
                asset_external_id:string,
                order_type:string,
                payment_method:string,
                currency:string,
                quantity:decimal(18,2),
                created_dt:string
                >,
    topic string,
    partition int,
    offset int,
    kafka_timestamp timestamp,
    CONSTRAINT order_msg_pk PRIMARY KEY(order_msg_id)
)
using delta;
