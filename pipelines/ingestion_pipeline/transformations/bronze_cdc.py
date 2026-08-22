"""
Bronze layer — CDC sources: orders, order_items, customers.

Each table preserves the full Debezium envelope (op, ts_ms, before, after) exactly
as it arrived. No flattening here — that's Silver's job.
Bronze exists to give us a durable, replayable record of exactly what arrived, so
any downstream rebuild never has to re-touch the source systems.

Schemas are declared explicitly, not inferred by Auto Loader. These are our own
internal, contract-bound CDC sources — the schema is already fully known and
locked. Schema inference is for genuinely
unknown or externally-evolving data, not this.
"""
from pyspark import pipelines as dp 
from pyspark.sql import functions as F 
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)
from utilities.helpers import get_catalog,landing_path

ORDERS_ROW_SCHEMA =  StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_date", StringType()),
        StructField("updated_at", StringType()),
        StructField("shipping_address_id", StringType()),
        StructField("shipping_city", StringType()),
        StructField("shipping_state", StringType()),
        StructField("shipping_country", StringType()),
        StructField("payment_method", StringType()),
        StructField("discount_code", StringType()),
        StructField("discount_amount", DoubleType()),
        StructField("total_amount", DoubleType())
    ]
)

ORDER_ITEMS_ROW_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType()),
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()),
        StructField("line_total", DoubleType()),
        StructField("return_requested", BooleanType()),
        StructField("return_reason", StringType()),
    ]
)

CUSTOMERS_ROW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("phone", StringType()),
        StructField("date_of_birth", StringType()),
        StructField("gender", StringType()),
        StructField("registration_date", StringType()),
        StructField("loyalty_tier", StringType()),
        StructField("address_line1", StringType()),
        StructField("address_line2", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip_code", StringType()),
        StructField("country", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_at", StringType()),
    ]
)

def _envelope_schema(row_schema:StructType)-> StructType:
    """Wraps a row schema in the standard Debezium-flattened envelope shape."""
    return StructType(
        [
           StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField("before", row_schema),
            StructField("after", row_schema) 
        ]
    )
def _read_cdc_bronze(subfolder:str,row_schema:StructType):
    """Shared Auto Loader read, with ingestion metadata attached, for one CDC source."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .schema(_envelope_schema(row_schema))
        .load(landing_path(subfolder))
        .withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_path"))
    )


@dp.table(name="bronze_orders",
          comment="Raw Debezium CDC envelope for orders, exactly as it arrived. Not flattened yet.")
def bronze_orders():
    return _read_cdc_bronze("orders_cdc",ORDERS_ROW_SCHEMA)
    
@dp.table(
    name="bronze_order_items",
    comment="Raw Debezium CDC envelope for order_items, exactly as it arrived. Not flattened yet.",
)
def bronze_order_items():
    return _read_cdc_bronze("order_items_cdc", ORDER_ITEMS_ROW_SCHEMA)


@dp.table(
    name="bronze_customers",
    comment="Raw Debezium CDC envelope for customers, exactly as it arrived. Not flattened yet.",
)
def bronze_customers():
    return _read_cdc_bronze("customers_cdc", CUSTOMERS_ROW_SCHEMA)    


