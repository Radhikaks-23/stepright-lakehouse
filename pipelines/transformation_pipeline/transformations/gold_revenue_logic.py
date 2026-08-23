from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, to_date, coalesce, lit, when

def compute_daily_revenue(
    order_items_df: DataFrame,
    orders_df: DataFrame,
    products_df: DataFrame,
    categories_df: DataFrame,
)-> DataFrame:
    orders_current = orders_df.filter(col("__END_AT").isNull())
    
    order_subtotals = (
        order_items_df.groupBy("order_id")
        .agg(_sum("line_total").alias("order_subtotal"))
    )

    items_with_discount = (
        order_items_df
        .join(orders_current, "order_id")
        .join(order_subtotals, "order_id")
        .withColumn("order_discount", coalesce(col("discount_amount"), lit(0.0)))
        .withColumn(
            "allocated_discount",
            when(col("order_subtotal") > 0, col("line_total") / col("order_subtotal") * col("order_discount"))
            .otherwise(lit(0.0)),
        )
    )

    return (
        items_with_discount
        .join(products_df, "product_id")
        .join(categories_df, "category_id")
        .groupBy(
            to_date(col("order_date")).alias("revenue_date"),
            col("category_name"),
            col("shipping_state").alias("region"),
        )
        .agg(
            _sum("line_total").alias("gross_revenue"),
            _sum("allocated_discount").alias("total_discount"),
            (_sum("line_total") - _sum("allocated_discount")).alias("net_revenue"),
        )
    )