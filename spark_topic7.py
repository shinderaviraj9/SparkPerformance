from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame



def process(orders_df:DataFrame, customers_df:DataFrame)-> DataFrame:

    # dedup_df= payment_df.drop_duplicates(["payment_id", "updated_at", "status", "amount","merchant_id"])
    # dedup_df = orders_df.drop_duplicates(["order_id", "customer_id", "status", "amount"])
    df=orders_df.withColumn("item", F.explode_outer(F.col("items")))

    df=df.select("customer_id","order_id",F.col("item.product_id").alias("product_id"),F.col("item.quantity").alias("quantity"),F.col("item.unit_price").alias("unit_price"))
    df = orders_df.withColumn("line_amount", F.col("quantity")* F.col("unit_price"))

    condn = (
            (F.col('quantity') > 0 )&( F.col('unit_price') > 0))

    orders_filter = df.filter(condn)



    result_df = orders_filter.groupBy(F.col("product_id")).agg(F.sum("quantity").alias("total_quantity"),
                                                        F.sum('line_amount').alias("total_revenue"))



    return result_df