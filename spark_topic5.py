from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame



def process(orders_df:DataFrame, customers_df:DataFrame)-> DataFrame:

    dedup_df= orders_df.drop_duplicates(["order_id", "customer_id", "status", "amount"])
    condn= (
            F.col('amount')>0 & F.col('status')=="COMPLETED")

    orders_filter= dedup_df.filter(condn)

    window_spec=Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())


    customer_df=customers_df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn")==1).drop("rn")

    joined_df= (orders_filter.alias('s').join(customer_df.alias('t'),F.col("s.customer_id")==F.col("t.customer_id"),'inner')
                    .select(F.col("s.amount").alias("amount"),F.col("t.country").alias("country"))
                     )

    # window_spec= Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc_nulls_last(),F.col("event_id").desc())
    #
    # latest_df=payment_filter.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn")==1).drop("rn")
    #
    # filter_df=latest_df.filter(F.col("status") == "SUCCESS")

    result_df=joined_df.groupBy(F.col("country")).agg(F.sum("amount").alias("total_revenue"), F.count('*').alias("completed_orders"))

    return result_df