from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame



def process(payment_df:DataFrame)-> DataFrame:

    # dedup_df= payment_df.drop_duplicates(["payment_id", "updated_at", "status", "amount","merchant_id"])

    window_spec= Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc(),F.col("event_id").desc())

    latest_df=payment_df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn")==1).drop("rn")

    filter_df=latest_df.filter(F.col("status") == "SUCCESS")

    result_df=filter_df.groupBy(F.col("merchant_id")).agg(F.sum("amount").alias("total_success_amount"), F.count('*').alias("successful_payment_count"))

    return result_df