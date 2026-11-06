from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame



def process(bronze_df:DataFrame, silver_df:DataFrame)-> DataFrame:

    # dedup_df= payment_df.drop_duplicates(["payment_id", "updated_at", "status", "amount","merchant_id"])
    # dedup_df = orders_df.drop_duplicates(["order_id", "customer_id", "status", "amount"])
    # df=orders_df.withColumn("item", F.explode_outer(F.col("items")))
    window_spec = Window.partitionBy("patient_id").orderBy(F.col("updated_at").desc(),F.col("event_id").desc())

    bronze_df = bronze_df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")


    joined_df=(silver_df.alias("silver_d").join(bronze_df.alias("bronze_d"),F.col("silver_d.payment_id")==F.col("bronze_d.payment_id"),"inner").select(F.col("silver_d.payment_id").alias("payment_id"),
          F.col("silver_d.updated_date").alias("silver_updated_dt"),F.col("bronze_d.updated_date").alias("bronze_updated_dt"),F.col("bronze_d.amount").alias("bronze_amount"),F.col("silver_d.amount").alias("silver_amount"),F.col("bronze_d.status").alias("bronze_status")
                                                                                                                                                       ,F.col("silver_d.status").alias("silver_status")))
    new_payments = (bronze_df.alias("bronze_d").join(silver_df.alias("silver_d"),
                                                     F.col("silver_d.patient_id") == F.col("bronze_d.patient_id"),
                                                     "left_anti"))

    complete_payment=joined_df.unionByName(new_payments.select("payment_id","updated_date","amount","status"))






    return complete_payment