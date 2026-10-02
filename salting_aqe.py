from pyspark.sql import SparkSession
import  pyspark.sql.functions as F

spark= (SparkSession.builder.appName("data_skew").
        master("local[3]").
        config("spark.sql.shuffle.partitions", "8").
        config("spark.sql.adaptive.enabled", "false").
        config("spark.sql.autoBroadcastJoinThreshold","-1")
        .config("spark.sql.adaptive.coalescePartitions.enabled","false")
        .getOrCreate())


spark.sparkContext.setLogLevel("WARN")
print(
    "Broadcast threshold:",
    spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
)

print(
    "AQE:",
    spark.conf.get("spark.sql.adaptive.enabled")
)
ROWS=1_000_000
rows = spark.range(ROWS)

# rows.show(5)
print(rows.rdd.getNumPartitions())

fact_df =(rows.withColumn("customer_id",F.when(F.col("id")<700000 , F.lit(1)).
                                         otherwise((F.col("id"))%1000+2))
                .withColumn("amount",(F.rand()*1000).cast("double"))
                .withColumn("amt_int", F.col("amount").cast("integer")))

# fact_df.show(5)
print(
    "Shuffle parititons :",
    spark.conf.get("spark.sql.shuffle.partitions")
)

print("AQE coalesce:",
      spark.conf.get(
          "spark.sql.adaptive.coalescePartitions.enabled"
      ))
gdf=(fact_df.groupBy(F.col("customer_id"))
            .count()
            .orderBy(F.col("count").desc()))

print("*"*10)
print(gdf.rdd.getNumPartitions())

customer_df= (spark.range(1,1002)
            .withColumnRenamed('id',"customer_id")
            .withColumn("customer_type", F.when((F.col('customer_id'))%2==0, F.lit("Premium")).otherwise(F.lit("Standard")))
             )
# customer_df.show(5)