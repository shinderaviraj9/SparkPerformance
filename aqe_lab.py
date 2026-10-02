from pyspark.sql import SparkSession
import  pyspark.sql.functions as F

spark= SparkSession.builder.appName("AQE").getOrCreate()

spark.sparkContext.setLogLevel("Warn")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spakr.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedParitionThresholdInBytes","1MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2")

ROWS=1_000_000
rows = spark.range(ROWS)

print("Before Join Partitions")
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


joined_df= fact_df.join(customer_df,"customer_id", how="inner")
print("After Join Partitions")
print(joined_df.rdd.getNumPartitions())

# joined_df.explain("formatted")
joined_df.show()
joined_df.explain("formatted")
