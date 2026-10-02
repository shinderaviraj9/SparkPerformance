from pyspark.sql import SparkSession
import  pyspark.sql.functions as F

spark= SparkSession.builder.appName("AQE").getOrCreate()

spark.sparkContext.setLogLevel("Warn")
print(spark.sparkContext.uiWebUrl)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set(
    "spark.sql.adaptive.forceOptimizeSkewedJoin",
    "true"
)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","64MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2")

ROWS=1_000_000
rows = spark.range(ROWS)

print("Before Join Partitions")
print(rows.rdd.getNumPartitions())

fact_df =(rows.withColumn("customer_id",F.when(F.col("id")<700000 , F.lit(1)).
                                         otherwise((F.col("id"))%1000+2))
                .withColumn("amount",(F.rand()*1000).cast("double"))
                .withColumn("amt_int", F.col("amount").cast("integer"))
                .withColumn("payload",F.concat(F.col('customer_id'),F.lit('_'),F.repeat( F.lit("X"),1000))

          ))


((fact_df.repartition(8, "customer_id").withColumn ("pid",F.spark_partition_id()).
                                        groupBy("pid").
                                       agg(F.count('*').alias("hot_record"),
                                       F.sum(F.length('payload')).alias("payload_lenght"))).
                                       orderBy(F.col("hot_record").desc()).show(10))


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
gdf.show(10)
print("*"*10)
print(gdf.rdd.getNumPartitions())

customer_df= (spark.range(1,1002)
            .withColumnRenamed('id',"customer_id")
            .withColumn("customer_type", F.when((F.col('customer_id'))%2==0, F.lit("Premium")).otherwise(F.lit("Standard")))
             )


joined_df= fact_df.join(customer_df,"customer_id", how="inner")
print("After Join Partitions")
print(joined_df.rdd.getNumPartitions())

print(
    spark.conf.get(
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"
    )
)

print(
    spark.conf.get(
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor"
    )
)

print(
    spark.conf.get(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes"
    )
)


print("Spark:", spark.version)

print(
    "forceOptimizeSkewedJoin:",
    spark.conf.get("spark.sql.adaptive.forceOptimizeSkewedJoin")
)

print(
    "skewJoin:",
    spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
)

# joined_df.explain("formatted")
joined_df.explain("formatted")
result=joined_df.agg(F.sum(F.length('payload')).alias("payload_lenght_total"))
result.show()

input("Query finished. Check Spark UI now. Press ENTER to exit...")
