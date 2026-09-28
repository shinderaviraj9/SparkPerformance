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


joined_df=fact_df.join(customer_df, customer_df.customer_id==fact_df.customer_id, "inner")
# joined_df.show(5)
print(joined_df.rdd.getNumPartitions())
# joined_df.explain("formatted")
(joined_df.withColumn("partition_records",F.spark_partition_id())
            .groupBy("partition_records").count().orderBy(F.col("count").desc()).show(5))



#####Salting
SALT_BUCKETS=8
customer_df= customer_df.withColumn("salt_array", F.when(F.col('customer_id')==1, F.sequence(F.lit(0), F.lit(SALT_BUCKETS-1)))
                            .otherwise(F.lit([0]))
                            ).withColumn("salted", F.explode(F.col("salt_array"))).drop('salt_array')

# customer_df.show(5)


fact_df=fact_df.withColumn("salted", F.when(F.col("customer_id")==1, (F.rand()*SALT_BUCKETS).cast("int")).otherwise(F.lit(0)))

# fact_df.show(5)


joined_salted=fact_df.join(customer_df,['customer_id','salted'], how="inner")


joined_salted=joined_salted.withColumn('amt',F.round('amount',2)).drop(F.col("amount"))

joined_recs_per_partition=(joined_salted.withColumn("partition_recs", F.spark_partition_id()).
               groupBy("partition_recs").count().orderBy(F.col("count").desc()))
#
# print("*"*30)
# joined_salted.show(5)

joined_recs_per_partition.show(5)


###Group by by salting

agg_gby_df=(joined_salted.groupBy("customer_id","salted").agg(F.sum("amt").alias("partial_sum")).
        groupBy("customer_id").agg(F.round(F.sum("partial_sum"),2).alias("total")))


agg_gby_df.show(5)