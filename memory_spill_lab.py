from pyspark.sql import SparkSession
import  pyspark.sql.functions as F

def count_records_per_parition(index ,partition_rows):
    rows_sum=sum(1 for _ in partition_rows)
    yield rows_sum, index


spark =(SparkSession.builder.master('local[2]').config("spark.driver.memory", "1g").
        config("spark.sql.shuffle.partitions",'1').
        config("spark.sql.adaptive.enabled", "false").
        getOrCreate())
#  2 * 1024 =2gb=(2048 -300) *0.6= 1048 mb


conf = spark.sparkContext.getConf()

for key in [
    "spark.driver.memory",
    "spark.executor.memory",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.memory.offHeap.enabled",
    "spark.memory.offHeap.size",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled"
]:
    print(key, "=", conf.get(key, "<default>"))

jvm = spark.sparkContext._jvm
runtime = jvm.java.lang.Runtime.getRuntime()

print(
    "Actual JVM max heap MiB:",
    runtime.maxMemory() / 1024**2
)

print(
    "Spark memory fraction:",
    spark.sparkContext.getConf().get(
        "spark.memory.fraction", "0.6 (default)"
    )
)

print(
    "Off-heap enabled:",
    spark.sparkContext.getConf().get(
        "spark.memory.offHeap.enabled", "false (default)"
    )
)

result = (spark.range(0,5000000).
          withColumn('grp_id', F.col('id')%1000000).
          withColumn('payload', F.sha2(F.col('id').cast('string'), 256)).
          withColumn('sort_key',F.pmod(F.col('id')*7919, 5000000) ).
          withColumn('value', F.col('id')*10))
result.show()
result_grp=result.groupBy('grp_id').agg(F.sum('value').alias('value_sum'))



result_grp.show()
# result.count()


result_sum=(result.sort(F.desc('sort_key')).rdd.
             mapPartitionsWithIndex(count_records_per_parition).collect())

print(result_sum)

print("Spark UI:", spark.sparkContext.uiWebUrl)
input("Inspect Spark UI, then press ENTER...")



