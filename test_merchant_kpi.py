from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructField, TimestampType, StructType
from spark_topic1 import process

def test_merchant_kpi(spark):


    schema=StructType()


    data=([])


    dataframe=spark.createDataFrame(data,schema)

    result=process(dataframe)

    actual={row["mechant_id"]: (row["total_success_amount"], row["successful_payment_count"]) for row in result.collect()}

    assert actual=={"M1":(200,2),"M2":(400,3)}