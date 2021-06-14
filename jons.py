from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from baseLogger import configure_logger
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from pyspark.ml.feature import Bucketizer

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "8g") \
    .config("spark.cores.max", "3") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

df = spark.read.json("json_split/part*.csv.gz")
df2 = spark.read.json("metadatatest/part*.csv")
df2 = df2.na.drop(subset=["asin"])
df2.count()