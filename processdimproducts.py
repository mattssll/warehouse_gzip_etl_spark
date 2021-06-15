from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from baseLogger import configure_logger
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from pyspark.ml.feature import Bucketizer

logger = configure_logger()
logger.warn("Starting to process metadata")

#log4jLogger = sc._jvm.org.apache.log4j
#LOGGER = log4jLogger.LogManager.getLogger(__name__)
#LOGGER.info("started processing metadata.json")


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "8g") \
    .config("spark.cores.max", "3") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

initialSchema = StructType([ \
    StructField("asin",StringType(),False), \
    StructField("categories", ArrayType(ArrayType(StringType())),True), \
    StructField("price",DoubleType(),True), \
  ])

def clean_data(path: str) -> DataFrame:
    df = spark.read.json(path, schema=initialSchema, allowSingleQuotes=True,allowBackslashEscapingAnyCharacter=True)
    df = df.withColumn("categories", flatten(df.categories))
    df = df.withColumn("categories", concat_ws(",",col("categories")))
    cols_to_drop = ['title', 'imUrl', 'description', 'related', 'brand', 'salesRank']
    df = df.drop(*cols_to_drop)
    df = df.na.drop(subset=["asin"])
    return df

def write_to_csv(df: DataFrame, path: str, compression: str) -> None:
    print("log: starting to write the csvs, this might take some time")
    df.write.csv(path = path ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, emptyValue='', compression=compression)
    print("log: finished writing csvs")


cleanedDf = clean_data("metadata/small*")
#print(cleanedDf.printSchema())
#print(cleanedDf.count())
#print(type(cleanedDf))
#print(cleanedDf.count())

#print(cleanedDf.show(150,truncate=True))
#create_sparksql_view_n_query(df = cleanedDf, view_name="pmetadata", query="SELECT * FROM {} LIMIT 1000000")
write_to_csv(cleanedDf, "/Users/mateus.leao/Documents/mattssll/takeaway/dimproducts", compression="gzip")
#helpers.info("the csvs were written successfully")

"""
5:28
#40 mi rows in 10 mins
# 20 secs to copy a gb of data - estimate of 15 mins
# 5 mins to copy my JSON to CSVS - damn fast!
"""
