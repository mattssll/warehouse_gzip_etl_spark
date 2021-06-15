from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from baseLogger import configure_logger
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer

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


def run_pgsql_query(query):
    jdbcDF = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:takeaway") \
        .option("dbtable", "schema.tablename") \
        .option("user", "username") \
        .option("password", "password") \
        .load()

    jdbcDF2 = spark.read \
        .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
              properties={"user": "username", "password": "password"})
#9430088
cleanedDf = clean_data("input_data/metadata.json.gzip-*")
bucketizedDf = bucketize_df(cleanedDf, 10)
