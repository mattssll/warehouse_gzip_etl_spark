from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer

from .spark_writes import create_sparksql_with_sql_query
def create_session():
    spark = SparkSession \
        .builder \
        .appName("getSparkSession") \
        .config("spark.executor.memory", "8g") \
        .config("spark.cores.max", "4") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    return spark

spark = create_session()

# this could be separated better related to the 2 writes
def bucketize_df(df: DataFrame, numBuckets: int,  \
            outPathDimProducts:str, hasDropFields: bool, \
            colsToDrop: ["str"], compression: str):
    " This function does ..."
    if hasDropFields:
        df = df.drop(*colsToDrop)
    buckets = QuantileDiscretizer(relativeError=0.01, handleInvalid="error", \
    numBuckets=numBuckets, inputCol="price", outputCol="bucketID")
    dfWithBuckets = buckets.setHandleInvalid("keep").fit(df).transform(df)
    dfWithBucketsToWrite = dfWithBuckets
    # Write dimension_products
    df = df.na.drop(subset=["price"])
    dfWithBucketsToWrite.write.csv(path = outPathDimProducts ,sep=",", header=False, lineSep="\n", \
        escape='"', nullValue=None, emptyValue='', compression=compression)
    return dfWithBuckets
