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
    .config("spark.cores.max", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

initialSchema = StructType([ \
    StructField("asin",StringType(),False), \
    StructField("categories", ArrayType(ArrayType(StringType())),True), \
    StructField("title",StringType(),True), \
    StructField("price",DoubleType(),True), \
    StructField("imUrl", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField('related', StructType([
             StructField('also_bought', ArrayType(StringType()), True),
             StructField('also_viewed', ArrayType(StringType()), True),
             StructField('bought_together', ArrayType(StringType()), True),
             StructField('buy_after_viewing', ArrayType(StringType()), True),
             ])), \
    StructField("brand", StringType(), True), \
    StructField("salesRank", StringType(), True)
  ])

def clean_data(path: str) -> DataFrame:
    df = spark.read.json(path, schema=initialSchema, allowSingleQuotes=True,allowBackslashEscapingAnyCharacter=True)
    df = df.withColumn("categories", flatten(df.categories))
    df = df.withColumn("categories", concat_ws(",",col("categories")))
    df = df.withColumn("also_bought", df.related.also_bought).withColumn("also_bought", concat_ws(",",col("also_bought")))
    df = df.withColumn("also_viewed", df.related.also_viewed).withColumn("also_viewed", concat_ws(",",col("also_viewed")))
    df = df.withColumn("bought_together", df.related.bought_together).withColumn("bought_together", concat_ws(",",col("bought_together")))
    df = df.withColumn("buy_after_viewing", df.related.buy_after_viewing).withColumn("buy_after_viewing", concat_ws(",",col("buy_after_viewing")))
    df = df.withColumn("sales_rank_type", split(col("salesRank"), ':').getItem(0)).withColumn("sales_rank_type", regexp_replace(col("sales_rank_type"),'\{"','')).withColumn("sales_rank_type", regexp_replace(col("sales_rank_type"),'"',''))
    df = df.withColumn("sales_rank_pos", split(col("salesRank"), ':').getItem(1)).withColumn("sales_rank_pos", regexp_replace(col("sales_rank_pos"),'}',''))
    cols_to_drop = ['related', 'salesRank']
    df = df.drop(*cols_to_drop)
    #if drop_duplicates == True:
    #df = df.dropDuplicates() # very heavypyt
    df = df.na.drop(subset=["asin"])
    print(df.printSchema())
    return df

def write_to_csv(df: DataFrame, path: str, compression: str) -> None:
    print("log: starting to write the csvs, this might take some time")
    df.write.csv(path = path ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, emptyValue='', compression=compression)
    print("log: finished writing csvs")

def create_sparksql_view_n_query(df: DataFrame, view_name:str, query: str) -> None:
    df.createOrReplaceTempView(view_name)
    queriedDf = spark.sql(query.format(view_name))
    logger.debug("writing to disk")
    #query.write.csv(path = "testarquivos" ,sep=",", header=True, lineSep="\n", escape='"', nullValue=None, compression="gzip")
    logger.info("Printing results from SQL Query")
    queriedDf.show(20,truncate=True)
    return queriedDf

def bucketize_df(df, numBuckets, outPath, hasDropFields, colsToDrop ):
    df = df.na.drop(subset=["price"])
    if hasDropFields:
        df = df.drop(*colsToDrop)

    buckets = QuantileDiscretizer(relativeError=0.01, handleInvalid="error", numBuckets=numBuckets, inputCol="price", outputCol="buckets")
    dfWithBuckets = buckets.setHandleInvalid("keep").fit(df).transform(df)# \
        #.write.csv(path = outPath ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, emptyValue='', compression=None)
    dfWithBucketsMaxMin = create_sparksql_view_n_query(dfWithBuckets, 'dfWithBucketsMaxMin', 'SELECT buckets, min(price) as minprice, max(price) as maxPrice FROM {} GROUP BY buckets')
    print(dfWithBucketsMaxMin.show())
    return dfWithBuckets
#9430088
cleanedDf = clean_data("metadata/small*")

#cleanedDf = clean_data("input_data/metadata.json.gzip-*")
print(cleanedDf.show())
#bucketizedDf = bucketize_df(cleanedDf, 20, './20bucketfiles', True, ['categories','title', 'imUrl','description', 'also_bought','also_viewed', 'bought_together', 'buy_after_viewing','brand','sales_rank_type', 'sales_rank_pos'])
#bucketizedDf = bucketize_df(cleanedDf, 30, './30bucketfiles', True, ['categories','title', 'imUrl','description', 'also_bought','also_viewed', 'bought_together', 'buy_after_viewing','brand','sales_rank_type', 'sales_rank_pos'])
#create_sparksql_view_n_query(bucketizedDf)
#print(cleanedDf.printSchema())
#print(cleanedDf.count())
#print(type(cleanedDf))
#print(cleanedDf.count())

#print(cleanedDf.show(150,truncate=True))
#create_sparksql_view_n_query(df = cleanedDf, view_name="pmetadata", query="SELECT * FROM {} LIMIT 1000000")

#write_to_csv(cleanedDf, "/Users/mateus.leao/Documents/mattssll/takeaway/metadatatestjsongzip", compression="gzip")
#helpers.info("the csvs were written successfully")

"""
5:28
#40 mi rows in 10 mins
# 20 secs to copy a gb of data - estimate of 15 mins
# 5 mins to copy my JSON to CSVS - damn fast!
"""
