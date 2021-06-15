from flask import Flask
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer
import json

app = Flask(__name__)

@app.route("/")
def hello_world():
    print("hello world in terminal")
    return "200"


@app.route("/runSparkFile", methods=['GET'])
def runSparkFile():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
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
    print("hi, got the spark session")
    print("printing spark session", spark)
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
        queriedDf.show(20,truncate=True)
        return queriedDf

    def bucketize_df(df, numBuckets, outPath, hasDropFields, colsToDrop ):
        df = df.na.drop(subset=["price"])
        if hasDropFields:
            df = df.drop(*colsToDrop)

        buckets = QuantileDiscretizer(relativeError=0.01, handleInvalid="error", numBuckets=numBuckets, inputCol="price", outputCol="buckets")
        dfWithBuckets = buckets.setHandleInvalid("keep").fit(df).transform(df)# \
        dfWithBucketsMaxMin = create_sparksql_view_n_query(dfWithBuckets, 'dfWithBucketsMaxMin', 'SELECT buckets, min(price) as minprice, max(price) as maxPrice FROM {} GROUP BY buckets')
        print(dfWithBucketsMaxMin.show())
        return dfWithBuckets

    cleanedDf = clean_data("smaller_aaaa")
    print(cleanedDf.show())

    return {"response_code" : "200"}

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
