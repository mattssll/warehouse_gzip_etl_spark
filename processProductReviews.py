from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from baseLogger import configure_logger

logger = configure_logger()
logger.info("starting to process product reviews data")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def clean_data(path: str) -> DataFrame:
    """
    This function reads data from multiple json files and clean the data
    so it can be inserted to postgresql later on.
    """
    logger.debug("reading data from multiple JSONs and cleaning it")
    df = spark.read.json(path)
    df = df.withColumn("reviewID", monotonically_increasing_id())
    df = df.withColumn("helpful", df["helpful"].cast("string")).withColumn("overall", df["overall"].cast("integer"))
    df= df.withColumn("reviewTime", to_date(df.reviewTime, 'yyyy-MM-dd'))
    null = u'\u0000'
    colsToClean = ['reviewText','reviewerID', 'summary']
    df = df.select(
          *(regexp_replace(col(c), null, '').alias(c) if c in colsToClean else c for
            c in df.columns)
      )
    logger.debug("DataFrame was successfully cleaned")
    df = df.dropDuplicates()
    return df

def create_sparksql_view_n_query(df: DataFrame, view_name:str, query: str) -> None:
    df.createOrReplaceTempView(view_name)
    query = spark.sql(query.format(view_name))
    print(query)
    logger.debug("writing to disk")

def write_to_csv(df: DataFrame, path: str, compression: str) -> None:
    logger.debug("starting to write dataframe to multiple CSVs")
    print("log: starting to write the csvs, this might take some time")
    df.write.csv(path = path ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, compression=compression)
    logger.debug("Data was successfully written to CSVs")



cleanedDf = clean_data("products/smaller*")
#create_sparksql_view_n_query(df = cleanedDf, view_name="productsReview", query="SELECT count(distinct reviewerid) FROM {}")



#write_to_csv(cleanedDf, "/Users/mateus.leao/Documents/mattssll/takeaway/json_split", "gzip")
print("log: the csvs were written successfully")
print(cleanedDf.printSchema())
print(cleanedDf.show())
#changedTypedf.printSchema()
#df = spark.read.csv("/Users/mateus.leao/Documents/mattssll/takeaway/json_split/part*.csv")
#print(df.count())









