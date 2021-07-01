from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from methods.spark_cleaning import clean_data_generics, clean_data_custom_products_metadata
from methods.spark_writes import write_to_csv, create_sparksql_with_sql_query
from methods.spark_bucketize import bucketize_df
from schemas.dataschemas import getMetadataSchemas

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "8g") \
    .config("spark.cores.max", "4") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

absolutePathToApp = "/Users/mateus.leao/Documents/mattssll/takeaway"
# Doing the Transformation of the Extraction and Transformation
pathInputFiles = f"{absolutePathToApp}/input_data/metadata_split/split*"  # gotta fix this absolute path
schema = getMetadataSchemas()[0]
# Reads from multiple json.gzips and clean our data for us - handle duplicates, get ride of some fields, etc

cleanedDf = clean_data_generics(path=pathInputFiles, hasSchema=True, schema=schema, hasFixByteError=False,
                                colsFixByteError=None, \
                                hasDropColumns=None, dropColsFields=None, hasHandleDupesByField=True,
                                fieldHandleDupes=["asin"], hasDoSelectDistinct=False)
print(cleanedDf.count())
cleanedCustomDf = clean_data_custom_products_metadata(cleanedDf)

# Writing our dimension tables (dim_products and dim_price_buckets) to disk in CSV so we can use copy command to send to pgsql
outputPathDimBuckets = f"{absolutePathToApp}/output_data/dim_buckets"
outputPathDimProducts = f"{absolutePathToApp}/output_data/dim_products"
outputPathDimCategories = f"{absolutePathToApp}/output_data/dim_categories"
bucketizedDf = bucketize_df(df=cleanedCustomDf, numBuckets=30, \
                            outPathDimProducts=outputPathDimProducts, hasDropFields=True, \
                            colsToDrop=['also_bought', 'also_viewed', 'bought_together', 'buy_after_viewing', \
                                        'brand', 'sales_rank_type', 'sales_rank_pos', 'imUrl', 'description'],
                            compression='gzip')

# Write dimension price buckets
queryGetBucketDimension = 'SELECT bucketID, min(price) as minprice, max(price) as maxPrice FROM {} GROUP BY bucketID HAVING maxPrice>=0'
create_sparksql_with_sql_query(df=bucketizedDf, view_name='dfWithBucketsMaxMin', numFiles=1, \
                               query=queryGetBucketDimension, willWrite=True, outPathIfWrite=outputPathDimBuckets,
                               compression=None)
# Write dimension categories
dfUniquesCats = cleanedCustomDf.select('categories').distinct()
write_to_csv(df=dfUniquesCats, numFiles=5, path=outputPathDimCategories, compression="gzip")
print("log: done writing dim_products, dim_buckets, and dim_categories")
