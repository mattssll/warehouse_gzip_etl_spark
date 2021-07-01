from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from methods.spark_cleaning import clean_data_generics, clean_data_custom_reviews, clean_data_products
from methods.spark_writes import write_to_csv, create_sparksql_with_sql_query

absolutePathToApp = "/Users/mateus.leao/Documents/mattssll/takeaway"
numSplitFiles = 50
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
inputPath = f"{absolutePathToApp}/input_data/reviews_split/*"
outputPathFactReviews = f"{absolutePathToApp}/output_data/fct_reviews"
outputDimReviewers = f"{absolutePathToApp}/output_data/dim_reviewers"

# Cleaning our fact table so it can scale
cleanedDfCustom = clean_data_products(inputPath)
print(cleanedDfCustom.show())
write_to_csv(df=cleanedDfCustom, numFiles=numSplitFiles, path=outputPathFactReviews, compression="gzip")
queryGetDimReviewers = 'SELECT DISTINCT reviewerID, reviewerName FROM {}'
# Writing dim reviewers
create_sparksql_with_sql_query(df=cleanedDfCustom, view_name='dfDimReviewers', numFiles=15, \
                               query=queryGetDimReviewers, willWrite=True, outPathIfWrite=outputDimReviewers,
                               compression="gzip")
