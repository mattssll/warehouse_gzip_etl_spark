
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

def getMetadataSchemas():
    metadataSchema = StructType([ \
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
        StructField("salesRank", StringType(), True)])
    reviewSchema=""
    return [metadataSchema, reviewSchema]
