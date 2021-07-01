from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType


def create_session() -> SparkSession:
    spark = SparkSession \
        .builder \
        .appName("getSparkSession") \
        .config("spark.executor.memory", "8g") \
        .config("spark.cores.max", "4") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    return spark


def clean_data_generics(path: str, hasSchema: bool, schema, hasFixByteError: bool, colsFixByteError: ["str"], \
                        hasDropColumns: bool, dropColsFields: ["str"], hasHandleDupesByField: bool, \
                        fieldHandleDupes: ["str"], hasDoSelectDistinct: bool) -> DataFrame:
    """
    This function reads data from multiple json files and clean the data
    with standard cleaning transformations
    so it can be inserted to postgresql later on.
    """
    try:
        spark = create_session()
        if hasSchema:
            df = spark.read.json(path, schema=schema, allowSingleQuotes=True,
                                 allowBackslashEscapingAnyCharacter=True)
        else:
            df = spark.read.json(path, allowSingleQuotes=True, allowBackslashEscapingAnyCharacter=True)
        if hasFixByteError:
            byteToReplace = u'\u0000'
            df = df.select(*(regexp_replace(col(c), byteToReplace, '').alias(c) if c in colsFixByteError else c for c in
                             df.columns))
        if hasHandleDupesByField:
            df = df.na.drop(subset=fieldHandleDupes)
        if hasDropColumns:
            df = df.drop(*dropColsFields)
        if hasDoSelectDistinct:
            df = df.dropDuplicates()
        return df
    except Exception as e:
        raise ValueError("code failed, log error: ", e)


def clean_data_custom_reviews(df: DataFrame) -> DataFrame:
    """
    This function reads data from multiple json files and clean the data
    with custom transformations for the data regarding reviews
    """
    try:
        spark = create_session()
        print("info: starting to clean custom reviews table in a custom way")
        df = df.withColumn("overall", df["overall"].cast("integer")).withColumn("month",
                                                                                split(col("reviewTime"), ' ').getItem(
                                                                                    0)) \
            .withColumn("day", split(col("reviewTime"), ' ').getItem(1)).withColumn("day",
                                                                                    regexp_replace(col("day"), ",", "")) \
            .withColumn("year", split(col("reviewTime"), ' ').getItem(2))
        df = df.drop("reviewTime")
        df = df.withColumn('reviewTime', concat('year', lit('-'), 'month', lit('-'), 'day')).withColumn('reviewTime',
                                                                                                        regexp_replace(
                                                                                                            col("reviewTime"),
                                                                                                            "--",
                                                                                                            "-06-"))
        df = df.na.drop(subset=["year", "month", "day"])
        cols_to_drop = ['year', 'month', 'day', 'helpful']
        df = df.drop(*cols_to_drop)
        print("info: finished cleaning data from reviews")
        return df
    except Exception as e:
        raise ValueError(e)


def clean_data_custom_products_metadata(df: DataFrame) -> DataFrame:
    """
    This function does some custom cleaning for the products metadata
    """
    try:
        spark = create_session()
        df = df.withColumn("categories", flatten(df.categories))
        df = df.withColumn("categories", concat_ws(",", col("categories")))
        df = df.withColumn("also_bought", df.related.also_bought).withColumn("also_bought",
                                                                             concat_ws(",", col("also_bought")))
        df = df.withColumn("also_viewed", df.related.also_viewed).withColumn("also_viewed",
                                                                             concat_ws(",", col("also_viewed")))
        df = df.withColumn("bought_together", df.related.bought_together).withColumn("bought_together", concat_ws(",",
                                                                                                                  col("bought_together")))
        df = df.withColumn("buy_after_viewing", df.related.buy_after_viewing).withColumn("buy_after_viewing",
                                                                                         concat_ws(",",
                                                                                                   col("buy_after_viewing")))
        df = df.withColumn("sales_rank_type", split(col("salesRank"), ':').getItem(0)).withColumn("sales_rank_type",
                                                                                                  regexp_replace(
                                                                                                      col("sales_rank_type"),
                                                                                                      '\{"',
                                                                                                      '')).withColumn(
            "sales_rank_type", regexp_replace(col("sales_rank_type"), '"', ''))
        df = df.withColumn("sales_rank_pos", split(col("salesRank"), ':').getItem(1)).withColumn("sales_rank_pos",
                                                                                                 regexp_replace(
                                                                                                     col("sales_rank_pos"),
                                                                                                     '}', ''))
        cols_to_drop = ['related', 'salesRank']
        df = df.drop(*cols_to_drop)
        return df
    except Exception as e:
        raise ValueError(e)


def clean_data_products(path: str) -> DataFrame:
    spark = create_session()
    df = spark.read.json(path)
    df = df.withColumn("helpful", df["helpful"].cast("string")).withColumn("overall", df["overall"].cast("integer"))
    null = u'\u0000'
    colsToClean = ['reviewText', 'summary']
    df = df.select(
        *(regexp_replace(col(c), null, '').alias(c) if c in colsToClean else c for
          c in df.columns)
    )
    df = df.withColumn("month", split(col("reviewTime"), ' ').getItem(0)) \
        .withColumn("day", split(col("reviewTime"), ' ').getItem(1)).withColumn("day",
                                                                                regexp_replace(col("day"), ",", "")) \
        .withColumn("year", split(col("reviewTime"), ' ').getItem(2))
    df = df.drop("reviewTime")
    df = df.withColumn('reviewTime', concat('year', lit('-'), 'month', lit('-'), 'day')).withColumn('reviewTime',
                                                                                                    regexp_replace(
                                                                                                        col("reviewTime"),
                                                                                                        "--", "-06-"))
    df = df.na.drop(subset=["year", "month", "day"])
    cols_to_drop = ['year', 'month', 'day', 'unixReviewTime', 'helpful', 'reviewText', 'summary']
    df = df.drop(*cols_to_drop)
    return df
