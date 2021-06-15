from pyspark.sql import SparkSession, DataFrame

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

def write_to_csv(df: DataFrame, numFiles: int, path: str, compression: str) -> None:
    try:
        df.repartition(numFiles).write.csv(path = path ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, compression=compression)
    except Exception as e:
        raise ValueError("code failed with error :", e)


def create_sparksql_with_sql_query(df: DataFrame, view_name:str, query: str, willWrite: bool, outPathIfWrite: str, numFiles: int, compression:str) -> None:
    try:
        df.createOrReplaceTempView(view_name)
        query = spark.sql(query.format(view_name))
        if willWrite:
                query.repartition(numFiles).write.csv(path = outPathIfWrite ,sep=",", header=False, lineSep="\n", escape='"', nullValue=None, compression=compression)
    except Exception as e:
        raise ValueError("code failed with error :", e)
