from pyspark.sql import SparkSession
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
    fixTypedf = df.withColumn("helpful", df["helpful"].cast("string")).withColumn("overall", df["overall"].cast("integer"))
    #newDf3 = newDf2.withColumn("reviewTime", split(df.reviewTime, " "))

    null = u'\u0000'
    colsToClean = ['reviewText','reviewerID', 'summary']
    fixNullDf = fixTypedf.select(
          *(regexp_replace(col(c), null, '').alias(c) if c in colsToClean else c for
            c in df.columns)
      )
    logger.debug("DataFrame was successfully cleaned")
    fixNullDf = fixNullDf.dropDuplicates()
    return fixNullDf

def create_sparksql_view_n_query(df: DataFrame, view_name:str, query: str) -> None:
    df.createOrReplaceTempView(view_name)
    query = spark.sql(query.format(view_name))
    print(query)
    logger.debug("writing to disk")

def write_to_csv(df: DataFrame, path: str, compression: str) -> None:
    logger.debug("starting to write dataframe to multiple CSVs")
    print("log: starting to write the csvs, this might take some time")
    df.write.csv(path = path ,sep=",", header=True, lineSep="\n", escape='"', nullValue=None, compression=compression)
    logger.debug("Data was successfully written to CSVs")



cleanedDf = clean_data("products/smaller*")
#create_sparksql_view_n_query(df = cleanedDf, view_name="productsReview", query="SELECT count(*) FROM {}")
df2 = cleanedDf.select(countDistinct("reviewerID"))
df2.show()


#write_to_csv(cleanedDf, "/Users/mateus.leao/Documents/mattssll/takeaway/json_split", "None")
print("log: the csvs were written successfully")
#print(cleanedDf.printSchema())
#print(cleanedDf.show())
#changedTypedf.printSchema()
#df = spark.read.csv("/Users/mateus.leao/Documents/mattssll/takeaway/json_split/part*.csv")
#print(df.count())













"""
import glob
import psycopg2
from multiprocessing import Pool, cpu_count


filepath="/base_path/psql_multiprocessing_data"

df.repartition(400) \
    .write \
    .mode("overwrite") \
    .format("csv") \ # even faster using binary format, but ok with csv
    .save(filepath,header='false')

file_path_list=sorted(glob.glob("/base_path/psql_multiprocessing_data/*.csv"))

def psql_copy_load(fileName):
    con = psycopg2.connect(database="takeaway",user="my_user",password="admin",host="127.0.0.1",port="5432")
    cursor = con.cursor()
    with open(fileName, 'r') as f:
        # next(f)  # in case to skip the header row.
        cursor.copy_from(f, 'my_schema.my_table', sep="|")

    con.commit()
    con.close()
    return (fileName)


with Pool(cpu_count()) as p:
        p.map(psql_copy_load,file_path_list)

print("parallelism (cores): ",cpu_count())
print("files processed: ",len(file_path_list))








import psycopg2
#iterate over your files here and generate file object you can also get files list using os module
file = open('path/to/save/data/part-00000_0.csv')
file1 = open('path/to/save/data/part-00000_1.csv')

#define a function
def execute_copy(fileName):
    con = psycopg2.connect(database=dbname,user=user,password=password,host=host,port=port)
    cursor = con.cursor()
    cursor.copy_from(fileName, 'table_name', sep=",")
    con.commit()
    con.close()


from multiprocessing import Pool, cpu_count
with Pool(cpu_count()) as p:
        print(p.map(execute_copy, [file,file1]))
5:28
#40 mi rows in 10 mins
# 20 secs to copy a gb of data - estimate of 15 mins
# 5 mins to copy my JSON to CSVS - damn fast!
"""
