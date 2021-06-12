from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from baseLogger import configure_logger

logger = configure_logger()
logger.warn("My test info statement")

#log4jLogger = sc._jvm.org.apache.log4j
#LOGGER = log4jLogger.LogManager.getLogger(__name__)
#LOGGER.info("started processing metadata.json")


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "8g") \
    .config("spark.cores.max", "3") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()


def clean_data(path: str) -> DataFrame:
    df = spark.read.json(path)
    newDf = df.withColumn('categories', flatten(df.categories))
    newDf2 = newDf.withColumn("categories", concat_ws(",",col("categories")))
    #cleanedDf = df.withColumn("categories",concat_ws(",",col("categories"))) # converting field to string
    return newDf2


def write_to_csv(df: DataFrame, path: str, compression: str) -> None:
    print("log: starting to write the csvs, this might take some time")
    df.write.csv(path = path ,sep=",", header=True, lineSep="\n", escape='"', nullValue=None, compression=compression)
    print("log: finished writing csvs")

cleanedDf = clean_data("metadata/small*")
print(cleanedDf.printSchema())
#print(cleanedDf.show(truncate=True))
#write_to_csv(cleanedDf, "/Users/mateus.leao/Documents/mattssll/takeaway/metadata_csv")
#print("log: the csvs were written successfully")












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
