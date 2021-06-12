from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# map to all string columns the needed cleaning to get rid of 0x00 byte problem when inserting to pgresql
string_columns = ['reviewText','reviewerID', 'summary']

#sc = spark.sparkContext
path = "/Users/mateus.leao/Documents/mattssll/takeaway/parquet_split/reviews*.parquet/part-00021-05dbefde-227b-4c2f-97b3-457f398176cb-c000.snappy.parquet"
df = spark.read.csv(path, header=True)
null = u'\u0000'
newDf = df.select(
      *(regexp_replace(col(c), null, '').alias(c) if c in string_columns else c for
        c in df.columns)
  )

newDf.repartition(1).write.csv(encoding="utf8", path = "/Users/mateus.leao/Documents/mattssll/takeaway/products2/reprocess",sep=",", header=True, lineSep="\n", escape='"', nullValue=None)
#print("log: print schema")
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
