from pyspark.sql import SparkSession

import glob
import psycopg2
from multiprocessing import Pool, cpu_count

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


file_path_list=sorted(glob.glob("/Users/mateus.leao/Documents/mattssll/takeaway/parquet_split/reviews*.parquet/part-00021-05dbefde-227b-4c2f-97b3-457f398176cb-c000.snappy.parquet"))
print("done with the repartitioning (saving again)")
print("preparing to send to pgsql")
def psql_copy_load():
    con = psycopg2.connect(database="takeaway",user="postgres",password="admin",host="127.0.0.1",port="5432")
    cursor = con.cursor()
    with open("/Users/mateus.leao/Documents/mattssll/takeaway/parquet_split/reviews*.parquet/part-00021-05dbefde-227b-4c2f-97b3-457f398176cb-c000.snappy.parquet", 'r') as f:
        cursor.copy_from(f, 'public.productreviewsz')

    con.commit()
    con.close()
    return (fileName)



psql_copy_load()

print("done")

"""
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
