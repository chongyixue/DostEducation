from pprint import pprint
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_date, datediff
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, NumericType, StringType,
    StructField, StructType
)

from pyspark.sql import functions as F
import os


appName = "PySpark SQL example - aggregate user - via JDBC"#appname shows on Spark UI
master = "local"
#JDBCjar_path = "/home/yc983_cornell_edu/postgresql-42.2.16.jar"
JDBCjar_path = os.environ.get("JARPATH")


conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath",JDBCjar_path)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession


readuser = os.environ.get("READPGUSER")
readpassword = os.environ.get("PGPASSWORDREAD")
writeuser = os.environ.get("WRITEPGUSER") 
writepassword = os.environ.get("PGPASSWORDWRITE")

# depends on how cloud_sql_proxy is set up
#readurl = "jdbc:postgresql://127.0.0.1:5432/postgres"
#writeurl = "jdbc:postgresql://127.0.0.1:5431/postgres"
readurl = os.environ.get("READURL")
writeurl = os.environ.get("WRITEURL")


#READ
def readpsql(tablename, read = 1):
    if read==1:
        url = readurl
        usr = readuser
        pswrd = readpassword
    else:
        url = writeurl
        usr = writeuser
        pswrd = writepassword
    DF = spark.read \
        .format("jdbc") \
        .option("url",url) \
        .option("dbtable",tablename) \
        .option("user", usr) \
        .option("password",pswrd)\
        .load()
    return DF


factDF = readpsql("aggregatefacts",0)

print("loaded!")


factDF.createOrReplaceTempView("agg")

print("**********preparing for sparkSQL*******")

# TRANSFORM
query1 = spark.sql("""
        SELECT
        *,
        SUM(totalsec) OVER (PARTITION BY user_id,program_id
                        ORDER BY deploymonth_abs) AS cumsec,
        sum(nlessons) OVER (PARTITION BY user_id,program_id
                        ORDER BY deploymonth_abs) AS cumlesson
FROM agg 
        """)




# WRITE

mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}

query1.write.jdbc(url=writeurl, table='aggregatefacts_cum',
                      mode=mode, properties=properties)
print("Write complete!")

spark.stop()

