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


factDF = readpsql("campaignfact",0)

print("loaded!")


factDF.createOrReplaceTempView("campaignfact")

print("**********preparing for sparkSQL*******")

# TRANSFORM
query1 = spark.sql("""
SELECT
    *,
    (extract(year from (prog_start))-2017)*12
        + extract(month from (prog_start))
            AS progstartmonth_abs,
    (extract(year from (deploy_datetime))-2017)*12
                + extract(month from (deploy_datetime))
                        AS deploymonth_abs,
    ((extract(year from (deploy_datetime))-extract(year from (prog_start)))*12
        +(extract(month from (deploy_datetime))-extract(month from (prog_start)))
        + (case when ((extract(day from (deploy_datetime))-extract(day from (prog_start))))<0 then -1
                else 0
                end ))
            AS ab_months_in
from campaignfact
        """)




# WRITE

mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}

query1.write.jdbc(url=writeurl, table='campaignfact_added',
                      mode=mode, properties=properties)
print("Write complete!")

spark.stop()

