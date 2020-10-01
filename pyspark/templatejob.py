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


user = "postgres"
readpassword = os.environ.get("PGPASSWORDREAD")
writepassword = os.environ.get("PGPASSWORDWRITE")

# depends on how cloud_sql_proxy is set up
#readurl = "jdbc:postgresql://127.0.0.1:5432/postgres"
#writeurl = "jdbc:postgresql://127.0.0.1:5431/postgres"
readurl = os.environ.get("READURL")
writeurl = os.environ.get("WRITEURL")


#READ
def readpsql(tablename):
    DF = spark.read \
        .format("jdbc") \
        .option("url",readurl) \
        .option("dbtable", "campaign") \
        .option("user", user) \
        .option("password",readpassword)\
        .load()
    return DF 
campaignDF = readpsql("campaign")

print("loaded!")

campaignDF.createOrReplaceTempView("campaign")


# TRANSFORM

spark.sql("""
    SELECT COUNT(*)
    FROM campaign
""").show()

test_query = spark.sql("""
        SELECT
                user_id,
                COUNT (id)
        FROM
                campaign
        WHERE
                status = 'completed'
        GROUP BY
                user_id
        LIMIT 10
""")




# WRITE

mode = "overwrite"
properties = {"user": "postgres", "password": writepassword,
              "driver": "org.postgresql.Driver"}

test_query.write.jdbc(url=writeurl, table='dummy2',
                      mode=mode, properties=properties)

print("Write complete!")

spark.stop()

