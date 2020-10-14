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
def readpsql(tablename):
    DF = spark.read \
        .format("jdbc") \
        .option("url",readurl) \
        .option("dbtable",tablename) \
        .option("user", readuser) \
        .option("password",readpassword)\
        .load()
    return DF 

usersDF = readpsql("users")
partnerDF = readpsql("partner")
channelDF = readpsql("channel")
programseqDF = readpsql("programseq")
experienceDF = readpsql("experience")

print("loaded!")

usersDF.createOrReplaceTempView("users")
partnerDF.createOrReplaceTempView("partner")
channelDF.createOrReplaceTempView("channel")
programseqDF.createOrReplaceTempView("programseq")
experienceDF.createOrReplaceTempView("experience")


# TRANSFORM
# make table1: userpartner_lookup
query1 = spark.sql("""
SELECT
        users.id,
        partner.channel_id,
        channel.name
FROM users
INNER JOIN partner ON users.partner_id=partner.id
INNER JOIN channel ON channel.id=partner.channel_id
        """)

# table2: programmaxseqlookup
query2 = spark.sql("""
SELECT
        program_id,
        MAX(sequence_index) AS maxseq
FROM programseq
GROUP BY program_id
ORDER BY program_id
        """)

# table3: program_experience
query3 = spark.sql("""
SELECT
        user_id,
        program_id,
        MIN(start_date) AS prog_start
from experience
GROUP BY program_id,user_id
ORDER BY prog_start
        """)


# WRITE

mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}

query1.write.jdbc(url=writeurl, table='userpartner_lookup',
                      mode=mode, properties=properties)
query2.write.jdbc(url=writeurl, table='programmaxseqlookup',
                      mode=mode, properties=properties)
query3.write.jdbc(url=writeurl, table='program_experience',
                      mode=mode, properties=properties)
print("Write complete!")

spark.stop()

