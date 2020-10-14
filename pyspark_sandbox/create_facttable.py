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


campaignDF = readpsql("campaign")
program_experienceDF = readpsql("program_experience",0)
programmaxseqlookupDF = readpsql("programmaxseqlookup",0)
userpartner_lookupDF = readpsql("userpartner_lookup",0)
programseqDF = readpsql("programseq")
usersDF = readpsql("users")

print("loaded!")


campaignDF.createOrReplaceTempView("campaign")
program_experienceDF.createOrReplaceTempView("program_experience")
programmaxseqlookupDF.createOrReplaceTempView("programmaxseqlookup")
userpartner_lookupDF.createOrReplaceTempView("userpartner_lookup")
programseqDF.createOrReplaceTempView("programseq")
usersDF.createOrReplaceTempView("users")

print("**********preparing for sparkSQL*******")

# TRANSFORM
query1 = spark.sql("""
SELECT
        campaign.user_id,
        users.created_on,
        campaign.deploy_datetime,
        --campaign.status,
        campaign.program_id,
        program_experience.prog_start,
        campaign.listen_secs,
        userpartner_lookup.channel_id,
        programseq.sequence_index,
        programmaxseqlookup.maxseq
FROM campaign
INNER JOIN program_experience
        ON (program_experience.user_id=campaign.user_id AND program_experience.program_id = campaign.program_id)
INNER JOIN programmaxseqlookup  ON programmaxseqlookup.program_id=campaign.program_id
INNER JOIN userpartner_lookup ON userpartner_lookup.id=campaign.user_id
INNER JOIN users ON users.id=campaign.user_id
INNER JOIN programseq ON programseq.id=campaign.programseq_id
WHERE campaign.status="completed"
        """)




# WRITE

mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}

query1.write.jdbc(url=writeurl, table='campaignfact',
                      mode=mode, properties=properties)
print("Write complete!")

spark.stop()

