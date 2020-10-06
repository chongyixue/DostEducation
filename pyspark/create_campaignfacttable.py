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
programmaxseqDF = readpsql('programmaxseqlookup',0)
program_expDF = readpsql("program_experience",0)
userpartnerDF = readpsql("userpartner_lookup",0)


print("loaded!")

campaignDF.createOrReplaceTempView("campaign")
program_expDF.createOrReplaceTempView("program_experience")
userpartnerDF.createOrReplaceTempView("userpartner_lookup")
programmaxseqDF.createOrReplaceTempView("programmaxseqlookup")


# TRANSFORM

test_query = spark.sql("""
CREATE TABLE campaignFact AS
SELECT
        campaign.user_id,
        users.created_on,
        campaign.deploy_datetime,
        campaign.status,
        campaign.program_id,
        program_experience.prog_start,
        campaign.listen_secs,
        userpartner_lookup.channel_id,
        programmaxseqlookup.maxseq
FROM campaign
INNER JOIN program_experience
        ON (program_experience.user_id=campaign.user_id AND program_experience.program_id = campaign.program_id)
INNER JOIN programmaxseqlookup  ON programmaxseqlookup.program_id=campaign.program_id
INNER JOIN userpartner_lookup ON userpartner_lookup.id=campaign.user_id
INNER JOIN users ON users.id=campaign.user_id
""")



# WRITE

mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}

test_query.write.jdbc(url=writeurl, table='campaignFact',
                      mode=mode, properties=properties)

print("Write complete!")

spark.stop()

