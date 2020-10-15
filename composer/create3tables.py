from pprint import pprint
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_date, datediff
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, NumericType, StringType,
    StructField, StructType
)
  
from pyspark.sql import functions as F
import sys

appName = "PySpark SQL example - really simple - via JDBC"#appname shows on Spark UI
#JDBCjar_path = "gs://dostbucket/jar/postgresql-42.2.16.jar"

conf = SparkConf() \
	.setAppName(appName) \
	.set("spark.driver.extraClassPath",JDBCjar_path)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

readuser=sys.argv[1]
readpassword=sys.argv[2]
writeuser=sys.argv[3]
writepassword=sys.argv[4]
readip=sys.argv[5]
writeip=sys.argv[6]
JDBCjar_path=sys.argv[7]

readurl='jdbc:postgresql://'+readip+'/postgres'
writeurl='jdbc:postgresql://'+writeip+'/postgres'

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
	print("usr="+usr)
	print("pswd="+pswrd)
	DF = spark.read \
		.format("jdbc") \
		.option("url",url) \
		.option("driver","org.postgresql.Driver") \
		.option("dbtable",tablename) \
		.option("user", usr) \
		.option("password",pswrd)\
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


print("********************Spark SQL loaded************************")


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
print("Write 3 tables 3 tables 3 tables complete!")









