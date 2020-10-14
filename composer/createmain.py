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
JDBCjar_path = "gs://dostbucket/jar/postgresql-42.2.16.jar"

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
	DF = spark.read \
		.format("jdbc") \
		.option("url",url) \
		.option("driver","org.postgresql.Driver") \
		.option("dbtable",tablename) \
		.option("user", usr) \
		.option("password",pswrd)\
		.load()
	return DF

listen_rateDF = readpsql("listen_rate")
program_experienceDF = readpsql("program_experience",0)
programmaxseqlookupDF=readpsql("programmaxseqlookup",0)
userpartner_lookupDF=readpsql("userpartner_lookup",0)
programseqDF=readpsql("programseq")
usersDF=readpsql("users")
contentDF=readpsql("content")

print("loaded!")

listen_rateDF.createOrReplaceTempView("listen_rate")
program_experienceDF.createOrReplaceTempView("program_experience")
programmaxseqlookupDF.createOrReplaceTempView("programmaxseqlookup")
userpartner_lookupDF.createOrReplaceTempView("userpartner_lookup")
programseqDF.createOrReplaceTempView("programseq")
usersDF.createOrReplaceTempView("users")
contentDF.createOrReplaceTempView("content")

# TRANSFORM
#spark.sql("""
#    SELECT COUNT(*)
#    FROM campaign
#""").show()
print("*******************GONNA DO A WRITE NOW******************")
test_query = spark.sql("""
        SELECT
		lr.user_id,
		MAX((year(lr.deploy_datetime)-year(users.created_on))    *12+(month(lr.deploy_datetime)-month(users.created_on)) + (case when     (day(lr.deploy_datetime)-day(users.created_on))<0 then -1 else 0 end     )) AS usr_months_in,
		MAX((year(pe.prog_start)-2017)*12+(month(pe.prog_start))) AS progstartmonth_abs,
		(year(lr.deploy_datetime)-2017)*12+month(lr.deploy_datetime) AS deploymonth_abs,
		MAX((year(lr.deploy_datetime)-year(pe.prog_start))*12+(month(lr.deploy_datetime)-month(pe.prog_start)) + (case when (day(lr.deploy_datetime)-day(pe.prog_start))<0 then -1 else 0 end )) AS ab_months_in,
		lr.program_id,
		min(pe.prog_start) AS prog_start,
		MAX(CASE WHEN lr.listen_secs_corrected > content.duration_secs THEN content.duration_secs
                	ELSE lr.listen_secs_corrected --correspond to chartio datastore logic
                	END) AS listen_secs_capped,
        	MAX(content.duration_secs) as duration_sec,
		lr.content_id,
		min(userpartner_lookup.channel_id) as channel_id,
        	max(programmaxseqlookup.maxseq) as maxseq
        FROM
                listen_rate AS lr
	INNER JOIN program_experience AS pe ON (pe.user_id=lr.user_id AND pe.program_id=lr.program_id)
	INNER JOIN programmaxseqlookup  ON programmaxseqlookup.program_id=lr.program_id
	INNER JOIN userpartner_lookup ON userpartner_lookup.id=lr.user_id
	INNER JOIN users ON users.id=lr.user_id
	INNER JOIN content ON content.id=lr.content_id
	GROUP BY lr.user_id,lr.program_id,deploymonth_abs,lr.content_id
""")
print("********************Spark SQL loaded************************")


# WRITE
mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}
test_query.write.jdbc(url=writeurl, table='mainfact',
                      mode=mode, properties=properties)
print("**************Write complete!*****************")
spark.stop()






"""
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}
test_query.write.jdbc(url=writeurl, table='copiedcampaign',
                      mode=mode, properties=properties)
print("**************Write complete!*****************")
spark.stop()
"""

