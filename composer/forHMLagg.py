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
readurl='jdbc:postgresql://'+readip+'/postgres'
writeurl='jdbc:postgresql://'+writeip+'/postgres'
JDBCjar_path=sys.argv[7]

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

mainfactDF = readpsql("mainfact",0)

print("loaded!")

mainfactDF.createOrReplaceTempView("mainfact")

test_query = spark.sql("""
SELECT
	user_id,
	deploymonth_abs,
	min(ab_months_in) AS ab_months_in,
	min(usr_months_in) as usr_months_in,
	max(progstartmonth_abs) as progstartmonth_abs,
	sum(duration_sec) AS duration_secs,
	sum(listen_secs_capped) AS listen_secs_capped,
	max(maxseq) AS maxseq
FROM mainfact
GROUP BY user_id,deploymonth_abs
""")



# WRITE
mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}
test_query.write.jdbc(url=writeurl, table='aggmonth',
                      mode=mode, properties=properties)

# READ again (maybe oK?)
test_query.createOrReplaceTempView("aggmonth")
query2 = spark.sql("""
SELECT
        user_id,
        usr_months_in,
        deploymonth_abs,
        listen_secs_capped,
        duration_secs,
        1.0*listen_secs_capped/duration_secs AS consume_frac,
        (case   when usr_months_in<0 then 'NA'
                when 1.0*listen_secs_capped/duration_secs<0.05 then '0-Very Low'
                when 1.0*listen_secs_capped/duration_secs<0.25 then '1-Low'
                when 1.0*listen_secs_capped/duration_secs<0.5 then '2-Medium'
                else '3-High'
              end) AS HML
FROM aggmonth
""")
query2.write.jdbc(url=writeurl,table='HMLmonth',
	mode=mode,properties=properties)

spark.stop()










