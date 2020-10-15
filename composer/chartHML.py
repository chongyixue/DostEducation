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

mainfactDF = readpsql("hmlmonth",0)

print("loaded!")

mainfactDF.createOrReplaceTempView("hmlmonth")

print("*******************GONNA DO A WRITE NOW******************")
test_query = spark.sql("""
SELECT
        (CASE WHEN usr_months_in=0 THEN 1
                WHEN usr_months_in<=2 THEN 3
                WHEN usr_months_in<=5 THEN 6
                ELSE 12
                END)AS months_up_to,
        hml,
        count (distinct user_id) AS user_count
FROM HMLmonth
WHERE deploymonth_abs>36 AND --months_up_to in (1,3,6) AND
        hml IN ('0-Very Low','1-Low','2-Medium','3-High')
GROUP BY months_up_to, hml
""")



print("********************Spark SQL loaded************************")
# WRITE
mode = "overwrite"
properties = {"user": writeuser, "password": writepassword,
              "driver": "org.postgresql.Driver"}
test_query.write.jdbc(url=writeurl, table='year2020hml',
                      mode=mode, properties=properties)
print("**************Write to year2020hml!*****************")















spark.stop()










