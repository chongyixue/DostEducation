from pprint import pprint
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, NumericType, StringType,
    StructField, StructType
)
import os

# Get password from environment
PASSWORD = os.environ.get('PGPASSWORD')
JDBCjar_path = os.environ.get('JARPATH')

appName = "PySpark SQL example - aggregate user - via JDBC"#appname shows on Spark UI
master = "local"

#JDBCjar_path = "~/postgresql-42.2.6.jar"

conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath",JDBCjar_path)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession


#database = "dost-data"
#table = "campaign"
user = "postgres"
password = PASSWORD

#url = jdbc:postgresql:///{database_name}?socketFactory=com.google.cloud.sql.postgres.SocketFactory&cloudSqlInstance={connection_name}:{instance_id}
#url = "jdbc:postgresql:///postgres?socketFactory=com.google.cloud.sql.postgres.SocketFactory&cloudSqlInstance=dost-demo:us-east4:dost-data"
#url = "jdbc:postgresql://35.245.162.58:5432/postgres/";
#url = 'jdbc:postgresql://%s:5432/%s?user=%s&password=%s'%('35.245.162.58','dost-data','postgres',password)
#url = "jdbc:postgresql://35.245.162.58:5432";
#url = "jdbc:postgresql://postgres@35.245.162.58:5432/postgres";
#url = "jdbc:postgresql://localhost:5432/postgres";
url = "jdbc:postgresql://127.0.0.1:5432/postgres";


#dbname = "35.245.162.58:5432/postgress"
#instance_conn_name="dost-data"
#dbname = "127.0.0.1:5432/postgres"
#instance_conn_name="dost-demo:us-east4:dost-data"

#url = "jdbc:postgresql:///"+dbname+"?cloudSqlInstance="+instance_conn_name+\
#    "&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user="+user+\
#    "&password="+password



#jdbcDF = spark.read \
#    .format("jdbc") \
#    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#    .option("dbtable", "campaign") \
#    .option("user", user) \
#    .option("password", password) \
#    .load()


#properties = {"user": "postgres", "password": password,
#              "driver": "org.postgresql.Driver"}

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url",url) \
    .option("dbtable", "campaign") \
    .option("user", user) \
    .option("password",password) \
    .load()

"""
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url",url) \
    .option("properties",properties)\
    .option("dbtable", "campaign") \
    .load()
"""
    

print("loaded!")

jdbcDF.createOrReplaceTempView("campaign")


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




# Postgresql info

mode = "overwrite"
url = "jdbc:postgresql://127.0.0.1:5432/postgres"
properties = {"user": "postgres", "password": password,
              "driver": "org.postgresql.Driver"}

test_query.write.jdbc(url=url, table='dummy',
                      mode=mode, properties=properties)

print("Write complete!")

spark.stop()

