import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
google_mobility_df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
google_mobility_df.createOrReplaceTempView("google")
drop_country = ['country','countryiso3']
united_states = spark.sql("select * from google where country = 'United States' and countryiso3 = 'USA'").drop(*drop_country)

county =  united_states.withColumn("region",split(col("region"), ", ")).withColumn(
          "state",  col("region").getItem(0)).withColumn("county", col("region").getItem(1)).drop("region")
county.createOrReplaceTempView("county")

clean_result = county.drop().distinct().orderBy(['state','county'])
clean_result.coalesce(1).write.option("header", "true").csv("google_mobility_county.csv")