from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
sc = SparkContext()
spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
apple_mobility_df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load("applemobilitytrends-2020-04-25.csv")
apple_mobility_df.createOrReplaceTempView("apple")

# drop country/region other than the united states
total = spark.sql("select * from apple where geo_type = 'country/region' and region = 'United States'")
total = total.withColumn("geo_type", when(total["geo_type"] == "country/region", "AllCountry"))
# drop cites from other countries
city = spark.sql("select * from apple where geo_type = 'city' and (region = 'Atlanta' or region = 'Baltimore' or \
    region = 'Boston' or region = 'Chicago' or region = 'Dallas' or region = 'Denver' or region = 'Detroit' or region = 'Houston'\
    or region = 'Los Angeles' or region = 'Miami' or region = 'New York City' or region = 'Philadelphia' \
    or region = 'San Francisco - Bay Area' or region = 'Seattle' or region = 'Washington DC')")
# clean data
output = total.unionAll(city).dropna().distinct().orderBy(["geo_type","region","transportation_type"])
output.coalesce(1).write.option("header", "true").csv("apple_mobility_25.csv")
