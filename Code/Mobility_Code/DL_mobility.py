import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
dl_mobility_df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load("/user/xt544/DL-us-mobility-daterow.csv")
dl_mobility_df.createOrReplaceTempView("dl")
drop_state = ['country_code','admin_level','admin2']
drop_county = ['country_code','admin_level']
# extract admin level 1
clean = spark.sql("select * from dl where country_code = 'US'")
clean = clean.withColumn('date', date_format(col('date'), 'MM-dd'))
clean.createOrReplaceTempView("data")

def createPivot(dataframe, group, classOut):
    dataframe_pivot = dataframe.groupBy(group).pivot('date').sum(classOut).orderBy("fips")
    dataframe_out = dataframe_pivot.withColumn("class", lit(classOut))
    return dataframe_out

state = spark.sql("select * from data where admin_level = 1")
state = state.drop(*drop_state).orderBy(['fips','date'])

state_pivot_samples = createPivot(state, ['admin1', 'fips'], "samples")
state_pivot_m50 = createPivot(state, ['admin1', 'fips'], "m50")
state_pivot_m50index = createPivot(state, ['admin1', 'fips'], "m50_index")

state_output = state_pivot_samples.unionAll(state_pivot_m50).unionAll(state_pivot_m50index).orderBy(["fips","class"]).dropna().distinct()
state_output.coalesce(1).write.option("header", "true").csv("DL_mobility_state.csv")

county = spark.sql("select * from data where admin_level = 2")
county = county.drop(*drop_county).orderBy(['fips','date'])
county_pivot_samples = createPivot(county, ['admin1','admin2', 'fips'], "samples")
county_pivot_m50 = createPivot(county, ['admin1', 'admin2', 'fips'], "m50")
county_pivot_m50index = createPivot(county, ['admin1', 'admin2', 'fips'], "m50_index")
county_output = county_pivot_samples.unionAll(county_pivot_m50).unionAll(county_pivot_m50index).orderBy(["fips","class"]).dropna().distinct()
county_output.coalesce(1).write.option("header", "true").csv("DL_mobility_county.csv")
