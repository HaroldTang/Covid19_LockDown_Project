#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ue").config("spark.some.config.option", "some-value").getOrCreate()

industry = spark.read.format('csv').options(header='true',inferschema='true').load("/user/kc4152/hw5/unemployment_series_id.csv")
rate = spark.read.format('csv').options(header='true',inferschema='true').load("/user/kc4152/hw5/unemployment_rate.csv")
number = spark.read.format('csv').options(header='true',inferschema='true').load("/user/kc4152/hw5/unemployment_number.csv")

# industry = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
# rate = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

industry.createOrReplaceTempView("industry")
rate.createOrReplaceTempView("rate")
number.createOrReplaceTempView("number")


result1 = spark.sql("SELECT t.unemployment_industry, f.* FROM industry t JOIN rate f on t.series_id = f.`series id`")
result1 = result1.drop("series id", "Apr 2020", "May 2020", "Jun 2020", "Jul 2020", "Aug 2020", "Sep 2020", "Oct 2020", "Nov 2020", "Dec 2020")


result2 = spark.sql("SELECT t.unemployment_industry, f.* FROM industry t JOIN number f on t.series_id = f.`series id`")
result2 = result2.drop("series id", "Apr 2020", "May 2020", "Jun 2020", "Jul 2020", "Aug 2020", "Sep 2020", "Oct 2020", "Nov 2020", "Dec 2020")

result1.coalesce(1).write.option("header", "true").csv("rate_output")
result2.coalesce(1).write.option("header", "true").csv("number_output")
