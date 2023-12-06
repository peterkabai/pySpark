from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("Dataframe Example").getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

print(df)

mtcars = spark.read.csv("mtcars.csv")
print(mtcars)

mtcars = spark.read.option('header', 'true').csv("mtcars.csv")
print(mtcars)

mtcars.printSchema()

print(" *** Spark SQL Section *** ")
# Some Spark SQL operations on the mtcars df
gearGroups = mtcars.groupby('gear').count()
gearGroups.show()

withSum = mtcars.join(gearGroups, 'gear', 'inner')
withSum.show()

# filter meathod: requires a confition and returns a boolean
withSum.filter(withSum.gear == 3).show()

withSum.filter(withSum.gear == 3).select("model", "gear").show()