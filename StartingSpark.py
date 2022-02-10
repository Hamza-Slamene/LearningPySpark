

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import year, month, when, lit

spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

csvfile = "C:\\Users\\hslamene\\Desktop\\LearningPySpark\\resources\\concerts.csv"

df = spark.read.option("header", True).csv(csvfile)

selectedColumn = df.select("Date","Location","eventType","id")

dff = selectedColumn\
        .withColumn("year", year("Date").alias("year"))\
        .withColumn("mois", month("Date").alias("mois"))\

dff.printSchema()

df3 = dff.withColumn("month_name", 
     when(dff.mois == 1, lit("January"))\
    .when(dff.mois == 2, lit("February"))\
    .when(dff.mois == 3, lit("March"))\
    .when(dff.mois == 4, lit("April"))\
    .when(dff.mois == 5, lit("May"))\
    .when(dff.mois == 6, lit("June"))\
    .when(dff.mois == 7, lit("July"))\
    .when(dff.mois == 8, lit("Auguest"))\
    .when(dff.mois == 9, lit("September"))\
    .when(dff.mois == 10, lit("October"))\
    .when(dff.mois == 11, lit("Novermber"))\
    .when(dff.mois == 12, lit("December")))

df3.show(10, False)

spark.stop



