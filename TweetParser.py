import imp
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import year, month, when, lit, regexp_replace


spark = SparkSession.builder \
    .master("local") \
    .appName("Word Count") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

csvfile = "C:\\Users\\hslamene\\Desktop\\LearningPySpark\\resources\\TinderSwindlerVersion2.csv"

tweets = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .csv(csvfile)

tweets.printSchema()

proctweets = tweets.select("user_name", "text")

withoutEmoji = proctweets\
    .withColumn("username", regexp_replace(proctweets.user_name, """[^ 'a-zA-Z0-9,.?!]""", ""))\
    .withColumn("texte", regexp_replace(proctweets.text, """[^ 'a-zA-Z0-9,.?!]""", "").alias("texte"))\
    .withColumn("texte", regexp_replace(proctweets.text, """https.*.co/.*""", "").alias("texte"))\
    .drop("user_name")\
    .drop("text")

withoutEmoji.select("texte").show(20, False)