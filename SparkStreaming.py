
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType

spark = SparkSession.builder \
    .master("local") \
    .appName("Spark Streaming App") \
    .getOrCreate()

schema = StructType([ \
    StructField("Date",TimestampType(),True), \
    StructField("Location",StringType(),True), \
    StructField("Time",StringType(),True), \
    StructField("Venue", StringType(), True), \
    StructField("eventType", StringType(), True), \
    StructField("season", StringType(), True), \
    StructField("programID", IntegerType(), True), \
    StructField("orchestra", StringType(), True), \
    StructField("id", StringType(), True) \
  ])

stream = spark.readStream \
    .schema(schema)\
    .csv("C:\\Users\\hslamene\\Desktop\\LearningPySpark\\resources\\concerts.csv")



outputDStream = stream.filter("Location = 'Brooklyn, NY'")\
    .where("programID > 100")\
    .writeStream\
    .format("console")\
    .outputMode("update")\
    .start()\
    
outputDStream.awaitTermination()