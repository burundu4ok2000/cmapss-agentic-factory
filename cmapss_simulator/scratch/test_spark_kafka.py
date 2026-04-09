import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"

spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

print("Reading from Kafka...")
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "engine_telemetry") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print(f"Total rows in Kafka: {df.count()}")
df.show(5)
