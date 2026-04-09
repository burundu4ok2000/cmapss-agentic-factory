import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

os.environ['PYSPARK_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"

# Configuration
KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC = "engine_telemetry"
GCS_BUCKET = "cmapss-datalake-bucket"
GCP_CREDS = "/home/donald_trump/.google/credentials/my-credentials.json"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/telemetry/data/"

# Schema definition
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("unit_number", IntegerType(), True),
    StructField("time_cycles", IntegerType(), True),
    StructField("op_setting_1", DoubleType(), True),
    StructField("op_setting_2", DoubleType(), True),
    StructField("op_setting_3", DoubleType(), True),
    StructField("T2", DoubleType(), True),
    StructField("T24", DoubleType(), True),
    StructField("T30", DoubleType(), True),
    StructField("T50", DoubleType(), True),
    StructField("P2", DoubleType(), True),
    StructField("P15", DoubleType(), True),
    StructField("P30", DoubleType(), True),
    StructField("Nf", DoubleType(), True),
    StructField("Nc", DoubleType(), True),
    StructField("epr", DoubleType(), True),
    StructField("Ps30", DoubleType(), True),
    StructField("phi", DoubleType(), True),
    StructField("NRf", DoubleType(), True),
    StructField("NRc", DoubleType(), True),
    StructField("BPR", DoubleType(), True),
    StructField("farB", DoubleType(), True),
    StructField("htBleed", DoubleType(), True),
    StructField("Nf_dmd", DoubleType(), True),
    StructField("PCNfR_dmd", DoubleType(), True),
    StructField("W31", DoubleType(), True),
    StructField("W32", DoubleType(), True),
    StructField("processing_at", StringType(), True),
])

def main():
    spark = SparkSession.builder \
        .appName("CMAPSS-Final-Verification") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    print("🚀 Running Final Batch Ingestion...")
    
    # Read everything from topic
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # Parse JSON
    telemetry_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("is_corrupted", F.lit(False)) \
        .withColumn("corruption_reason", F.lit(None).cast("string")) \
        .withColumn("processing_date", F.to_date(F.current_timestamp()))

    # Write to GCS (increased partitions for scalability)
    telemetry_df.limit(100000).write \
        .mode("overwrite") \
        .partitionBy("processing_date", "unit_number") \
        .parquet(OUTPUT_PATH)

    print(f"✅ Data successfully flushed to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
