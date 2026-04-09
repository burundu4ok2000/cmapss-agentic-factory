import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelemetryConsumer")

# 🐍 PRINCIPAL DE ORCHESTRATION: Spark 4.1.1 Environment
os.environ['PYSPARK_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/home/linuxbrew/.linuxbrew/bin/python3"

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = "engine_telemetry"
GCS_BUCKET = "cmapss-datalake-bucket"
GCP_CREDS = "/home/donald_trump/.google/credentials/my-credentials.json"

# GCS Cloud Storage Paths
OUTPUT_PATH = f"gs://{GCS_BUCKET}/telemetry/"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/telemetry_stream/"

# 🛡️ TELEMETRY CONTRACT (matching Go-Code aecded6)
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
    logger.info("Initializing Finalized Spark Session (Spark 4.1.1 Clean Room)...")

    # 🚀 PRINCIPAL DE CONFIGURATION
    # Matching Kafka connector version with the execution engine to fix SerializedOffset error
    spark = SparkSession.builder \
        .appName("CMAPSS-Cloud-Ingestion") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.local.dir", "/tmp/spark-temp") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDS) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    logger.info("Subscribing to Air Traffic Control (Redpanda)...")

    # Read stream
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    telemetry_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

    # 🛡️ DATA LAKE COMPATIBILITY
    processed_df = telemetry_df \
        .withColumn("is_corrupted", F.lit(False)) \
        .withColumn("corruption_reason", F.array().cast("array<string>")) \
        .withColumn("processing_date", F.to_date(F.current_timestamp()))

    # Write to GCS (Sink)
    query = processed_df.writeStream \
        .format("parquet") \
        .partitionBy("processing_date", "unit_number") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    logger.info(f"Ingestion Active. Destination: {OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
