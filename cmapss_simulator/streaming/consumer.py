import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from processors import TelemetryShield

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelemetryConsumer")

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "engine_telemetry"
GCS_BUCKET = "cmapss-datalake-bucket"
OUTPUT_PATH = f"gs://{GCS_BUCKET}/telemetry/"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/telemetry_stream/"

# Schema definition for the incoming telemetry (ARINC 429 decoded or direct JSON)
# Based on Lead DE instructions, we expect these columns:
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("unit_number", IntegerType(), True),
    StructField("time_cycles", IntegerType(), True),
    StructField("htBleed", DoubleType(), True),
    # Additional sensors can be added here
    StructField("T2", DoubleType(), True),
    StructField("T50", DoubleType(), True),
    StructField("P30", DoubleType(), True),
    StructField("Nf", DoubleType(), True),
    StructField("Nc", DoubleType(), True),
    StructField("phi", DoubleType(), True),
])

def main():
    logger.info("Starting CMAPSS Immune System Consumer...")

    # Initialize Spark Session with Kafka and GCS support
    spark = SparkSession.builder \
        .appName("CMAPSS-Telemetry-Immune-System") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()

    # Read stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON body
    telemetry_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema).alias("data")) \
        .select("data.*")

    # Apply the TelemetryShield (Immune System)
    shield = TelemetryShield()
    shielded_df = shield.apply(telemetry_df)

    # Logging function for corrupted rows (used in foreachBatch)
    def process_batch(batch_df, batch_id):
        # Add processing metadata
        processed_batch = batch_df.withColumn("processing_date", F.to_date(F.current_timestamp()))
        
        corrupted_count = processed_batch.filter(F.col("is_corrupted") == True).count()
        if corrupted_count > 0:
            logger.warning(f"Batch {batch_id}: Detected {corrupted_count} corrupted rows (Morgoth Attack blocked)!")
        
        # Write to GCS (Parquet) with logical partitioning
        processed_batch.write \
            .format("parquet") \
            .partitionBy("processing_date", "unit_number") \
            .mode("append") \
            .save(OUTPUT_PATH)

    # Note: Since I'm using foreachBatch for logging, the checkpointing and trigger 
    # should be applied on the stream writing call.
    query = shielded_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
