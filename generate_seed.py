from pyspark.sql import SparkSession
from datetime import datetime
import os

spark = SparkSession.builder.master("local[1]").appName("Seed").getOrCreate()
data = [{
    "timestamp": datetime.now(),
    "unit_number": 1,
    "time_cycles": 1,
    "htBleed": 0.0,
    "T2": 518.67,
    "T50": 641.82,
    "P30": 554.43,
    "Nf": 2388.0,
    "Nc": 8138.62,
    "phi": 0.0,
    "is_corrupted": False,
    "corruption_reason": []
}]
df = spark.createDataFrame(data)
df.write.mode("overwrite").parquet("seed_data")
print("Parquet seed data generated in seed_data/")
