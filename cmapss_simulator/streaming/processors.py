from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

class TelemetryShield:
    """
    'Immune System' for the CMAPSS data pipeline.
    Protects the Data Lake from Morgoth's telemetry-based attacks.
    """

    @staticmethod
    def anti_schema_rot(df: DataFrame) -> DataFrame:
        """
        Attack 1: Schema Rot. 
        Morgoth injects NaN or Inf into the 'htBleed' column to break downstream analytics.
        We flag these rows and nullify the corrupt values.
        """
        # Identify NaN or Infinite values
        is_corrupt_col = (
            F.isnan(F.col("htBleed")) | 
            F.col("htBleed").cast("string").contains("Infinity") | 
            F.col("htBleed").isNull()
        )
        
        return df.withColumn(
            "is_corrupted", 
            F.when(is_corrupt_col, True).otherwise(F.col("is_corrupted") if "is_corrupted" in df.columns else False)
        ).withColumn(
            "htBleed",
            F.when(is_corrupt_col, F.lit(None)).otherwise(F.col("htBleed"))
        )

    @staticmethod
    def anti_network_burst(df: DataFrame) -> DataFrame:
        """
        Attack 2: Network Burst.
        Morgoth floods the broker with identical duplicate frames.
        Spark uses watermarking to drop duplicates in a 10s window.
        """
        # Ensure we have a watermark for deduplication to work in streaming
        return df.withWatermark("timestamp", "10 seconds") \
                 .dropDuplicates(["unit_number", "time_cycles", "timestamp"])

    @staticmethod
    def anti_time_drift(df: DataFrame) -> DataFrame:
        """
        Attack 3: Time Drift ('GPS 2006').
        Morgoth historical-spoofs timestamps. We reject anything older than 1 hour.
        """
        one_hour_ago = F.current_timestamp() - F.expr("INTERVAL 1 HOUR")
        return df.filter(F.col("timestamp") >= one_hour_ago)

    def apply(self, df: DataFrame) -> DataFrame:
        """Sequential application of the defensive layers."""
        # Initialize is_corrupted if not exists
        if "is_corrupted" not in df.columns:
            df = df.withColumn("is_corrupted", F.lit(False))
            
        df = self.anti_schema_rot(df)
        df = self.anti_network_burst(df)
        df = self.anti_time_drift(df)
        
        return df
