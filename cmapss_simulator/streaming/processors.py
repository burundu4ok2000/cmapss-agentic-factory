from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

class TelemetryShield:
    """
    Principal 'Immune System' for CMAPSS data pipeline.
    Advanced physics-aware, statistical, and SLA defense layers.
    """

    @staticmethod
    def anti_schema_rot(df: DataFrame) -> DataFrame:
        """
        Attack 1: Schema Rot.
        Detects NaN, Inf, or Nulls in critical calibration channels.
        """
        is_corrupt_col = (
            F.isnan(F.col("htBleed")) | 
            F.col("htBleed").cast("string").contains("Infinity") | 
            F.col("htBleed").isNull()
        )
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_corrupt_col, F.array_union(F.col("corruption_reason"), F.array(F.lit("SCHEMA_ROT"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_corrupt_col, F.lit(True)).otherwise(F.col("is_corrupted"))
        ).withColumn(
            "htBleed",
            F.when(is_corrupt_col, F.lit(None)).otherwise(F.col("htBleed"))
        )

    @staticmethod
    def anti_network_burst(df: DataFrame) -> DataFrame:
        """
        Attack 2: Network Burst.
        Deduplication of telemetry packets within a 10s watermark.
        """
        return df.withWatermark("timestamp", "10 seconds") \
                 .dropDuplicates(["unit_number", "time_cycles", "timestamp"])

    @staticmethod
    def anti_time_drift(df: DataFrame) -> DataFrame:
        """
        Attack 3: Time Drift.
        Rejection of spoofed historical data (>1h old).
        """
        one_hour_ago = F.current_timestamp() - F.expr("INTERVAL 1 HOUR")
        return df.filter(F.col("timestamp") >= one_hour_ago)

    @staticmethod
    def anti_out_of_bounds(df: DataFrame) -> DataFrame:
        """
        Attack 4: Thermocouple Break.
        Physics check: T50 cannot be lower than T2 during active operation.
        """
        is_break = (F.col("T50") < F.col("T2")) & (F.col("Nf") > 2000)
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_break, F.array_union(F.col("corruption_reason"), F.array(F.lit("THERMOCOUPLE_BREAK"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_break, F.lit(True)).otherwise(F.col("is_corrupted"))
        ).withColumn(
            "T50",
            F.when(is_break, F.lit(None)).otherwise(F.col("T50"))
        )

    @staticmethod
    def anti_sla_breach(df: DataFrame) -> DataFrame:
        """
        Attack 5: DSP Throttling.
        Detects artificial delays in message processing exceeding 5 minutes.
        """
        # Calculate latency in seconds
        latency = F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("timestamp"))
        is_sla_violation = latency > 300 # 5 minutes
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_sla_violation, F.array_union(F.col("corruption_reason"), F.array(F.lit("SLA_VIOLATION"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_sla_violation, F.lit(True)).otherwise(F.col("is_corrupted"))
        )

    @staticmethod
    def anti_sensor_freeze(df: DataFrame) -> DataFrame:
        """
        Attack 6: Sensor Freeze (P30 Icing).
        Detects static sensor values over a 5-step horizon while RPM is changing.
        """
        window = Window.partitionBy("unit_number").orderBy("timestamp")
        
        p30_lag1 = F.lag("P30", 1).over(window)
        p30_lag2 = F.lag("P30", 2).over(window)
        p30_lag3 = F.lag("P30", 3).over(window)
        p30_lag4 = F.lag("P30", 4).over(window)
        
        nc_lag4 = F.lag("Nc", 4).over(window)
        
        is_frozen = (F.col("P30") == p30_lag1) & \
                    (F.col("P30") == p30_lag2) & \
                    (F.col("P30") == p30_lag3) & \
                    (F.col("P30") == p30_lag4) & \
                    (F.col("Nc") != nc_lag4)
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_frozen, F.array_union(F.col("corruption_reason"), F.array(F.lit("SENSOR_FREEZE"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_frozen, F.lit(True)).otherwise(F.col("is_corrupted"))
        ).withColumn(
            "P30",
            F.when(is_frozen, F.lit(None)).otherwise(F.col("P30"))
        )

    @staticmethod
    def anti_adversarial_drift(df: DataFrame) -> DataFrame:
        """
        Attack 9: Adversarial Drift.
        Detects inverse correlation between Fuel Flow (phi) and T50.
        """
        window = Window.partitionBy("unit_number").orderBy("timestamp")
        
        phi_lag1 = F.lag("phi", 1).over(window)
        t50_lag1 = F.lag("T50", 1).over(window)
        
        is_drift = (F.col("phi") > phi_lag1) & (F.col("T50") < t50_lag1)
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_drift, F.array_union(F.col("corruption_reason"), F.array(F.lit("ADVERSARIAL_DRIFT"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_drift, F.lit(True)).otherwise(F.col("is_corrupted"))
        )

    @staticmethod
    def anti_zombie_state(df: DataFrame) -> DataFrame:
        """
        Attack 10: Zombie State (IT/OT Split Brain).
        Ensures that 'time_cycles' strictly increases for each unit_number.
        """
        window = Window.partitionBy("unit_number").orderBy("timestamp")
        prev_cycles = F.lag("time_cycles", 1).over(window)
        
        is_zombie = (F.col("time_cycles") < prev_cycles)
        
        return df.withColumn(
            "corruption_reason",
            F.when(is_zombie, F.array_union(F.col("corruption_reason"), F.array(F.lit("ZOMBIE_STATE_DETECTED"))))
             .otherwise(F.col("corruption_reason"))
        ).withColumn(
            "is_corrupted",
            F.when(is_zombie, F.lit(True)).otherwise(F.col("is_corrupted"))
        )

    def apply(self, df: DataFrame) -> DataFrame:
        """
        Orchestrates the 'Immune System' protection pipeline.
        """
        # Initialization
        if "is_corrupted" not in df.columns:
            df = df.withColumn("is_corrupted", F.lit(False))
        if "corruption_reason" not in df.columns:
            df = df.withColumn("corruption_reason", F.array().cast("array<string>"))
            
        # 1. Transport & Timing Layer
        df = self.anti_network_burst(df)
        df = self.anti_time_drift(df)
        df = self.anti_sla_breach(df)
        
        # 2. Integrity & Schema Layer
        df = self.anti_schema_rot(df)
        df = self.anti_zombie_state(df)
        
        # 3. Physics & Advanced layers
        df = self.anti_out_of_bounds(df)
        df = self.anti_sensor_freeze(df)
        
        if "phi" in df.columns:
            df = self.anti_adversarial_drift(df)
        
        return df
