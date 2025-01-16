from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta
from pyspark.sql.functions import broadcast

class UserAnalyticsOptimized:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def prepare_datasets(self, interactions_path: str, metadata_path: str):
        """Load and prepare datasets with optimizations."""
        # Load interactions dataset with range partitioning
        self.interactions_df = (
            self.spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv(interactions_path)
            .repartitionByRange(200, "user_id")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Load metadata and broadcast it
        self.metadata_df = broadcast(
            self.spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv(metadata_path)
        )

    def calculate_active_users(self, start_date: datetime):
        """Calculate Daily and Monthly Active Users."""
        # Daily Active Users (DAU)
        daily_active = (
            self.interactions_df
            .withColumn("date", F.to_date("timestamp"))
            .where(F.col("date") >= start_date)
            .groupBy("date")
            .agg(F.approx_count_distinct("user_id").alias("daily_active_users"))
        )

        # Monthly Active Users (MAU)
        monthly_window = Window.orderBy("date").rowsBetween(-30, 0)
        monthly_active = daily_active.withColumn(
            "monthly_active_users",
            F.sum("daily_active_users").over(monthly_window)
        )

        return {"daily_active": daily_active, "monthly_active": monthly_active}

    def calculate_session_metrics(self, session_timeout_minutes: int = 30):
        """Calculate session-based metrics."""
        window_spec = Window.partitionBy("user_id").orderBy("timestamp")

        # Mark session boundaries
        with_sessions = (
            self.interactions_df
            .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
            .withColumn(
                "time_diff_minutes",
                F.when(
                    F.col("prev_timestamp").isNotNull(),
                    (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 60
                ).otherwise(0)
            )
            .withColumn(
                "new_session",
                F.when(
                    (F.col("time_diff_minutes") > session_timeout_minutes) |
                    (F.col("prev_timestamp").isNull()),
                    1
                ).otherwise(0)
            )
            .withColumn(
                "session_id",
                F.concat(
                    F.col("user_id"),
                    F.lit("_"),
                    F.sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
                )
            )
        )

        # Session metrics
        session_metrics = (
            with_sessions
            .groupBy("user_id", "session_id")
            .agg(
                F.count("*").alias("actions_per_session"),
                F.min("timestamp").alias("session_start"),
                F.max("timestamp").alias("session_end")
            )
            .withColumn(
                "session_duration_hours",
                (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 3600
            )
        )

        # Filter sessions for outliers
        valid_sessions = session_metrics.filter(
            (F.col("session_duration_hours") <= 24) & (F.col("actions_per_session") >= 2)
        )

        return valid_sessions

def main():
    # Initialize Spark
    spark = (SparkSession.builder
             .appName("UserAnalyticsOptimized")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())

    analytics = UserAnalyticsOptimized(spark)

    # Load datasets
    analytics.prepare_datasets(
        "dbfs:/test/deepak/user_interactions_sample.csv",
        "dbfs:/test/deepak/user_metadata_sample.csv"
    )

    # Calculate DAU and MAU
    active_users = analytics.calculate_active_users(datetime.now() - timedelta(days=365))
    active_users["daily_active"].show()
    active_users["monthly_active"].show()

    # Calculate session metrics
    sessions = analytics.calculate_session_metrics()
    sessions.show()

if __name__ == "__main__":
    main()
