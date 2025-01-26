from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from typing import Dict
import logging
from pyspark.sql.functions import broadcast

class SparkConfigManager:
    """Manages Spark configurations for different data scales"""

    @staticmethod
    def get_config(data_scale_gb: int) -> Dict[str, str]:
        # Base configuration
        config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "auto",
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.broadcastTimeout": "600",
        }

        # Scale-specific configurations
        if data_scale_gb <= 100:
            config.update({
                "spark.executor.memory": "8g",
                "spark.executor.cores": "4",
                "spark.executor.instances": "10",
                "spark.driver.memory": "4g"
            })
        elif data_scale_gb <= 500:
            config.update({
                "spark.executor.memory": "16g",
                "spark.executor.cores": "5",
                "spark.executor.instances": "20",
                "spark.driver.memory": "8g"
            })
        else:  # 1TB or more
            config.update({
                "spark.executor.memory": "32g",
                "spark.executor.cores": "6",
                "spark.executor.instances": "40",
                "spark.driver.memory": "16g"
            })

        return config

class UserAnalytics:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def prepare_datasets(self, interactions_path: str, metadata_path: str):
        """Load and prepare datasets with optimizations"""
        # Load interactions with partitioning hint
        self.interactions_df = (
            self.spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv(interactions_path)
            .repartition(200, "user_id")  # Optimize for joins
            .cache()  # Cache for multiple uses
        )

        # Load metadata - broadcast hint for smaller dataset
        self.metadata_df = (
            broadcast(
                self.spark.read.option("inferSchema", "true")
                .option("header", "true")
                .csv(metadata_path)
            )  # Hint for join optimization
        )

    def calculate_active_users(self, start_date: datetime) -> Dict:
        """Calculate DAU and MAU metrics"""
        # Window for monthly calculations
        monthly_window = (
            Window.orderBy("date")
            .rowsBetween(-30, 0)  # 30-day rolling window
        )

        # Daily active users
        daily_active = (
            self.interactions_df
            .withColumn("date", F.to_date("timestamp"))
            .where(F.col("date") >= start_date)
            .groupBy("date")
            .agg(F.countDistinct("user_id").alias("daily_active_users"))
        )

        # Monthly active users
        monthly_active = (
            daily_active
            .withColumn("monthly_active_users",
                        F.sum("daily_active_users").over(monthly_window))
        )

        return {
            "daily_active": daily_active,
            "monthly_active": monthly_active
        }

    def calculate_session_metrics(self,
                                  session_timeout_minutes: int = 30,
                                  max_session_duration_hours: float = 24.0,
                                  min_actions_per_session: int = 2) -> Dict:
        """
        Calculate session-based metrics with enhanced outlier handling and validation
        
        Parameters:
        - session_timeout_minutes: Minutes of inactivity before starting new session
        - max_session_duration_hours: Maximum allowed session duration
        - min_actions_per_session: Minimum actions required for valid session
        """
        # Add session markers with enhanced timeout handling
        with_sessions = (
            self.interactions_df
            .withColumn("prev_timestamp",
                        F.lag("timestamp").over(Window.partitionBy("user_id")
                                                .orderBy("timestamp")))
            .withColumn("time_diff_minutes",
                        F.when(F.col("prev_timestamp").isNotNull(),
                               (F.unix_timestamp("timestamp") -
                                F.unix_timestamp("prev_timestamp")) / 60)
                        .otherwise(0))
            .withColumn("new_session",
                        F.when(
                            (F.col("time_diff_minutes") > session_timeout_minutes) |
                            (F.col("prev_timestamp").isNull()) |
                            (F.to_date("timestamp") != F.to_date("prev_timestamp")),
                            1
                        ).otherwise(0))
            .withColumn("session_id",
                        F.concat(
                            F.col("user_id"),
                            F.lit("_"),
                            F.sum("new_session").over(Window.partitionBy("user_id")
                                                      .orderBy("timestamp")
                                                      .rowsBetween(Window.unboundedPreceding, 0))
                        ))
        )

        # Calculate initial session metrics
        session_metrics = (
            with_sessions
            .groupBy("user_id", "session_id")
            .agg(
                F.count("*").alias("actions_per_session"),
                F.min("timestamp").alias("session_start"),
                F.max("timestamp").alias("session_end"),
                F.avg("time_diff_minutes").alias("avg_time_between_actions")
            )
            .withColumn("session_duration_hours",
                        (F.unix_timestamp("session_end") -
                         F.unix_timestamp("session_start")) / 3600)
        )

        # Apply session validation and outlier handling
        valid_sessions = session_metrics.filter(
            (F.col("session_duration_hours") <= max_session_duration_hours) &
            (F.col("actions_per_session") >= min_actions_per_session)
        )

        # Calculate outlier thresholds using IQR method
        stats = valid_sessions.select(
            F.percentile_approx("session_duration_hours", 0.25).alias("q1_duration"),
            F.percentile_approx("session_duration_hours", 0.75).alias("q3_duration"),
            F.percentile_approx("actions_per_session", 0.25).alias("q1_actions"),
            F.percentile_approx("actions_per_session", 0.75).alias("q3_actions")
        ).first()

        # Calculate IQR bounds
        iqr_duration = stats["q3_duration"] - stats["q1_duration"]
        iqr_actions = stats["q3_actions"] - stats["q1_actions"]

        duration_upper_bound = stats["q3_duration"] + (1.5 * iqr_duration)
        actions_upper_bound = stats["q3_actions"] + (1.5 * iqr_actions)

        # Apply outlier filtering
        clean_sessions = valid_sessions.filter(
            (F.col("session_duration_hours") <= duration_upper_bound) &
            (F.col("actions_per_session") <= actions_upper_bound)
        )

        # Calculate comprehensive metrics
        final_metrics = clean_sessions.agg(
            F.count("session_id").alias("total_valid_sessions"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("session_duration_hours").alias("avg_session_duration_hours"),
            F.stddev("session_duration_hours").alias("stddev_session_duration"),
            F.avg("actions_per_session").alias("avg_actions_per_session"),
            F.stddev("actions_per_session").alias("stddev_actions_per_session"),
            F.percentile_approx("session_duration_hours", [0.25, 0.5, 0.75])
            .alias("session_duration_quartiles"),
            F.percentile_approx("actions_per_session", [0.25, 0.5, 0.75])
            .alias("actions_quartiles")
        ).withColumn(
            "session_duration_quartiles", F.col("session_duration_quartiles").cast("string")
        ).withColumn(
            "actions_quartiles", F.col("actions_quartiles").cast("string")
        )

        # Time-based analysis
        time_metrics = clean_sessions.withColumn(
            "hour_of_day",
            F.hour(F.col("session_start"))
        ).withColumn(
            "day_of_week",
            F.dayofweek(F.col("session_start"))
        ).groupBy("hour_of_day", "day_of_week").agg(
            F.count("session_id").alias("session_count"),
            F.avg("session_duration_hours").alias("avg_duration"),
            F.avg("actions_per_session").alias("avg_actions")
        )

        # Log outlier statistics
        self.logger.info(f"Outlier thresholds - Duration: {duration_upper_bound:.2f} hours, "
                         f"Actions: {actions_upper_bound:.0f}")
        self.logger.info(f"Sessions removed: "
                         f"{session_metrics.count() - clean_sessions.count()}")

        return {
            "session_metrics": final_metrics,
            "detailed_sessions": clean_sessions,
            "time_metrics": time_metrics,
            "outlier_thresholds": {
                "duration_upper_bound": duration_upper_bound,
                "actions_upper_bound": actions_upper_bound
            }
        }

def main():
    # Initialize Spark with optimized configuration
    spark = (SparkSession.builder
             .appName("UserAnalytics")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    # Initialize analytics
    analytics = UserAnalytics(spark)

    # Load datasets from DBFS
    analytics.prepare_datasets(
        "dbfs:/test/deepak/user_interactions_sample.csv",
        "dbfs:/test/deepak/user_metadata_sample.csv"
    )

    # Calculate metrics
    active_users = analytics.calculate_active_users(
        datetime.now() - timedelta(days=365)
    )
    session_metrics = analytics.calculate_session_metrics()

    # Show results
    active_users["daily_active"].show()
    active_users["monthly_active"].show()
    session_metrics["session_metrics"].show()
    session_metrics["time_metrics"].show()

    # spark.stop()

if __name__ == "__main__":
    main()