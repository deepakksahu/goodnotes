# test_user_analytics.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
from user_analytics import UserAnalyticsOptimized  # assuming this is the file name

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("UserAnalyticsTest")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .getOrCreate())

@pytest.fixture(scope="function")
def analytics(spark, tmp_path):
    """Create test datasets and initialize UserAnalyticsOptimized."""
    # Create test interaction data
    interactions_data = [
        ("user1", "2024-01-01 10:00:00", "view", "page1", 120, "1.0.0"),
        ("user1", "2024-01-01 10:05:00", "edit", "page1", 300, "1.0.0"),
        ("user1", "2024-01-01 11:00:00", "view", "page2", 180, "1.0.0"),  # New session
        ("user2", "2024-01-01 09:00:00", "view", "page3", 240, "1.0.0"),
        ("user2", "2024-01-01 09:03:00", "edit", "page3", 420, "1.0.0")
    ]
    
    interactions_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("action_type", StringType(), False),
        StructField("page_id", StringType(), False),
        StructField("duration_ms", IntegerType(), False),
        StructField("app_version", StringType(), False)
    ])
    
    interactions_df = spark.createDataFrame(interactions_data, schema=interactions_schema)
    
    # Create test metadata
    metadata_data = [
        ("user1", "2023-01-01", "US", "desktop", "premium"),
        ("user2", "2023-06-01", "UK", "mobile", "free")
    ]
    
    metadata_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("join_date", StringType(), False),
        StructField("country", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("subscription_type", StringType(), False)
    ])
    
    metadata_df = spark.createDataFrame(metadata_data, schema=metadata_schema)
    
    # Save test data to temporary files
    interactions_path = str(tmp_path / "interactions.csv")
    metadata_path = str(tmp_path / "metadata.csv")
    
    interactions_df.write.csv(interactions_path, header=True, mode="overwrite")
    metadata_df.write.csv(metadata_path, header=True, mode="overwrite")
    
    # Initialize analytics class
    analytics = UserAnalyticsOptimized(spark)
    analytics.prepare_datasets(interactions_path, metadata_path)
    
    return analytics

class TestUserAnalytics:
    def test_prepare_datasets(self, analytics):
        """Test dataset preparation."""
        assert analytics.interactions_df is not None
        assert analytics.metadata_df is not None
        assert analytics.interactions_df.count() == 5
        assert analytics.metadata_df.count() == 2

    def test_calculate_active_users(self, analytics):
        """Test DAU and MAU calculations."""
        start_date = datetime(2024, 1, 1)
        results = analytics.calculate_active_users(start_date)
        
        # Test DAU
        daily_active = results["daily_active"]
        daily_counts = daily_active.collect()
        assert len(daily_counts) == 1  # One day in our test data
        assert daily_counts[0]["daily_active_users"] == 2  # Two unique users
        
        # Test MAU
        monthly_active = results["monthly_active"]
        monthly_counts = monthly_active.collect()
        assert len(monthly_counts) == 1  # One day in our test data
        assert monthly_counts[0]["monthly_active_users"] == 2  # Two unique users

    def test_calculate_session_metrics(self, analytics):
        """Test session metrics calculation."""
        sessions = analytics.calculate_session_metrics(session_timeout_minutes=30)
        session_data = sessions.collect()
        
        # Verify number of sessions
        assert len(session_data) == 3  # Should have 3 sessions total
        
        # Check session durations
        session_durations = {row["session_id"]: row["session_duration_hours"] 
                           for row in session_data}
        
        # Verify session boundaries
        for session in session_data:
            assert session["actions_per_session"] >= 1
            assert 0 <= session["session_duration_hours"] <= 24

    def test_session_timeout(self, analytics):
        """Test session timeout behavior."""
        # Test with different timeout values
        short_timeout = analytics.calculate_session_metrics(session_timeout_minutes=15)
        long_timeout = analytics.calculate_session_metrics(session_timeout_minutes=60)
        
        # Should have more sessions with shorter timeout
        assert short_timeout.count() >= long_timeout.count()

    def test_outlier_handling(self, analytics):
        """Test handling of outlier sessions."""
        sessions = analytics.calculate_session_metrics()
        
        # Verify no invalid sessions
        invalid_sessions = sessions.filter(
            (sessions.session_duration_hours > 24) | 
            (sessions.actions_per_session < 1)
        )
        
        assert invalid_sessions.count() == 0

@pytest.fixture(autouse=True)
def cleanup_spark(spark):
    """Cleanup Spark session after tests."""
    yield
    spark.stop()

# Helper function for comparing DataFrames
def assert_dataframe_equals(df1, df2, ordered=False):
    """Compare two DataFrames for equality."""
    if ordered:
        assert df1.collect() == df2.collect()
    else:
        assert df1.subtract(df2).count() == 0 and df2.subtract(df1).count() == 0

if __name__ == "__main__":
    pytest.main([__file__])
