import csv
import random
from datetime import datetime, timedelta
from io import StringIO
from pyspark.dbutils import DBUtils

dbutils = DBUtils(spark)

def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"

def generate_timestamp():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 1, 13)
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

def generate_action_type():
    return random.choice(['page_view', 'edit', 'create', 'delete', 'share'])

def generate_page_id():
    return f"p{random.randint(1, 1000000):06d}"

def generate_duration_ms():
    return random.randint(100, 300000)

def generate_app_version():
    major = random.randint(5, 7)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)
    return f"{major}.{minor}.{patch}"

def generate_join_date():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 1, 10)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_country():
    return random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'MX'])

def generate_device_type():
    return random.choice(['iPhone', 'iPad', 'Android Phone', 'Android Tablet', 'Windows', 'Mac'])

def generate_subscription_type():
    return random.choice(['free', 'basic', 'premium', 'enterprise'])

def generate_user_interactions(num_records, dbfs_path):
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['user_id', 'timestamp', 'action_type', 'page_id', 'duration_ms', 'app_version'])
    for _ in range(num_records):
        writer.writerow([
            generate_user_id(),
            generate_timestamp().strftime("%Y-%m-%d %H:%M:%S"),
            generate_action_type(),
            generate_page_id(),
            generate_duration_ms(),
            generate_app_version()
        ])
    dbutils.fs.put(dbfs_path, output.getvalue(), overwrite=True)

def generate_user_metadata(num_records, dbfs_path):
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['user_id', 'join_date', 'country', 'device_type', 'subscription_type'])
    for _ in range(num_records):
        writer.writerow([
            generate_user_id(),
            generate_join_date().strftime("%Y-%m-%d"),
            generate_country(),
            generate_device_type(),
            generate_subscription_type()
        ])
    dbutils.fs.put(dbfs_path, output.getvalue(), overwrite=True)

# Generate sample datasets
generate_user_interactions(10000000, 'dbfs:/test/deepak/user_interactions_sample.csv')
generate_user_metadata(1000000, 'dbfs:/test/deepak/user_metadata_sample.csv')

print("Sample datasets generated successfully.")