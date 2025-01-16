# Insights: Data Engineering Challenge

## Background

As a leading digital note-taking app, has accumulated vast amounts of user interaction data. As a Senior Data Engineer, your task is to design and implement a highly optimized Spark-based system to process and analyze this data, deriving valuable insights about user behavior and app performance.

## Dataset

You are provided with two large datasets:

1. User Interactions (~ 1 TB, partitioned by date)
   - Schema: `(user_id: String, timestamp: Timestamp, action_type: String, page_id: String, duration_ms: Long, app_version: String)`
   - Example: `("u123", "2023-07-01 14:30:15", "page_view", "p456", 12000, "5.7.3")`

2. User Metadata (~ 100 GB, partitioned by country)
   - Schema: `(user_id: String, join_date: Date, country: String, device_type: String, subscription_type: String)`
   - Example: `("u123", "2022-01-15", "US", "iPad", "premium")`

Code to generate dataset:

```py
import csv
import random
from datetime import datetime, timedelta

def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"

def generate_timestamp():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
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
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_country():
    return random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'MX'])

def generate_device_type():
    return random.choice(['iPhone', 'iPad', 'Android Phone', 'Android Tablet', 'Windows', 'Mac'])

def generate_subscription_type():
    return random.choice(['free', 'basic', 'premium', 'enterprise'])

def generate_user_interactions(num_records, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
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

def generate_user_metadata(num_records, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['user_id', 'join_date', 'country', 'device_type', 'subscription_type'])
        for _ in range(num_records):
            writer.writerow([
                generate_user_id(),
                generate_join_date().strftime("%Y-%m-%d"),
                generate_country(),
                generate_device_type(),
                generate_subscription_type()
            ])

# Generate sample datasets
generate_user_interactions(1000000, 'user_interactions_sample.csv')
generate_user_metadata(100000, 'user_metadata_sample.csv')

print("Sample datasets generated successfully.")
```

## Tasks

1. Calculate Daily Active Users (DAU) and Monthly Active Users (MAU) for the past year:
   - Define clear criteria for what constitutes an "active" user
   - Implement a solution that scales efficiently for large datasets
   - [Optional] Explain how you:
     - Handle outliers and extremely long duration values, OR
     - Describe challenges you might face while creating similar metrics and how you would address them

2. Calculate session-based metrics:
   - Calculate metrics including:
     - Average session duration
     - Actions per session
   - Define clear criteria for what constitutes a "session"
   - [Optional] Provide analysis of the results if time permits

3. Spark UI Analysis

   a. Analyze the Spark UI for the above data pipeline job and identify any findings or bottlenecks:
      - Provide a detailed walkthrough (using screenshots or a screen recording with loom.com) of your analysis process
      - Identify specific areas for potential optimization based on the Spark UI data

   b. Explain or implement improvements based on your Spark UI analysis:
      - Choose one of your proposed optimizations and implement it (a high-level explanation via loom.com recording is also acceptable)
      - [Optional] Provide before and after comparisons of relevant Spark UI metrics to demonstrate the improvement

   c. Explain key areas for optimization:
      - For each identified bottleneck, provide a hypothesis about its cause and a proposed solution (a high-level explanation via loom.com recording is also acceptable)
      - [Optional] Discuss how you would validate the impact of your proposed optimizations

   NOTE: If you don't have time to document comprehensively, you can record a video walkthrough using loom.com to explain your findings, analysis, and answers to questions 4 and 5.

4. Data Processing and Optimization

   a. In the above Spark job, join the User Interactions and User Metadata datasets efficiently while handling data skew:
      - Explain your approach to mitigating skew in the join operation, considering that certain user_ids may be significantly more frequent
      - Implement and justify your choice of join strategy (e.g., broadcast join, shuffle hash join, sort merge join)

5. Design and implement optimizations for the Spark job to handle large-scale data processing efficiently:

   1. Analyze and optimize operations - Provide two files: pre-optimization and post-optimization
      - Provide a detailed analysis of the execution plan, identifying shuffle bottlenecks [a high-level analysis via loom.com recording is also acceptable]
      - Explain how changes are implemented to improve the plan
      - [Optional]: Justify your chosen approach with performance metrics

   2. Memory Management and Configuration:
      - Design a memory tuning strategy for executors and drivers
      - Document memory allocation calculations for different data scales (100GB, 500GB, 1TB)
      - [Optional]: Implement safeguards against OOM errors for skewed data

   3. Parallelism and Resource Utilization:
      - Determine optimal parallelism based on data volume and cluster resources
      - Implement dynamic partition tuning
      - Provide configuration recommendations for:
         - Number of executors
         - Cores per executor
         - Memory settings
         - Shuffle partitions
      - [Optional]: Include benchmarks comparing different configurations

   4. [Optional] Advanced optimization techniques:
      - Implement custom partitioning strategies for skewed keys
      - Design and implement data preprocessing steps to optimize downstream operations
      - [Optional]: Provide metrics showing the impact of your optimizations

## Requirements

1. Use Spark 3.x with Scala or PySpark.
2. Implement the solution using best practices for production-grade code.
3. Write unit tests for your key functions.
4. Provide a comprehensive README with setup instructions and explanations of your approach.
5. Include comments in your code explaining complex logic and optimization techniques.
6. Provide a system architecture diagram explaining how your solution would be deployed in a production environment.
7. Include a section on data quality checks and error handling in your implementation.
8. Discuss how you would schedule and orchestrate these jobs in a production setting.

## Deliverables

1. Source code for all implemented Spark jobs.
2. A detailed report (markdown format) covering:
   - Your approach to each task
   - Optimization techniques used
   - Analysis of results
   - Spark UI screenshots with explanations
   - Challenges faced and how you overcame them
3. Unit tests, if you have time you can write IT tests as well for key components of your solution.
4. A requirements.txt or build.sbt file listing all dependencies.
5. A system architecture diagram and explanation of the production deployment strategy.

NOTE: If you don't have time to document comprehensively, you can record a video walkthrough using loom.com to explain your findings, analysis, and answers to questions 4, 5, and 3c, which includes Spark cluster configuration recommendations and an explanation of the system architecture diagram.

## Evaluation Criteria

1. Correctness and completeness of the implemented solutions.
2. Efficiency and scalability of the Spark jobs.
3. Code quality, readability, and adherence to best practices.
4. Depth of analysis and insights derived from the data.
5. Appropriate use of Spark features and optimization techniques.
6. Quality of explanations

## Time Estimate

This challenge is designed to take approximately 3 to 6 hours for an experienced Spark developer. However, feel free to spend additional time if you wish to explore more advanced optimizations or analyses.

## Submission

Please submit your solution as a Git repository with all the required files and documentation. Ensure that your repo includes a clear history of commits showing your development process.

Good luck!