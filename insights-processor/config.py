# insights-processor/config.py

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "ssip"
MONGO_COLLECTION = "storage_metrics"

# PostgreSQL Configuration
POSTGRES_URI = "jdbc:postgresql://localhost:5434/storageinsights"
POSTGRES_USER = "ssip"
POSTGRES_PASSWORD = "ssip123"
POSTGRES_DRIVER = "org.postgresql.Driver"

# Spark Configuration
SPARK_APP_NAME = "SSIP-Insights-Processor"
SPARK_MASTER = "local[*]"

# Processing Configuration
BATCH_INTERVAL_MINUTES = 60  # Process data in 1-hour batches
PERFORMANCE_THRESHOLDS = {
    "high_latency": 10.0,     # ms
    "low_iops": 500,          # IO operations per second
    "high_capacity": 800.0    # GB
}