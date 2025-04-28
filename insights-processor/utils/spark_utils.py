# insights-processor/utils/spark_utils.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, collect_list, when, array
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, ArrayType
import logging
from datetime import datetime

from config import SPARK_APP_NAME, SPARK_MASTER, PERFORMANCE_THRESHOLDS

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create and configure a Spark session."""
    logger.info(f"Creating Spark session with app name: {SPARK_APP_NAME}")
    
    spark = (SparkSession.builder
             .appName(SPARK_APP_NAME)
             .master(SPARK_MASTER)
             .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,org.postgresql:postgresql:42.6.0")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate())
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def define_metrics_schema():
    """Define the schema for storage metrics data."""
    return StructType([
        StructField("_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", FloatType(), True),
        StructField("iops", IntegerType(), True),
        StructField("latency", FloatType(), True),
        StructField("capacity_used", FloatType(), True)
    ])


def define_insights_schema():
    """Define the schema for device insights data."""
    return StructType([
        StructField("device_id", StringType(), True),
        StructField("period_start", TimestampType(), True),
        StructField("period_end", TimestampType(), True),
        StructField("avg_iops", FloatType(), True),
        StructField("avg_latency", FloatType(), True),
        StructField("avg_capacity_used", FloatType(), True),
        StructField("max_iops", FloatType(), True),
        StructField("max_latency", FloatType(), True),
        StructField("max_capacity_used", FloatType(), True),
        StructField("min_iops", FloatType(), True),
        StructField("min_latency", FloatType(), True),
        StructField("min_capacity_used", FloatType(), True),
        StructField("metrics_count", IntegerType(), True),
        StructField("alerts", ArrayType(StringType()), True)
    ])


def generate_alerts(df):
    """Generate alerts based on thresholds."""
    # Define alert conditions
    high_latency_condition = col("avg_latency") > PERFORMANCE_THRESHOLDS["high_latency"]
    low_iops_condition = col("avg_iops") < PERFORMANCE_THRESHOLDS["low_iops"]
    high_capacity_condition = col("avg_capacity_used") > PERFORMANCE_THRESHOLDS["high_capacity"]
    
    # Create alerts array based on conditions
    return df.withColumn(
        "alerts",
        array(
            when(high_latency_condition, f"High average latency detected: > {PERFORMANCE_THRESHOLDS['high_latency']}ms"),
            when(low_iops_condition, f"Low average IOPS detected: < {PERFORMANCE_THRESHOLDS['low_iops']}"),
            when(high_capacity_condition, f"High capacity usage detected: > {PERFORMANCE_THRESHOLDS['high_capacity']}GB")
        )
    )


def process_metrics_batch(spark, metrics_df, start_time, end_time):
    """Process a batch of metrics and generate insights."""
    logger.info(f"Processing metrics batch from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
    
    # Convert Unix timestamp to datetime
    metrics_df = metrics_df.withColumn(
        "timestamp_dt", 
        (col("timestamp") / 1000).cast("timestamp")
    )
    
    # Aggregate metrics by device
    aggregated_df = (metrics_df
                    .groupBy("device_id")
                    .agg(
                        avg("iops").alias("avg_iops"),
                        avg("latency").alias("avg_latency"),
                        avg("capacity_used").alias("avg_capacity_used"),
                        max("iops").alias("max_iops"),
                        max("latency").alias("max_latency"),
                        max("capacity_used").alias("max_capacity_used"),
                        min("iops").alias("min_iops"),
                        min("latency").alias("min_latency"),
                        min("capacity_used").alias("min_capacity_used"),
                        count("*").alias("metrics_count")
                    ))
    
    # Add period start and end times
    period_df = (aggregated_df
                .withColumn("period_start", lit(datetime.fromtimestamp(start_time)))
                .withColumn("period_end", lit(datetime.fromtimestamp(end_time))))
    
    # Generate alerts based on thresholds
    insights_df = generate_alerts(period_df)
    
    # Filter out null values in alerts array
    insights_df = insights_df.withColumn(
        "alerts", 
        array_remove(col("alerts"), None)
    )
    
    return insights_df