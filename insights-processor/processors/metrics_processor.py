# insights-processor/processors/metrics_processor.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_unixtime, col, lit, array_remove
import logging
from datetime import datetime
from pyspark.sql import functions as fn

from config import PERFORMANCE_THRESHOLDS
from utils.spark_utils import generate_alerts

logger = logging.getLogger(__name__)


class MetricsProcessor:
    """Process storage metrics and generate insights."""
    
    def __init__(self, spark_session):
        """Initialize with a Spark session."""
        self.spark = spark_session
    
    def load_metrics_from_list(self, metrics_list):
        """Convert a list of metrics to a Spark DataFrame."""
        if not metrics_list:
            logger.warning("No metrics provided to process")
            return None
        
        return self.spark.createDataFrame(metrics_list)
    
    def process_metrics(self, metrics_df, start_time, end_time):
        """Process a batch of metrics and generate insights."""
        logger.info(f"Processing metrics batch from {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        
        if metrics_df is None or metrics_df.count() == 0:
            logger.warning("No metrics to process")
            return None
        
        # Convert timestamp to datetime if it's not already
        if "timestamp_dt" not in metrics_df.columns:
            metrics_df = metrics_df.withColumn(
                "timestamp_dt", 
                from_unixtime(col("timestamp"))
            )
        
        # Aggregate metrics by device
        # aggregated_df = (metrics_df
        #                 .groupBy("device_id")
        #                 .agg(
        #                     {"iops": "avg", "latency": "avg", "capacity_used": "avg",
        #                      "iops": "max", "latency": "max", "capacity_used": "max",
        #                      "iops": "min", "latency": "min", "capacity_used": "min",
        #                      "*": "count"})
        #                 .withColumnRenamed("avg(iops)", "avg_iops")
        #                 .withColumnRenamed("avg(latency)", "avg_latency")
        #                 .withColumnRenamed("avg(capacity_used)", "avg_capacity_used")
        #                 .withColumnRenamed("max(iops)", "max_iops")
        #                 .withColumnRenamed("max(latency)", "max_latency")
        #                 .withColumnRenamed("max(capacity_used)", "max_capacity_used")
        #                 .withColumnRenamed("min(iops)", "min_iops")
        #                 .withColumnRenamed("min(latency)", "min_latency")
        #                 .withColumnRenamed("min(capacity_used)", "min_capacity_used")
        #                 .withColumnRenamed("count(1)", "metrics_count"))

        # Inside the process_metrics method in MetricsProcessor class
# Change this code:

        # Replace the aggregation step with this:
        aggregated_df = (metrics_df
                        .groupBy("device_id")
                        .agg(
                            fn.avg("iops").alias("avg_iops"),
                            fn.avg("latency").alias("avg_latency"), 
                            fn.avg("capacity_used").alias("avg_capacity_used"),
                            fn.max("iops").alias("max_iops"),
                            fn.max("latency").alias("max_latency"),
                            fn.max("capacity_used").alias("max_capacity_used"),
                            fn.min("iops").alias("min_iops"),
                            fn.min("latency").alias("min_latency"),
                            fn.min("capacity_used").alias("min_capacity_used"),
                            fn.count("*").alias("metrics_count")
                        ))
        
        # Add period start and end times
        start_datetime = datetime.fromtimestamp(start_time)
        end_datetime = datetime.fromtimestamp(end_time)
        
        period_df = (aggregated_df
                    .withColumn("period_start", lit(start_datetime))
                    .withColumn("period_end", lit(end_datetime)))
        
        # Generate alerts
        insights_df = self._generate_alerts(period_df)
        
        return insights_df
    
    def _generate_alerts(self, df):
        """Generate alerts based on predefined thresholds."""
        # Define alert conditions
        from pyspark.sql.functions import when, array
        
        # Create alerts array based on conditions
        with_alerts = df.withColumn(
            "alerts",
            array(
                when(col("avg_latency") > PERFORMANCE_THRESHOLDS["high_latency"], 
                     f"High average latency detected: {col('avg_latency')}ms (threshold: {PERFORMANCE_THRESHOLDS['high_latency']}ms)"),
                when(col("avg_iops") < PERFORMANCE_THRESHOLDS["low_iops"], 
                     f"Low average IOPS detected: {col('avg_iops')} (threshold: {PERFORMANCE_THRESHOLDS['low_iops']})"),
                when(col("avg_capacity_used") > PERFORMANCE_THRESHOLDS["high_capacity"], 
                     f"High capacity usage detected: {col('avg_capacity_used')}GB (threshold: {PERFORMANCE_THRESHOLDS['high_capacity']}GB)")
            )
        )
        
        # Filter out null values in alerts array
        return with_alerts.withColumn(
            "alerts", 
            array_remove(col("alerts"), None)
        )