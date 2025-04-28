#!/usr/bin/env python
# insights-processor/spark_job.py

import logging
import time
from datetime import datetime, timedelta
import argparse
import os
import sys

# Add parent directory to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BATCH_INTERVAL_MINUTES
from utils.db_utils import get_metrics_for_period, save_insights_to_postgres, create_postgres_tables_if_not_exist
from utils.spark_utils import create_spark_session
from processors.metrics_processor import MetricsProcessor
from processors.insights_generator import InsightsGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process storage metrics and generate insights')
    
    parser.add_argument(
        '--start-time',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        help='Start time for processing (format: YYYY-MM-DD HH:MM:SS)'
    )
    
    parser.add_argument(
        '--end-time',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        help='End time for processing (format: YYYY-MM-DD HH:MM:SS)'
    )
    
    parser.add_argument(
        '--hours-back',
        type=int,
        default=24,
        help='Number of hours to look back from current time (default: 24)'
    )
    
    return parser.parse_args()


def process_batch(start_time, end_time, spark_session):
    """Process a single batch of metrics between start_time and end_time."""
    logger.info(f"Processing batch: {start_time} to {end_time}")
    
    # Convert datetime to Unix timestamp
    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()
    
    # Fetch metrics from MongoDB
    metrics = get_metrics_for_period(start_timestamp, end_timestamp)
    
    if not metrics:
        logger.warning(f"No metrics found for period: {start_time} to {end_time}")
        return
    
    # Process metrics using Spark
    processor = MetricsProcessor(spark_session)
    metrics_df = processor.load_metrics_from_list(metrics)
    
    if metrics_df is None:
        return
    
    # Generate insights
    insights_df = processor.process_metrics(metrics_df, start_timestamp, end_timestamp)
    
    if insights_df is None:
        return
    
    # Save insights to PostgreSQL
    generator = InsightsGenerator(spark_session)
    
    # Optional: Enrich insights with additional metadata or trend analysis
    insights_df = generator.enrich_insights(insights_df)
    insights_df = generator.generate_trend_analysis(insights_df)
    
    # Save insights to PostgreSQL
    success = generator.save_insights_to_postgres(insights_df)
    
    if success:
        logger.info(f"Successfully processed batch: {start_time} to {end_time}")
    else:
        logger.error(f"Failed to save insights for batch: {start_time} to {end_time}")


def main():
    """Main entry point for the Spark job."""
    logger.info("Starting Storage Insights Processor")
    
    # Parse arguments
    args = parse_arguments()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Ensure PostgreSQL tables exist
        create_postgres_tables_if_not_exist()
        
        # Determine time range for processing
        if args.start_time and args.end_time:
            # Use provided time range
            start_time = args.start_time
            end_time = args.end_time
            logger.info(f"Using provided time range: {start_time} to {end_time}")
            
            # Process the entire time range as a single batch
            process_batch(start_time, end_time, spark)
            
        else:
            # Use hours-back parameter to determine time range
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=args.hours_back)
            logger.info(f"Processing last {args.hours_back} hours: {start_time} to {end_time}")
            
            # Split the time range into smaller batches
            batch_minutes = BATCH_INTERVAL_MINUTES
            batch_start = start_time
            
            while batch_start < end_time:
                batch_end = min(batch_start + timedelta(minutes=batch_minutes), end_time)
                process_batch(batch_start, batch_end, spark)
                batch_start = batch_end
        
        logger.info("Storage Insights Processor completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Storage Insights Processor: {str(e)}", exc_info=True)
        
    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()