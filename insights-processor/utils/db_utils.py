# insights-processor/utils/db_utils.py

from pymongo import MongoClient
from datetime import datetime, timedelta
import psycopg2
import json
from typing import List, Dict, Any
import logging

from config import (
    MONGO_URI, MONGO_DB, MONGO_COLLECTION,
    POSTGRES_URI, POSTGRES_USER, POSTGRES_PASSWORD
)

logger = logging.getLogger(__name__)


def get_mongo_client():
    """Create and return a MongoDB client."""
    return MongoClient(MONGO_URI)


def get_metrics_for_period(start_time: float, end_time: float) -> List[Dict]:
    """Fetch metrics from MongoDB for the specified time period."""
    client = get_mongo_client()
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    query = {
        "timestamp": {
            "$gte": start_time,
            "$lt": end_time
        }
    }
    
    logger.info(f"Fetching metrics from MongoDB between {start_time} and {end_time}")
    metrics = list(collection.find(query))
    logger.info(f"Found {len(metrics)} metrics")
    
    # Convert MongoDB ObjectId to string to make it serializable
    for metric in metrics:
        if '_id' in metric:
            metric['_id'] = str(metric['_id'])
    
    return metrics


def save_insights_to_postgres(insights: List[Dict]):
    """Save processed insights to PostgreSQL."""
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="storageinsights",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        
        for insight in insights:
            # Convert alerts list to JSON string
            alerts_json = json.dumps(insight['alerts'])
            
            # SQL insert statement
            insert_query = """
            INSERT INTO device_insights (
                device_id, period_start, period_end, 
                avg_iops, avg_latency, avg_capacity_used,
                max_iops, max_latency, max_capacity_used,
                min_iops, min_latency, min_capacity_used,
                metrics_count, alerts
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Values to insert
            values = (
                insight['device_id'],
                insight['period_start'],
                insight['period_end'],
                insight['avg_iops'],
                insight['avg_latency'],
                insight['avg_capacity_used'],
                insight['max_iops'],
                insight['max_latency'],
                insight['max_capacity_used'],
                insight['min_iops'],
                insight['min_latency'],
                insight['min_capacity_used'],
                insight['metrics_count'],
                alerts_json
            )
            
            cursor.execute(insert_query, values)
        
        conn.commit()
        logger.info(f"Successfully saved {len(insights)} insights to PostgreSQL")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error saving insights to PostgreSQL: {str(e)}")
        raise
    
    finally:
        if conn:
            cursor.close()
            conn.close()


def create_postgres_tables_if_not_exist():
    """Create required PostgreSQL tables if they don't exist."""
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="storageinsights",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        
        # Create device_insights table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS device_insights (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            period_start TIMESTAMP NOT NULL,
            period_end TIMESTAMP NOT NULL,
            avg_iops FLOAT NOT NULL,
            avg_latency FLOAT NOT NULL,
            avg_capacity_used FLOAT NOT NULL,
            max_iops FLOAT NOT NULL,
            max_latency FLOAT NOT NULL,
            max_capacity_used FLOAT NOT NULL,
            min_iops FLOAT NOT NULL,
            min_latency FLOAT NOT NULL,
            min_capacity_used FLOAT NOT NULL,
            metrics_count INTEGER NOT NULL,
            alerts JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Index for faster querying by device_id
        CREATE INDEX IF NOT EXISTS idx_device_insights_device_id ON device_insights(device_id);
        
        -- Index for faster querying by time period
        CREATE INDEX IF NOT EXISTS idx_device_insights_period ON device_insights(period_start, period_end);
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("PostgreSQL tables created or already exist")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error creating PostgreSQL tables: {str(e)}")
        raise
    
    finally:
        if conn:
            cursor.close()
            conn.close()