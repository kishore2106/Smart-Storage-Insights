# dashboard-app/app.py

import streamlit as st
import pandas as pd
import pymongo
import psycopg2
import matplotlib.pyplot as plt
import altair as alt
import time
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from kafka import KafkaConsumer
import threading
import queue

# Set page configuration
st.set_page_config(
    page_title="SSIP - Smart Storage Insights Platform",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'storage_metrics'
MONGO_URI = 'mongodb://localhost:27017'
MONGO_DB = 'ssip'
MONGO_COLLECTION = 'storage_metrics'
POSTGRES_HOST = 'localhost'
POSTGRES_DB = 'storageinsights'
POSTGRES_USER = 'ssip'
POSTGRES_PASSWORD = 'ssip123'

# Create a queue to store Kafka messages
kafka_messages = queue.Queue(maxsize=100)

# Function to start Kafka consumer in a separate thread
def kafka_consumer_thread():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        for message in consumer:
            if not kafka_messages.full():
                kafka_messages.put((time.time(), message.value))
            else:
                # If queue is full, remove oldest message
                try:
                    kafka_messages.get_nowait()
                    kafka_messages.put((time.time(), message.value))
                except queue.Empty:
                    pass
    except Exception as e:
        st.error(f"Error in Kafka consumer thread: {e}")

# Start Kafka consumer thread
kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
kafka_thread.start()

# Connect to MongoDB
@st.cache_resource
def get_mongo_client():
    return MongoClient(MONGO_URI)

# Connect to PostgreSQL with better error handling
@st.cache_resource
def get_postgres_conn():
    try:
        # Try using the parameters from your Docker configuration
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,  # 'storageinsights' based on your Docker setup
            user=POSTGRES_USER,    # 'ssip' based on your Docker setup
            password=POSTGRES_PASSWORD  # 'ssip123' based on your Docker setup
        )
        return conn
    except psycopg2.OperationalError as e:
        error_message = str(e)
        
        # Check if it's an authentication error
        if "password authentication failed" in error_message:
            st.sidebar.error("PostgreSQL authentication failed. Double-check username and password.")
        # Check if it's a connection error
        elif "could not connect to server" in error_message:
            st.sidebar.error("Could not connect to PostgreSQL server. Make sure it's running.")
        # Check if database doesn't exist
        elif "database" in error_message and "does not exist" in error_message:
            st.sidebar.error(f"Database '{POSTGRES_DB}' does not exist. Check your configuration.")
            st.sidebar.info("You might need to create the database first.")
        # General error
        else:
            st.sidebar.error(f"PostgreSQL error: {error_message}")
            
        return None
    except Exception as e:
        st.sidebar.error(f"Unexpected error: {e}")
        return None

# Initialize PostgreSQL tables if they don't exist
def initialize_postgres_tables():
    conn = get_postgres_conn()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Create device_insights table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS device_insights (
                id SERIAL PRIMARY KEY,
                device_id VARCHAR(255) NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                avg_iops FLOAT,
                avg_latency FLOAT,
                avg_capacity_used FLOAT,
                max_iops FLOAT,
                max_latency FLOAT,
                max_capacity_used FLOAT,
                min_iops FLOAT,
                min_latency FLOAT,
                min_capacity_used FLOAT,
                metrics_count INTEGER,
                alerts TEXT
            );
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            st.sidebar.success("PostgreSQL tables initialized successfully!")
        except Exception as e:
            st.sidebar.error(f"Error initializing PostgreSQL tables: {e}")
    else:
        st.sidebar.warning("Cannot initialize tables: PostgreSQL connection failed.")

# Get recent metrics from MongoDB
@st.cache_data(ttl=5)  # Cache for 5 seconds
def get_recent_metrics(limit=50):
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Get most recent metrics
        metrics = list(collection.find().sort("timestamp", pymongo.DESCENDING).limit(limit))
        
        # Convert ObjectId to string for JSON serialization
        for metric in metrics:
            metric['_id'] = str(metric['_id'])
            
        return metrics
    except Exception as e:
        st.error(f"Error fetching data from MongoDB: {e}")
        return []

# Get insights from PostgreSQL
@st.cache_data(ttl=15)  # Cache for 15 seconds
def get_insights(hours=24):
    try:
        conn = get_postgres_conn()
        if not conn:
            return pd.DataFrame()
            
        query = """
        SELECT 
            device_id, 
            period_start, 
            period_end, 
            avg_iops, 
            avg_latency, 
            avg_capacity_used,
            max_iops,
            max_latency,
            max_capacity_used,
            min_iops,
            min_latency,
            min_capacity_used,
            metrics_count,
            alerts
        FROM device_insights
        WHERE period_end >= NOW() - INTERVAL %s HOUR
        ORDER BY period_end DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(hours,))
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching insights from PostgreSQL: {e}")
        return pd.DataFrame()

# Get metrics statistics from MongoDB
@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_metrics_stats(hours=24):
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Calculate time threshold
        time_threshold = datetime.now() - timedelta(hours=hours)
        timestamp_threshold = time_threshold.timestamp()
        
        # Get metrics
        metrics = list(collection.find({"timestamp": {"$gte": timestamp_threshold}}))
        
        if not metrics:
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(metrics)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        
        return df
    except Exception as e:
        st.error(f"Error getting metrics statistics: {e}")
        return pd.DataFrame()

# Get live kafka messages
@st.cache_data(ttl=1)  # Cache for 1 second
def get_live_kafka_messages():
    kafka_messages_list = []
    while not kafka_messages.empty():
        try:
            timestamp, message = kafka_messages.get_nowait()
            message['received_at'] = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
            kafka_messages_list.append(message)
        except queue.Empty:
            break
    return kafka_messages_list

# Function to create live charts
def create_live_charts(df):
    if df.empty:
        return None
        
    # Create three columns for charts
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # IOPS chart
        iops_chart = alt.Chart(df).mark_line().encode(
            x=alt.X('timestamp:T', title='Time'),
            y=alt.Y('iops:Q', title='IOPS'),
            color='device_id:N'
        ).properties(
            title='IOPS Over Time',
            width=300,
            height=300
        )
        st.altair_chart(iops_chart, use_container_width=True)
        
    with col2:
        # Latency chart
        latency_chart = alt.Chart(df).mark_line().encode(
            x=alt.X('timestamp:T', title='Time'),
            y=alt.Y('latency:Q', title='Latency (ms)'),
            color='device_id:N'
        ).properties(
            title='Latency Over Time',
            width=300,
            height=300
        )
        st.altair_chart(latency_chart, use_container_width=True)
        
    with col3:
        # Capacity chart
        capacity_chart = alt.Chart(df).mark_line().encode(
            x=alt.X('timestamp:T', title='Time'),
            y=alt.Y('capacity_used:Q', title='Capacity Used (GB)'),
            color='device_id:N'
        ).properties(
            title='Capacity Used Over Time',
            width=300,
            height=300
        )
        st.altair_chart(capacity_chart, use_container_width=True)

# Initialize PostgreSQL tables
initialize_postgres_tables()

# User interface
st.title("💾 Smart Storage Insights Platform (SSIP)")

# Create tabs
tab1, tab2, tab3 = st.tabs(["Live Metrics", "Historical Data", "Insights"])

with tab1:
    st.header("Live Storage Metrics Stream")
    
    # Display live Kafka messages
    st.subheader("Kafka Producer Stream")
    kafka_container = st.container()
    
    # Show last 10 messages (newest first)
    recent_kafka = get_live_kafka_messages()
    with kafka_container:
        for message in reversed(recent_kafka[:10]):
            st.write(f"**{message['received_at']}** - Device: {message['device_id']}, IOPS: {message['iops']}, Latency: {message['latency']}ms, Capacity: {message['capacity_used']}GB")
    
    # Display latest MongoDB data
    st.subheader("MongoDB Consumer Records")
    mongo_container = st.container()
    
    # Get recent metrics from MongoDB
    recent_metrics = get_recent_metrics(limit=50)
    
    # Display in table
    if recent_metrics:
        with mongo_container:
            metrics_df = pd.DataFrame(recent_metrics)
            metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'], unit='s')
            metrics_df = metrics_df.sort_values('timestamp', ascending=False)
            
            # Display only the most recent metrics
            display_df = metrics_df[['device_id', 'timestamp', 'iops', 'latency', 'capacity_used']].head(10)
            display_df.columns = ['Device ID', 'Timestamp', 'IOPS', 'Latency (ms)', 'Capacity Used (GB)']
            st.dataframe(display_df, use_container_width=True)
    
    # Create placeholder for charts
    chart_placeholder = st.container()
    
    # Create live charts if data is available
    if recent_metrics:
        with chart_placeholder:
            metrics_df = pd.DataFrame(recent_metrics)
            metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'], unit='s')
            create_live_charts(metrics_df[['device_id', 'timestamp', 'iops', 'latency', 'capacity_used']])

with tab2:
    st.header("Historical Storage Metrics")
    
    # Time range selector
    hours_back = st.slider("Select Hours to Look Back", 1, 72, 24)
    
    # Get historical data
    historical_data = get_metrics_stats(hours=hours_back)
    
    if not historical_data.empty:
        # Aggregate data by device and time (hourly) - Fix deprecated 'H' to 'h'
        historical_data['hour'] = historical_data['timestamp'].dt.floor('h')
        hourly_data = historical_data.groupby(['device_id', 'hour']).agg({
            'iops': 'mean',
            'latency': 'mean',
            'capacity_used': 'mean'
        }).reset_index()
        
        # Create hourly charts
        st.subheader("Hourly Trends")
        
        # IOPS chart
        fig_iops = px.line(
            hourly_data, 
            x='hour', 
            y='iops', 
            color='device_id',
            title='Average IOPS by Hour',
            labels={'hour': 'Time', 'iops': 'IOPS', 'device_id': 'Device'}
        )
        st.plotly_chart(fig_iops, use_container_width=True)
        
        # Latency chart
        fig_latency = px.line(
            hourly_data, 
            x='hour', 
            y='latency', 
            color='device_id',
            title='Average Latency by Hour',
            labels={'hour': 'Time', 'latency': 'Latency (ms)', 'device_id': 'Device'}
        )
        st.plotly_chart(fig_latency, use_container_width=True)
        
        # Capacity chart
        fig_capacity = px.line(
            hourly_data, 
            x='hour', 
            y='capacity_used', 
            color='device_id',
            title='Average Capacity Used by Hour',
            labels={'hour': 'Time', 'capacity_used': 'Capacity Used (GB)', 'device_id': 'Device'}
        )
        st.plotly_chart(fig_capacity, use_container_width=True)
        
        # Display statistics
        st.subheader("Device Statistics")
        device_stats = historical_data.groupby('device_id').agg({
            'iops': ['mean', 'min', 'max'],
            'latency': ['mean', 'min', 'max'],
            'capacity_used': ['mean', 'min', 'max']
        }).reset_index()
        
        device_stats.columns = ['Device ID', 
                                'Avg IOPS', 'Min IOPS', 'Max IOPS',
                                'Avg Latency', 'Min Latency', 'Max Latency',
                                'Avg Capacity', 'Min Capacity', 'Max Capacity']
        
        st.dataframe(device_stats, use_container_width=True)
    else:
        st.info("No historical data available. Make sure your producer has been running and data is being stored in MongoDB.")

with tab3:
    st.header("Storage Insights and Alerts")
    
    # Get insights from PostgreSQL
    insights_df = get_insights(hours=24)
    
    if not insights_df.empty:
        # Create device selector
        devices = insights_df['device_id'].unique()
        selected_device = st.selectbox("Select Device", devices)
        
        # Filter insights for selected device
        device_insights = insights_df[insights_df['device_id'] == selected_device]
        
        # Show key metrics
        st.subheader(f"Key Metrics for {selected_device}")
        
        # Create metric cards
        col1, col2, col3 = st.columns(3)
        
        with col1:
            latest_latency = device_insights['avg_latency'].iloc[0]
            st.metric(
                "Average Latency (ms)", 
                f"{latest_latency:.2f}",
                delta=f"{latest_latency - device_insights['avg_latency'].iloc[-1]:.2f}" if len(device_insights) > 1 else None
            )
            
        with col2:
            latest_iops = device_insights['avg_iops'].iloc[0]
            st.metric(
                "Average IOPS", 
                f"{latest_iops:.0f}",
                delta=f"{latest_iops - device_insights['avg_iops'].iloc[-1]:.0f}" if len(device_insights) > 1 else None
            )
            
        with col3:
            latest_capacity = device_insights['avg_capacity_used'].iloc[0]
            st.metric(
                "Average Capacity Used (GB)", 
                f"{latest_capacity:.2f}",
                delta=f"{latest_capacity - device_insights['avg_capacity_used'].iloc[-1]:.2f}" if len(device_insights) > 1 else None
            )
        
        # Create charts for the metrics over time
        st.subheader("Metrics Over Time")
        
        # Prepare data for time series - convert to proper datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(device_insights['period_end']):
            device_insights['period_end'] = pd.to_datetime(device_insights['period_end'])
            
        # Sort by time
        device_insights = device_insights.sort_values('period_end')
        
        # Create charts
        fig_insights = go.Figure()
        
        # Add traces
        fig_insights.add_trace(go.Scatter(
            x=device_insights['period_end'], 
            y=device_insights['avg_iops'],
            mode='lines+markers',
            name='IOPS'
        ))
        
        fig_insights.add_trace(go.Scatter(
            x=device_insights['period_end'], 
            y=device_insights['avg_latency'],
            mode='lines+markers',
            name='Latency (ms)',
            yaxis="y2"
        ))
        
        # Create a secondary Y-axis
        fig_insights.update_layout(
            title="IOPS and Latency Over Time",
            xaxis_title="Time",
            yaxis_title="IOPS",
            yaxis2=dict(
                title="Latency (ms)",
                overlaying="y",
                side="right"
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig_insights, use_container_width=True)
        
        # Display capacity usage separately
        fig_capacity = px.line(
            device_insights, 
            x='period_end', 
            y='avg_capacity_used',
            title='Average Capacity Used Over Time',
            labels={'period_end': 'Time', 'avg_capacity_used': 'Capacity Used (GB)'}
        )
        st.plotly_chart(fig_capacity, use_container_width=True)
        
        # Display alerts
        st.subheader("Alerts")
        
        # Process alerts column if it contains JSON strings
        alert_list = []
        for idx, row in device_insights.iterrows():
            timestamp = row['period_end']
            alerts = row['alerts']
            
            # Handle different alert formats (JSON string or list)
            if isinstance(alerts, str):
                try:
                    alerts = json.loads(alerts)
                except:
                    alerts = []
            elif not isinstance(alerts, list):
                alerts = []
                
            for alert in alerts:
                if alert:  # Only add non-empty alerts
                    alert_list.append({
                        'timestamp': timestamp,
                        'alert': alert,
                        'device_id': row['device_id']
                    })
        
        if alert_list:
            alert_df = pd.DataFrame(alert_list)
            st.dataframe(alert_df[['timestamp', 'alert']], use_container_width=True)
        else:
            st.info("No alerts generated for this device in the selected time period.")
            
        # Raw insights data
        with st.expander("Raw Insight Data"):
            st.dataframe(device_insights)
    else:
        st.info("No insights available. Make sure your Spark batch job has run and data is being stored in PostgreSQL.")

# Add refresh button for manually refreshing all data
if st.button('Refresh All Data'):
    st.experimental_rerun()

# Add auto-refresh checkbox
auto_refresh = st.sidebar.checkbox("Enable auto-refresh (5 seconds)", value=False)
if auto_refresh:
    time.sleep(5)
    st.experimental_rerun()

# Footer
st.sidebar.title("SSIP Dashboard")
st.sidebar.info(
    """
    **Smart Storage Insights Platform**
    
    This dashboard visualizes:
    - Live storage metrics from Kafka
    - Consumer data in MongoDB
    - Processed insights from batch jobs
    
    Data flows: Kafka → MongoDB → Spark → PostgreSQL
    """
)

# Add information about components
with st.sidebar.expander("System Components"):
    st.write("**Producer**: Sends simulated storage metrics to Kafka")
    st.write("**Consumer**: Reads from Kafka and stores in MongoDB")
    st.write("**Processor**: Spark job that analyzes MongoDB data")
    st.write("**Insights**: Generated insights stored in PostgreSQL")

# System status
with st.sidebar.expander("System Status"):
    # Check Kafka connection
    try:
        from kafka.admin import KafkaAdminClient
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        admin_client.close()
        kafka_status = "✅ Connected"
    except Exception as e:
        kafka_status = "❌ Not Connected"
    
    # Check MongoDB connection
    try:
        client = get_mongo_client()
        db = client[MONGO_DB]
        _ = db[MONGO_COLLECTION].count_documents({})
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = "❌ Not Connected"
    
    # Check PostgreSQL connection
    try:
        conn = get_postgres_conn()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            postgres_status = "✅ Connected"
        else:
            postgres_status = "❌ Not Connected"
    except Exception as e:
        postgres_status = "❌ Not Connected"
    
    st.write(f"**Kafka**: {kafka_status}")
    st.write(f"**MongoDB**: {mongo_status}")
    st.write(f"**PostgreSQL**: {postgres_status}")

# Display app version and last update
st.sidebar.text("Dashboard v1.0.0")
st.sidebar.text(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")