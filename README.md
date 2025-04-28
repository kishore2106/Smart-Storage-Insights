# Smart Storage Insights Platform (SSIP)

A comprehensive data engineering platform that collects storage usage metrics, processes them in real-time and batch modes, stores data in both NoSQL and SQL databases, and generates actionable insights for system administrators.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Setup Instructions](#setup-instructions)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
- [Running the Application](#running-the-application)
  - [Start Services](#start-services)
  - [Run the Metrics Generator](#run-the-metrics-generator)
  - [Monitor Kafka Consumer](#monitor-kafka-consumer)
  - [Execute Spark Jobs](#execute-spark-jobs)
- [Key Features](#key-features)
- [Monitoring](#monitoring)
- [Performance Metrics](#performance-metrics)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🌟 Overview

The Smart Storage Insights Platform (SSIP) addresses the challenge of efficiently monitoring and managing storage resources in large-scale environments. It provides both real-time monitoring and historical analysis of storage metrics, enabling administrators to:

- Detect high usage patterns and potential bottlenecks
- Analyze historical trends for capacity planning
- Generate detailed reports on storage utilization across systems

## 🏗️ Architecture

The platform follows a modern data engineering architecture with the following components:

1. **Metrics Generator**: Simulates storage metrics from multiple systems using Python
2. **Kafka Pipeline**: Provides real-time data ingestion and processing
3. **Storage Consumer**: Python-based microservice to process streaming data
4. **Insights Processor**: Spark-based batch processing for historical data analysis
5. **Databases**: MongoDB for metric documents and PostgreSQL for structured reports
6. **Visualization**: Data visualization through dashboards (optional)

![Architecture Diagram](docs/images/architecture-diagram.png)

## 💻 Technology Stack

- **Languages**: Python
- **Messaging**: Apache Kafka
- **Processing**: Apache Spark
- **Storage**: MongoDB (NoSQL), PostgreSQL (SQL)
- **Containerization**: Docker, Kubernetes
- **DevOps**: Unix Shell Scripting, GitHub Actions (CI/CD)
- **Optional**: Basic GenAI model for insights generation

## 🚀 Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Kubernetes cluster (local or cloud-based)
- Python 3.8 or higher
- Apache Kafka
- Apache Spark
- MongoDB
- PostgreSQL

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/smart-storage-insights.git
   cd smart-storage-insights
   ```

2. Install Python dependencies for the metrics generator:
   ```bash
   cd metrics-generator
   pip install -r requirements.txt
   cd ..
   ```

3. Install Python dependencies for the storage consumer:
   ```bash
   cd storage-consumer
   pip install -r requirements.txt
   cd ..
   ```

4. Install Python dependencies for the insights processor:
   ```bash
   cd insights-processor
   pip install -r requirements.txt
   cd ..
   ```

5. Build Docker images:
   ```bash
   docker-compose build
   ```

### Configuration

1. Configure Kafka connection in `config.py`:
   ```python
   KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
   KAFKA_TOPIC = "storage-metrics"
   ```

2. Set up database connections in `.env` file:
   ```
   MONGO_URI=mongodb://localhost:27017/storage_metrics
   POSTGRES_CONNECTION=postgresql://user:password@localhost:5432/insights_db
   ```

3. Configure Spark settings in `insights-processor/spark_job.py`:
   ```python
   # Spark configuration settings
   spark_conf = SparkConf().setAppName("StorageInsightsProcessor")
   ```

## 🏃‍♂️ Running the Application

### Start Services

1. Start Kafka, MongoDB, and PostgreSQL using Docker Compose:
   ```bash
   docker-compose up -d kafka mongodb postgres
   ```

2. Check that services are running:
   ```bash
   docker-compose ps
   ```

### Run the Metrics Generator

1. Start the metrics generator to simulate storage data:
   ```bash
   cd metrics-generator
   python producer.py
   ```

2. Verify data is being published to Kafka:
   ```bash
   docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic storage-metrics --from-beginning
   ```

### Monitor Kafka Consumer

1. Run the Python consumer service:
   ```bash
   cd storage-consumer
   docker-compose up storage-consumer
   ```

2. Check the logs to ensure metrics are being processed:
   ```bash
   docker-compose logs -f storage-consumer
   ```

### Execute Spark Jobs

1. Run the Spark batch processing job:
   ```bash
   cd insights-processor
   ./run-spark-job.sh
   ```

2. Monitor the Spark job execution:
   ```bash
   # Check Spark UI at http://localhost:4040
   ```

## 🔑 Key Features

- **Real-time Monitoring**: Streams storage metrics with low latency
- **Batch Processing**: Analyzes historical data for trend identification
- **Visualization**: Dashboard for data visualization and monitoring
- **Scalability**: Kubernetes-based deployment for horizontal scaling
- **Resiliency**: Fault-tolerant processing with automatic recovery

## 📊 Monitoring and Alerts

The platform provides several monitoring capabilities:

- **Usage Thresholds**: Configurable thresholds for storage capacity warnings
- **Performance Monitoring**: Track I/O latency and throughput metrics

## 📈 Performance Metrics

The SSIP platform has been tested with the following performance characteristics:

- **Throughput**: Can process up to 10,000 metrics per second
- **Latency**: Average processing time of <100ms for real-time metrics
- **Storage Efficiency**: Optimized data storage with compression
- **Scalability**: Linear scaling with additional workers

## 🛠️ Troubleshooting

Common issues and their solutions:

- **Kafka Connection Issues**: Check network connectivity and broker status
- **MongoDB Connection Failures**: Verify credentials and connection string
- **Spark Job Failures**: Examine logs in the Spark UI for errors
- **Data Processing Delays**: Check consumer lag metrics in Kafka

## 👥 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## 📜 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📷 Screenshots

Below are screenshots demonstrating the key features and functionality of the Smart Storage Insights Platform:

### Dashboard Overview
![Dashboard Overview](docs/images/dashboard-overview.png)
*The main dashboard showing real-time storage metrics across systems*

### Metrics Visualization
####
![Average IOPS by Hour](docs/images/average_iops_by_hour.png)
*Interactive charts showing storage usage trends over time*

![Average Latency By Hour](docs/images/average_latency_by_hour.png)
*Interactive charts showing storage usage trends over time*

![Average Capacity used by Hour](docs/images/average_capacity_used_by_hour.png)
*Interactive charts showing storage usage trends over time*