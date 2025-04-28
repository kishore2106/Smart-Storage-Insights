#!/bin/bash
# insights-processor/run-spark-job.sh

# Set environment variables (if not using Docker)
export PYTHONPATH=$(pwd)

# Ensure the script is executable
chmod +x spark_job.py

# Run the Spark job with parameters
# Process the last 24 hours by default
python spark_job.py --hours-back=24

# To process a specific time range, uncomment and modify the following line:
# python spark_job.py --start-time="2023-04-26 00:00:00" --end-time="2023-04-27 00:00:00"