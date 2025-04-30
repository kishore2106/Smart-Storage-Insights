# insights-processor/Dockerfile

FROM python:3.10-slim

# Install OpenJDK and curl
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command - process the last 24 hours of data
CMD ["python", "spark_job.py", "--hours-back=24"]