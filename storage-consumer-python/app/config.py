# app/config.py

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'    # CONNECT INSIDE NETWORK to kafka broker
KAFKA_TOPIC = 'storage_metrics'             # Kafka topic you want to consume
KAFKA_GROUP_ID = 'ssip-group'                # Kafka consumer group ID

# MongoDB Configuration
MONGO_URI = 'mongodb://localhost:27017'          # CONNECT INSIDE NETWORK to MongoDB
MONGO_DB = 'ssip'                            # MongoDB Database name
MONGO_COLLECTION = 'storage_metrics'         # Collection name

# CONNECT INSIDE NETWORK
# KAFKA_BOOTSTRAP_SERVERS = 'broker:9092'    # CONNECT INSIDE NETWORK to kafka broker
# MONGO_URI = 'mongodb://mongo:27017'          # CONNECT INSIDE NETWORK to MongoDB