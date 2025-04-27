# run.py

from app.consumer import StorageMetricConsumer

def main():
    consumer = StorageMetricConsumer()
    consumer.consume()

if __name__ == "__main__":
    main()
