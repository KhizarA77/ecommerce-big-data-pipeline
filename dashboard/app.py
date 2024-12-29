import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Kafka Configuration
KAFKA_BROKER = "localhost:29092"
TOPICS = {
    "payment_method": "payment_method_analytics",
    "age_group": "age_group_analytics",
    "category": "category_analytics",
    "city": "city_analytics",
    "device": "device_analytics",
    "fraud": "fraud_analytics"
}

# Initialize Kafka consumers for each topic
def consume_topic(topic):
    try: 
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )
        data = [message.value for message in consumer]
        consumer.close()
        return data
    except Exception as e:
        print(f"Error consuming topic {topic}: {e}")
        return []
# Payment Method Analytics
def plot_payment_method(data):
    labels = [d['payment_method'] for d in data]
    values = [d['transaction_count'] for d in data]

    plt.figure(figsize=(8, 6))
    plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title("Payment Method Distribution")
    plt.show()

# Age Group Analytics
def plot_age_group(data):
    labels = [d['age_group'] for d in data]
    values = [d['transaction_count'] for d in data]

    plt.figure(figsize=(8, 6))
    plt.bar(labels, values, color='skyblue')
    plt.title("Age Group Distribution")
    plt.xlabel("Age Group")
    plt.ylabel("Transaction Count")
    plt.show()

# Category Analytics
def plot_category(data):
    categories = [d['category'] for d in data]
    transaction_counts = [d['transaction_count'] for d in data]
    avg_amounts = [d['avg_transaction_amount'] for d in data]

    plt.figure(figsize=(12, 6))
    plt.bar(categories, transaction_counts, color='lightcoral', label="Transaction Count")
    plt.title("Category Analytics - Transaction Count")
    plt.xlabel("Category")
    plt.ylabel("Transaction Count")
    plt.xticks(rotation=45)
    plt.legend()
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.plot(categories, avg_amounts, marker='o', label="Avg Transaction Amount")
    plt.title("Category Analytics - Avg Transaction Amount")
    plt.xlabel("Category")
    plt.ylabel("Avg Transaction Amount")
    plt.xticks(rotation=45)
    plt.legend()
    plt.show()

# City Analytics
def plot_city(data):
    cities = [d['user_city'] for d in data]
    transaction_counts = [d['transaction_count'] for d in data]

    plt.figure(figsize=(12, 6))
    plt.bar(cities, transaction_counts, color='goldenrod')
    plt.title("City-wise Transaction Count")
    plt.xlabel("City")
    plt.ylabel("Transaction Count")
    plt.xticks(rotation=45)
    plt.show()

# Device Analytics
def plot_device(data):
    labels = [d['user_device'] for d in data]
    values = [d['transaction_count'] for d in data]

    plt.figure(figsize=(8, 6))
    plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title("Device Usage Distribution")
    plt.show()

# Fraud Analytics
def plot_fraud(data):
    categories = [d['category'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    plt.figure(figsize=(12, 6))
    plt.bar(categories, fraud_percentages, color='firebrick')
    plt.title("Fraud Transaction Percentage by Category")
    plt.xlabel("Category")
    plt.ylabel("Fraud Percentage")
    plt.xticks(rotation=45)
    plt.show()

# Main function to consume data and generate plots
def main():
    print("Consuming Kafka topics...")
    data = {key: consume_topic(topic) for key, topic in TOPICS.items()}

    print("Generating dashboards...")
    plot_payment_method(data["payment_method"])
    plot_age_group(data["age_group"])
    plot_category(data["category"])
    plot_city(data["city"])
    plot_device(data["device"])
    plot_fraud(data["fraud"])

    print("Dashboards generated.")

if __name__ == "__main__":
    main()