import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from flask import Flask, render_template, request
import os
from flask_cors import CORS

# Ensure matplotlib uses a non-interactive backend
import matplotlib
matplotlib.use('Agg')

# Flask app initialization
app = Flask(__name__)
CORS(app)

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
TOPICS = {
    "payment_method": "payment_method_analytics",
    "age_group": "age_group_analytics",
    "category": "category_analytics",
    "city": "city_analytics",
    "device": "device_analytics",
    "fraud": "fraud_analytics",
    "fraud_category": "fraud_category",
    "fraud_payment": "fraud_payment",
    "fraud_device": "fraud_device",
    "fraud_city": "fraud_city",
    "fraud_age": "fraud_age",
    "fraud_income": "fraud_income"
}

# Helper function to consume Kafka topics
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

def save_plot_fraud_category(data):
    categories = [d['category'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "categories": categories,
        "fraud_percentages": fraud_percentages
    }

def save_plot_fraud_payment(data):
    payment_methods = [d['payment_method'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "payment_methods": payment_methods,
        "fraud_percentages": fraud_percentages
    }

def save_plot_fraud_device(data):
    devices = [d['user_device'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "devices": devices,
        "fraud_percentages": fraud_percentages
    }

def save_plot_fraud_city(data):
    cities = [d['user_city'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "cities": cities,
        "fraud_percentages": fraud_percentages
    }

def save_plot_fraud_age(data):
    age_groups = [d['age_group'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "age_groups": age_groups,
        "fraud_percentages": fraud_percentages
    }

def save_plot_fraud_income(data):
    income_groups = [d['income_group'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "income_groups": income_groups,
        "fraud_percentages": fraud_percentages
    }

# Plot generation functions
def save_plot_payment_method(data):
    labels = [d['payment_method'] for d in data]
    values = [d['transaction_count'] for d in data]

    return {
        "labels": labels,
        "values": values
    }


def save_plot_age_group(data):
    labels = [d['age_group'] for d in data]
    values = [d['transaction_count'] for d in data]

    return {
        "labels": labels,
        "values": values
    }

def save_plot_category(data):
    categories = [d['category'] for d in data]
    transaction_counts = [d['transaction_count'] for d in data]
    avg_amounts = [d['avg_transaction_amount'] for d in data]
    return {
        "categories": categories,
        "transaction_counts": transaction_counts,
        "avg_amounts": avg_amounts

    }
def save_plot_city(data):
    cities = [d['user_city'] for d in data]
    transaction_counts = [d['transaction_count'] for d in data]

    return {
        "cities": cities,
        "transaction_counts": transaction_counts
    }

def save_plot_device(data):
    labels = [d['user_device'] for d in data]
    values = [d['transaction_count'] for d in data]

    return {
        "labels": labels,
        "values": values
    }

def save_plot_fraud(data):
    categories = [d['category'] for d in data]
    fraud_percentages = [d['fraud_percentage'] for d in data]

    return {
        "categories": categories,
        "fraud_percentages": fraud_percentages
    }

# Route to display dashboards
@app.route("/values")
def returnValues():
    query = request.args.get('query')
    if query in TOPICS:
        data = consume_topic(TOPICS[query])
        if query == "payment_method":
            return json.dumps(save_plot_payment_method(data))
        elif query == "age_group":
            return json.dumps(save_plot_age_group(data))
        elif query == "category":
            return json.dumps(save_plot_category(data))
        elif query == "city":
            return json.dumps(save_plot_city(data))
        elif query == "device":
            return json.dumps(save_plot_device(data))
        elif query == "fraud":
            return json.dumps(save_plot_fraud(data))
        elif query == "fraud_category":
            return json.dumps(save_plot_fraud_category(data))
        elif query == "fraud_payment":
            return json.dumps(save_plot_fraud_payment(data))
        elif query == "fraud_device":
            return json.dumps(save_plot_fraud_device(data))
        elif query == "fraud_city":
            return json.dumps(save_plot_fraud_city(data))
        elif query == "fraud_age":
            return json.dumps(save_plot_fraud_age(data))
        elif query == "fraud_income":
            return json.dumps(save_plot_fraud_income(data))
    return json.dumps({"error": "Invalid query parameter"})

@app.route('/')
def dashboard():
    return render_template("dashboard.html")

# Main function
if __name__ == "__main__":
    # Run the Flask app

    app.run(debug=True, host="0.0.0.0", port=3000)
