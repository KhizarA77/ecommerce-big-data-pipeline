# Ecommerce Big Data Pipeline

## Overview

This project involves Big Data Generation, Storage, Processing, Dashboarding, and Machine Learning.

## Prerequisites

Ensure you have the following installed on your system:
- Docker
- NodeJS
- Python

## Getting Started

1. **Start the Cluster**
    ```sh
    docker-compose up -d
    ```

2. **Create Kafka Topic**
    ```sh
    # Create topic transactions in Kafka
    ```

3. **Run Kafka Producer**
    ```sh
    cd kafka-producer
    node producer.js
    ```

4. **Create HBase Table**
    ```sh
    create 'transaction_detail_HBase_tbl', 'transaction_data', 'customer_data'
    ```

5. **Setup Spark Master Container**
    ```sh
    # Bash into spark-master container and run:
    apt update
    apt install build-essential
    pip install happybase
    ```

6. **Run Kafka to HBase Script**
    ```sh
    # Copy kafka_to_hbase to spark-master container and run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 kafka_to_hbase.py
    ```

    Data should now be in HBase.

## Kafka Topics

Create the following topics in Kafka:
- `age_group_analytics`
- `category_analytics`
- `city_analytics`
- `device_analytics`
- `fraud_age`
- `fraud_analytics`
- `fraud_category`
- `fraud_city`
- `fraud_device`
- `fraud_income`
- `fraud_payment`
- `payment_method_analytics`

## Generate Insights

Run the following scripts in the spark-master container:
```sh
# Fraud Analysis
generate-insights/fraud_analysis.py

# Read Kafka
generate-insights/read_kafka.py
```

## Dashboard

Once the above steps are completed, the dashboard will be available at:
```
http://localhost:3000
```