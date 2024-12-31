from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType



# def write_to_kafka(df, topic):
#     df.selectExpr("to_json(struct(*)) AS value") \
#         .writeStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#         .option("topic", topic) \
#         .option("checkpointLocation", f"/tmp/{topic}_checkpoints") \
#         .outputMode("complete") \
#         .start()



# Create Spark session
spark = SparkSession.builder \
    .appName("ReadKafkaTransactions") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
    # .config("spark.sql.shuffle.partitions", "100") \

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "transactions"
AGE_GROUP_TOPIC = "age_group_analytics"
CATEGORY_TOPIC = "category_analytics"
CITY_TOPIC = "city_analytics"
DEVICE_TOPIC = "device_analytics"
FRAUD_TOPIC = "fraud_analytics"
INCOME_TOPIC = "income_analytics"
PAYMENT_METHOD_TOPIC = "payment_method_analytics"

# Define Schema
schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_device", StringType(), True),
        StructField("user_city", StringType(), True),
        StructField("user_age", StringType(), True),
        StructField("user_gender", StringType(), True),
        StructField("user_income", StringType(), True),
        StructField("fraud_label", StringType(), True),
    ])

# Read data from Kafka
raw_stream = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

print("Reading from Kafka")

# Deserialize JSON messages
from pyspark.sql.functions import from_json
parsed_df = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), schema).alias("data"))
    .select("data.*")
    )

casted_df = (
        parsed_df
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("user_age", col("user_age").cast(IntegerType()))
        .withColumn("user_income", col("user_income").cast(IntegerType()))
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))
    )
# Show parsed stream schema
# casted_df.printSchema()

from pyspark.sql import functions as F

print("Processing data")

# 1. Aggregates by category
category_analytics = casted_df.groupBy("category") \
    .agg(
        F.avg("total_amount").alias("avg_transaction_amount"),
        F.count("transaction_id").alias("transaction_count"),
        F.max("total_amount").alias("max_transaction_amount"),
        F.min("total_amount").alias("min_transaction_amount")
    )

category_analytics.count() 
print("Category Analytics done")

# 2. Payment Method Distribution
payment_method_distribution = casted_df.groupBy("payment_method") \
    .agg(F.count("transaction_id").alias("transaction_count"))

payment_method_distribution.count()
print("Payment Method Distribution done")

# 3. Age Distribution (bucketed into ranges)
age_distribution = casted_df.withColumn("age_group", 
    F.when(col("user_age") < 20, "Under 20")
     .when((col("user_age") >= 20) & (col("user_age") < 30), "20-29")
     .when((col("user_age") >= 30) & (col("user_age") < 40), "30-39")
     .when((col("user_age") >= 40) & (col("user_age") < 50), "40-49")
     .when(col("user_age") >= 50, "50+")
) \
.groupBy("age_group") \
.agg(F.count("transaction_id").alias("transaction_count"))
age_distribution.count()
print("Age Distribution done")

# 4. City-Wise Transaction Count
city_transaction_count = casted_df.groupBy("user_city") \
    .agg(F.count("transaction_id").alias("transaction_count"))

city_transaction_count.count()
print("City-Wise Transaction Count done")

# 5. Device Usage Distribution
device_usage = casted_df.groupBy("user_device") \
    .agg(F.count("transaction_id").alias("transaction_count"))

device_usage.count()
print("Device Usage Distribution done")

# 6. Income vs. Average Spending
# income_spending = casted_df.groupBy("user_income") \
#     .agg(F.avg("total_amount").alias("avg_transaction_amount"))

# income_spending.count()
# print("Income vs. Average Spending done")

# 7. Fraud Transaction Percentage by Category
fraud_analytics = casted_df.groupBy("category") \
    .agg(
        (F.sum(F.when(col("fraud_label") == 1, 1).otherwise(0)) / F.count("transaction_id") * 100).alias("fraud_percentage")
    )
fraud_analytics.count()

print("Fraud Transaction Percentage by Category done")



print("All Analytics done")


def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .save()

# Write category analytics to Kafka
write_to_kafka(category_analytics, CATEGORY_TOPIC)

# Write payment method analytics to Kafka
write_to_kafka(payment_method_distribution, PAYMENT_METHOD_TOPIC)

# Write age group analytics to Kafka
write_to_kafka(age_distribution, AGE_GROUP_TOPIC)

# Write city transaction analytics to Kafka
write_to_kafka(city_transaction_count, CITY_TOPIC)

# Write device usage analytics to Kafka
write_to_kafka(device_usage, DEVICE_TOPIC)

# Write income vs. average spending analytics to Kafka
# write_to_kafka(income_spending, INCOME_TOPIC)

# Write fraud analytics to Kafka
write_to_kafka(fraud_analytics, FRAUD_TOPIC)

# Await termination for all streams
spark.streams.awaitAnyTermination()


# Print data from parsed_stream
# casted_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start() \
#     .awaitTermination()

# Write to Kafka





