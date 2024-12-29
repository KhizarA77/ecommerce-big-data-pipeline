from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum, stddev, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json

KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "transactions"

def write_to_kafka(df, topic):
    df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .save()


def process_pipeline():
    spark = SparkSession.builder \
    .appName("ReadKafkaTransactions") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

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
    return casted_df


casted_df = process_pipeline()

fraud_category = casted_df.groupBy("category") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )

fraud_category.count()
print('fraud-category done')

fraud_payment = casted_df.groupBy("payment_method") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )
fraud_payment.count()
print('fraud-payment done')

fraud_device = casted_df.groupBy("user_device") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )
fraud_device.count()
print('fraud-device done')

fraud_city = casted_df.groupBy("user_city") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )
print('fraud-city done')

fraud_age = casted_df.withColumn("age_group", 
    when(col("user_age") < 20, "Under 20")
    .when((col("user_age") >= 20) & (col("user_age") < 30), "20-29")
    .when((col("user_age") >= 30) & (col("user_age") < 40), "30-39")
    .when((col("user_age") >= 40) & (col("user_age") < 50), "40-49")
    .when(col("user_age") >= 50, "50+")
) \
.groupBy("age_group") \
.agg(
    count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
    count("transaction_id").alias("total_transactions"),
    (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
)

fraud_age.count()
print('fraud-age done')

fraud_income = casted_df.groupBy("user_income") \
    .agg(
        count(when(col("fraud_label") == 1, 1)).alias("fraud_count"),
        count("transaction_id").alias("total_transactions"),
        (count(when(col("fraud_label") == 1, 1)) / count("transaction_id") * 100).alias("fraud_percentage")
    )
fraud_income.count()
print('fraud-income done')


fraud_category.show()
fraud_payment.show()
fraud_device.show()
fraud_city.show()
fraud_age.show()
fraud_income.show()