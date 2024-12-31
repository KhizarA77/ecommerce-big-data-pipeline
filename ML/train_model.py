from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
INPUT_TOPIC = "transactions"

spark = SparkSession.builder \
    .appName("ReadKafkaTransactions") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
def process_pipeline():

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




# Parse Kafka messages
transactions = process_pipeline()

# Preprocess data
# Index categorical columns
categorical_columns = ["category", "sub_category", "payment_method", "user_device", "user_city", "user_gender"]
indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in categorical_columns]

# Assemble features
assembler = VectorAssembler(
    inputCols=["price", "quantity", "total_amount", "user_age", "user_income"] + [f"{col}_index" for col in categorical_columns],
    outputCol="features"
)

# Train a Random Forest Classifier
from pyspark.ml.classification import LogisticRegression

# Replace Random Forest with Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="fraud_label", maxIter=10)

# Create a pipeline
pipeline = Pipeline(stages=indexers + [assembler, lr])

# Train-test split
(training_data, test_data) = transactions.randomSplit([0.8, 0.2], seed=42)

# Train the model
model = pipeline.fit(training_data)

# Evaluate the model
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="fraud_label", metricName="areaUnderROC")
roc_auc = evaluator.evaluate(predictions)
print(f"ROC AUC: {roc_auc}")

# Save the trained model
model.write().overwrite().save("/spark_models/fraud_detection_model")
print("Model saved to /spark_models/fraud_detection_model")

# Stop Spark session
spark.stop()
