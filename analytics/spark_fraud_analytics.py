import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, stddev, min as spark_min, max as spark_max, corr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml import Pipeline

def main():
    spark = SparkSession.builder \
        .appName("KafkaFraudAnalytics") \
        .getOrCreate()

    # ----------------------------------------------------------------------------
    # 1. Read from Kafka source
    # ----------------------------------------------------------------------------
    kafka_bootstrap_servers = "kafka:9092"
    input_topic = "transactions"
    output_topic = "analytics"

    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # ----------------------------------------------------------------------------
    # 2. Define JSON schema for the transactions
    # ----------------------------------------------------------------------------
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

    parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # ----------------------------------------------------------------------------
    # 3. Cast columns to correct types
    # ----------------------------------------------------------------------------
    casted_df = parsed_df \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("quantity", col("quantity").cast(IntegerType())) \
        .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
        .withColumn("user_age", col("user_age").cast(IntegerType())) \
        .withColumn("user_income", col("user_income").cast(IntegerType())) \
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))

    # Drop rows where fraud_label is null
    casted_df = casted_df.na.drop(subset=["fraud_label"])

    # If there's no data, we might short-circuit
    if casted_df.count() == 0:
        print("No valid data found in Kafka for analytics. Exiting.")
        spark.stop()
        return

    # ----------------------------------------------------------------------------
    # 4. Basic Descriptive Statistics
    #    (mean, stddev, min, max for numeric columns)
    # ----------------------------------------------------------------------------
    numeric_cols = ["price", "quantity", "total_amount", "user_age", "user_income"]
    summary_stats = {}
    for col_name in numeric_cols:
        stats_df = casted_df.agg(
            avg(col_name).alias("mean"),
            stddev(col_name).alias("stddev"),
            spark_min(col_name).alias("min"),
            spark_max(col_name).alias("max")
        )
        row = stats_df.collect()[0]
        summary_stats[col_name] = {
            "mean": row["mean"],
            "stddev": row["stddev"],
            "min": row["min"],
            "max": row["max"]
        }

    # ----------------------------------------------------------------------------
    # 5. Aggregations: Fraud Rate by Different Dimensions
    # ----------------------------------------------------------------------------
    # 5.1. Fraud Rate by Device
    fraud_by_device_df = casted_df.groupBy("user_device").avg("fraud_label")
    fraud_by_device = {row["user_device"]: row["avg(fraud_label)"] for row in fraud_by_device_df.collect()}

    # 5.2. Fraud Rate by City
    fraud_by_city_df = casted_df.groupBy("user_city").avg("fraud_label")
    fraud_by_city = {row["user_city"]: row["avg(fraud_label)"] for row in fraud_by_city_df.collect()}

    # 5.3. Fraud Rate by Category
    fraud_by_category_df = casted_df.groupBy("category").avg("fraud_label")
    fraud_by_category = {row["category"]: row["avg(fraud_label)"] for row in fraud_by_category_df.collect()}

    # 5.4. Fraud Rate by Payment Method
    fraud_by_paymethod_df = casted_df.groupBy("payment_method").avg("fraud_label")
    fraud_by_paymethod = {row["payment_method"]: row["avg(fraud_label)"] for row in fraud_by_paymethod_df.collect()}

    # 5.5. Top 10 Subcategories with Highest Fraud Rate
    #      (assuming enough data)
    subcat_fraud_df = casted_df.groupBy("sub_category").avg("fraud_label").alias("fraud_rate")
    top10_subcat = subcat_fraud_df.orderBy(col("avg(fraud_label)").desc()).limit(10)
    subcat_fraud_list = [(r["sub_category"], r["avg(fraud_label)"]) for r in top10_subcat.collect()]

    # ----------------------------------------------------------------------------
    # 6. Correlation Among Numeric Columns and Fraud
    # ----------------------------------------------------------------------------
    correlations = {}
    for col_name in numeric_cols:
        c = casted_df.stat.corr(col_name, "fraud_label")
        correlations[col_name] = c

    # ----------------------------------------------------------------------------
    # 7. Machine Learning Models
    # ----------------------------------------------------------------------------
    # We'll do both Logistic Regression and RandomForest for classification

    # Index categorical features
    payment_indexer = StringIndexer(inputCol="payment_method", outputCol="payment_method_idx", handleInvalid="keep")
    device_indexer = StringIndexer(inputCol="user_device", outputCol="user_device_idx", handleInvalid="keep")
    gender_indexer = StringIndexer(inputCol="user_gender", outputCol="user_gender_idx", handleInvalid="keep")

    assembler_cols = [
        "payment_method_idx", "user_device_idx", "user_gender_idx",
        "user_age", "user_income", "price", "quantity", "total_amount"
    ]

    assembler = VectorAssembler(
        inputCols=assembler_cols,
        outputCol="features"
    )

    # 7.1 Logistic Regression
    lr = LogisticRegression(featuresCol="features", labelCol="fraud_label", maxIter=10)
    lr_pipeline = Pipeline(stages=[payment_indexer, device_indexer, gender_indexer, assembler, lr])

    train_df, test_df = casted_df.randomSplit([0.7, 0.3], seed=42)

    lr_model = lr_pipeline.fit(train_df)
    lr_preds = lr_model.transform(test_df)
    lr_correct = lr_preds.where(col("fraud_label") == col("prediction")).count()
    lr_total = lr_preds.count()
    lr_accuracy = lr_correct / lr_total if lr_total > 0 else 0.0

    # Feature importances in logistic regression => coefficients
    # The last stage in the pipeline is the logistic model
    lr_stage = lr_model.stages[-1]
    lr_feature_importances = {
        assembler_cols[i]: float(coeff)
        for i, coeff in enumerate(lr_stage.coefficients.toArray())
    }

    # 7.2 Random Forest
    rf = RandomForestClassifier(featuresCol="features", labelCol="fraud_label", numTrees=20)
    rf_pipeline = Pipeline(stages=[payment_indexer, device_indexer, gender_indexer, assembler, rf])

    rf_model = rf_pipeline.fit(train_df)
    rf_preds = rf_model.transform(test_df)
    rf_correct = rf_preds.where(col("fraud_label") == col("prediction")).count()
    rf_total = rf_preds.count()
    rf_accuracy = rf_correct / rf_total if rf_total > 0 else 0.0

    # Feature importances
    # The last stage in the pipeline is the RandomForest model
    rf_stage = rf_model.stages[-1]
    rf_importance_values = rf_stage.featureImportances.toArray()
    rf_feature_importances = {
        assembler_cols[i]: float(val)
        for i, val in enumerate(rf_importance_values)
    }

    # ----------------------------------------------------------------------------
    # 8. Consolidate All Analytics into One JSON
    # ----------------------------------------------------------------------------
    analytics_result = {
        "summary_stats": summary_stats,
        "fraud_by_device": fraud_by_device,
        "fraud_by_city": fraud_by_city,
        "fraud_by_category": fraud_by_category,
        "fraud_by_payment_method": fraud_by_paymethod,
        "top10_subcategories_by_fraud": subcat_fraud_list,
        "correlations_with_fraud": correlations,
        "logistic_regression": {
            "accuracy": lr_accuracy,
            "feature_importances": lr_feature_importances
        },
        "random_forest": {
            "accuracy": rf_accuracy,
            "feature_importances": rf_feature_importances
        }
    }

    # Convert to JSON string
    analytics_json_str = json.dumps(analytics_result, indent=2)

    # ----------------------------------------------------------------------------
    # 9. Publish This JSON to the 'analytics' Kafka Topic
    # ----------------------------------------------------------------------------
    # We'll create a single-row DataFrame with columns "key" and "value"
    # that Spark can write to a Kafka sink.
    analytics_df = spark.createDataFrame(
        [(None, analytics_json_str)],  # single row
        ["key", "value"]
    )

    analytics_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .save()

    print("Successfully wrote analytics to Kafka topic 'analytics'.")

    spark.stop()

if __name__ == "__main__":
    main()
