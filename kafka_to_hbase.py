import happybase

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

def main():
    spark = SparkSession.builder \
        .appName("KafkaToHBase_Batch") \
        .getOrCreate()

    kafka_bootstrap_servers = "kafka:9092"
    kafka_topic = "transactions"

    # 1) Define your schema
    json_schema = StructType([
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

    # 2) Batch read from Kafka
    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        # read from earliest to the latest offset as of the time this batch job starts
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # 3) Parse JSON
    from pyspark.sql.functions import from_json
    parsed_df = (
        df_kafka
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), json_schema).alias("data"))
        .select("data.*")
    )

    # 4) Cast numeric fields
    casted_df = (
        parsed_df
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("total_amount", col("total_amount").cast(DoubleType()))
        .withColumn("user_age", col("user_age").cast(IntegerType()))
        .withColumn("user_income", col("user_income").cast(IntegerType()))
        .withColumn("fraud_label", col("fraud_label").cast(IntegerType()))
    )

    # 5) Optional: Check how many rows
    total_rows = casted_df.count()
    print(f"Total rows read from Kafka: {total_rows}")

    # 6) Write to HBase
    #    We'll use an 'action' in Spark that triggers the HBase writes
    #    Easiest might be to do foreachPartition, so we can connect once per partition.
    def write_partition(iter_rows):
        # Open HBase connection once per partition
        connection = happybase.Connection(host='hbase-master', port=9090)
        table = connection.table('transaction_detail_HBase_tbl')

        def safe_encode(value):
            return str(value).encode() if value is not None else b''

        for row in iter_rows:
            table.put(
                safe_encode(row.transaction_id),
                {
                    b'transaction_data:user_id': safe_encode(row.user_id),
                    b'transaction_data:product_id': safe_encode(row.product_id),
                    b'transaction_data:category': safe_encode(row.category),
                    b'transaction_data:sub_category': safe_encode(row.sub_category),
                    b'transaction_data:price': safe_encode(row.price),
                    b'transaction_data:quantity': safe_encode(row.quantity),
                    b'transaction_data:total_amount': safe_encode(row.total_amount),
                    b'transaction_data:payment_method': safe_encode(row.payment_method),
                    b'transaction_data:timestamp': safe_encode(row.timestamp),
                    b'transaction_data:user_id': safe_encode(row.user_id),
                    b'customer_data:user_device': safe_encode(row.user_device),
                    b'customer_data:user_city': safe_encode(row.user_city),
                    b'customer_data:user_age': safe_encode(row.user_age),
                    b'customer_data:user_gender': safe_encode(row.user_gender),
                    b'customer_data:user_income': safe_encode(row.user_income),
                    b'customer_data:fraud_label': safe_encode(row.fraud_label),
                }
            )
        connection.close()

    # We collect partitions and write each partition to HBase
    casted_df.foreachPartition(write_partition)

    spark.stop()

if __name__ == "__main__":
    main()
