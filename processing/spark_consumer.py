from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import psycopg2

def save_to_postgres(df):
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="web_traffic_db",
        user="user",
        password="password",
        host="172.25.0.15",  # PostgreSQL service IP
        port="5432"
    )
    cursor = conn.cursor()

    # Write data to PostgreSQL
    for row in df.collect():
        cursor.execute(
            """
            INSERT INTO web_traffic_logs (user_id, session_id, new_page_visited, time_spent, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (row.user_id, row.session_id, row.new_page_visited, row.time_spent, row.timestamp)
        )
    conn.commit()
    cursor.close()
    conn.close()

def spark_function():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Kafka Spark Streaming Consumer") \
        .getOrCreate()
    
    TOPIC_NAME = 'web-traffic-logs'

    # Define the schema for the incoming data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("new_page_visited", StringType(), True),
        StructField("time_spent", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Streaming DataFrame from Kafka topic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.25.0.12:9092,172.25.0.13:9092") \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # Deserialize JSON data from Kafka topic
    json_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # Call the save_to_postgres function for every batch of data
    query = json_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df)) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    spark_function()

