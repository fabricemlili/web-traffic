import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def extract_data_from_postgres():
    spark = SparkSession.builder \
        .config("spark.jars", "postgresql-42.2.5.jar") \
        .master("local") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://172.25.0.15:5432/web_traffic_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "web_traffic_logs") \
        .option("user", "user") \
        .option("password", "password") \
        .load()

    df.show(5)
    return df

def analyze_data(df):
    # Group by 'new_page_visited', sum 'time_spent', and rename the column
    df_total_time_spent = df.groupBy('new_page_visited') \
        .agg(F.sum('time_spent').alias('total_time_spent')) \
        .orderBy(F.col('total_time_spent').desc())

    # Calculate percentage
    total_time_spent_sum = df_total_time_spent.agg(F.sum('total_time_spent')).collect()[0][0]
    df_total_time_spent = df_total_time_spent.withColumn(
        'total_time_spent (%)',
        (F.col('total_time_spent') / total_time_spent_sum) * 100
    )

    df_total_time_spent.show()

if __name__ == "__main__":
    data = extract_data_from_postgres()
    analyze_data(data)

