# kafka_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# Use Docker service names for connectivity
KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC_NAME = "test_topic" 

def main():
    """
    Main function to run the Spark streaming job writing to Delta Lake.
    """
    print("Starting Spark streaming job...")
    # Configure Spark for Delta Lake
    spark = SparkSession.builder \
        .appName("DeltaLakeStreaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session created and configured.")

    # Read from Kafka topic as a stream
    print(f"Reading from Kafka topic: {TOPIC_NAME}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_NAME) \
        .load()
    
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Define the schema to match the Kafka producer's JSON structure.
    schema = StructType() \
        .add("id", IntegerType()) \
        .add("fname", StringType()) \
        .add("age", IntegerType()) \
        .add("country", StringType())
    
    json_df = value_df.withColumn("data", from_json(col("json_str"), schema)) \
        .select("data.*") \
        .withColumn("timestamp", current_timestamp())

    # --- Write to Delta Lake ---
    # The Delta table will be created at this path inside the container.
    delta_table_path = "/tmp/delta/people_data"

    print(f"Writing streaming data to Delta Lake at: {delta_table_path}")
    query = json_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{delta_table_path}/_checkpoints") \
        .start(delta_table_path)
    
    print("Spark streaming query started. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
