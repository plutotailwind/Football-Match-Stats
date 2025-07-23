from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType
import mysql.connector
import time

# Create Spark Session
spark = SparkSession.builder \
    .appName("FoulStatsStreaming") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for parsing Kafka JSON
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("event_type", StringType()) \
    .add("team", StringType()) \
    .add("player", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "team_stats_1min") \
    .load()

# Parse and filter for fouls
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .filter("event_type = 'foul'")

# Aggregate foul count per team in 1-minute windows
windowed = parsed.groupBy(
    window(col("event_time"), "1 minute"),
    col("team")
).count().withColumnRenamed("count", "fouls") \
 .withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .drop("window")

# Define the foreachBatch logic to write to MySQL
def write_to_mysql(df, batch_id):
    if df.isEmpty():
        return
    start_time = time.time()

    # Convert Spark DataFrame to Pandas
    pandas_df = df.toPandas()

    # Format datetime columns to MySQL-compatible string format
    pandas_df['window_start'] = pandas_df['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    pandas_df['window_end'] = pandas_df['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",  # ⬅ Replace with your actual password
        database="football"
    )
    cursor = conn.cursor()

    # MySQL insert query
    query = """
        INSERT INTO foul_stats (team, fouls, window_start, window_end)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            fouls = VALUES(fouls),
            window_end = VALUES(window_end)
    """


    # Insert rows
    for _, row in pandas_df.iterrows():
        #print("DEBUG ROW:", row.to_dict())  # Helpful for debugging

        cursor.execute(query, (
            row['team'],
            int(row['fouls']),
            row['window_start'],
            row['window_end']
        ))


    conn.commit()
    cursor.close()
    conn.close()

    end_time = time.time()  # ⏱ End timing
    print(f"✅ Batch {batch_id} processed in {end_time - start_time:.2f} seconds")

# Start the query with foreachBatch
query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
