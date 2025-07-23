from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, when
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
import mysql.connector
import time 

# Create Spark Session
spark = SparkSession.builder \
    .appName("PlayerPerformanceStreaming") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema for parsing Kafka JSON
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("event_type", StringType()) \
    .add("team", StringType()) \
    .add("player", StringType()) \
    .add("shots_taken", IntegerType()) \
    .add("goals_scored", IntegerType()) \
    .add("assists", IntegerType())  # Added assists to the schema for aggregation

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "player_performance") \
    .load()

# Parse Kafka message
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player", "data.shots_taken", "data.goals_scored", "data.assists") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# Aggregating player performance: goals, assists, shots taken
aggregated = parsed.groupBy(
    col("team"),
    col("player")
).agg(
    sum("shots_taken").alias("total_shots_taken"),  # Sum of shots taken
    sum("goals_scored").alias("total_goals_scored"),  # Sum of goals scored
    sum(when(col("event_type") == "assist", 1).otherwise(0)).alias("total_assists")
)

# Calculate percentage of scoring goals
aggregated = aggregated.withColumn(
    "scoring_percentage",
    when(col("total_shots_taken") > 0, (col("total_goals_scored") / col("total_shots_taken")) * 100).otherwise(0)
)

# Function to write aggregated data to MySQL
def write_to_mysql(df, batch_id):
    if df.isEmpty():
        return
    start_time = time.time()

    # Convert DataFrame to Pandas for easier processing
    pdf = df.toPandas()

    pdf.fillna({
        "total_shots_taken": 0,
        "total_goals_scored": 0,
        "total_assists": 0,
        "scoring_percentage": 0
    }, inplace=True)

    # Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",  # Replace with your actual password
        database="football"
    )
    cursor = conn.cursor()

    query = """
        INSERT INTO player_performance_stats (team, player, goals, assists, shots_taken, scoring_percentage)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            goals = VALUES(goals),
            assists = VALUES(assists),
            shots_taken = VALUES(shots_taken),
            scoring_percentage = VALUES(scoring_percentage)
    """

    for _, row in pdf.iterrows():
        cursor.execute(query, (
            row['team'],
            row['player'],
            float(row['total_goals_scored']),
            float(row['total_assists']),
            float(row['total_shots_taken']),
            float(row['scoring_percentage'])
        ))

    conn.commit()
    cursor.close()
    conn.close()

    end_time = time.time()  # ⏱ End timing
    print(f"✅ Batch {batch_id} processed in {end_time - start_time:.2f} seconds")

# Start the stream with foreachBatch to write to MySQL
query = aggregated.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .start()


query.awaitTermination()
