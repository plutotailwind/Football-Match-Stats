from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import TimestampType
import time 

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FoulStatsBatch") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

start_time = time.time()

# Read match_events table from MySQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/football") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "match_events") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Ensure the timestamp is in correct format
df = df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

# Filter for foul events
foul_events = df.filter(col("event_type") == "foul")

# Group by 1-minute tumbling window and team
windowed = foul_events.groupBy(
    window(col("event_timestamp"), "1 minute"),
    col("team")
).count().withColumnRenamed("count", "fouls") \
 .withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .drop("window")


# Show results on screen
windowed.select("team", "fouls", "window_start", "window_end").show(truncate=False)

end_time = time.time()
print(f"✅ Batch execution completed in {end_time - start_time:.2f} seconds")


