from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from pyspark.sql.types import TimestampType, IntegerType
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PlayerPerformanceBatchFromDB") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
start_time = time.time()

# Read match_events table from MySQL (you can modify this to read from a local file if needed)
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/football") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "match_events") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Ensure timestamp is in correct format
df = df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

# -----------------------------------------------
# 🔹 Aggregate player performance
# -----------------------------------------------
aggregated = df.groupBy("team", "player").agg(
    sum("shots_taken").alias("total_shots_taken"),  # Sum of shots_taken from column
    sum("goals_scored").alias("total_goals_scored"),  # Sum of goals_scored from column
    sum(when(col("event_type") == "assist", 1).otherwise(0)).alias("total_assists")  # Count assists (always 1 per event)
)

# -----------------------------------------------
# 🔹 Calculate scoring percentage
# -----------------------------------------------
aggregated = aggregated.withColumn(
    "scoring_percentage",
    when(col("total_shots_taken") > 0, (col("total_goals_scored") / col("total_shots_taken")) * 100).otherwise(0)
)

# Sort and display the results
aggregated.orderBy(
    col("total_goals_scored").desc(),
    col("scoring_percentage").desc(),
    col("total_assists").desc()
).select(
    "team",
    "player",
    "total_shots_taken",
    "total_goals_scored",
    "total_assists",
    "scoring_percentage"
).show(truncate=False)

end_time = time.time()
print(f"✅ Batch execution completed in {end_time - start_time:.2f} seconds")




