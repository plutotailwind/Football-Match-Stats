from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import TimestampType
import time

# Start Spark session
spark = SparkSession.builder \
    .appName("YellowCardBatch") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

start_time=time.time()
# Read the persistent match_events table from MySQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/football") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "match_events") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

# Ensure timestamp is correctly cast
df = df.withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))

# Filter only yellow card events
yellow_cards = df.filter(col("event_type") == "yellow_card")

# Apply 5-minute sliding window with 1-minute slide
windowed = yellow_cards.groupBy(
    window(col("event_timestamp"), "5 minutes", "1 minute"),
    col("team"),
    col("player")
).count().withColumnRenamed("count", "yellow_cards") \
 .withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .drop("window")

# Add player_alert
windowed = windowed.withColumn("player_alert", col("yellow_cards") >= 2)

# Create a temporary view to reuse
windowed.createOrReplaceTempView("yellow_stats")

# Calculate team_momentum_lost based on sum of yellow cards per team
team_card_totals = spark.sql("""
    SELECT 
        team, window_start, window_end, 
        SUM(yellow_cards) AS total_team_cards
    FROM yellow_stats
    GROUP BY team, window_start, window_end
""")

# Join back with original stats to attach team_momentum_lost flag
final_df = windowed.join(
    team_card_totals,
    on=["team", "window_start", "window_end"],
    how="left"
).withColumn("team_momentum_lost", col("total_team_cards") > 3) \
 .drop("total_team_cards")

# Show result on console
final_df.select("team", "player", "yellow_cards", "window_start", "window_end", "player_alert", "team_momentum_lost") \
    .orderBy("window_start", "team", "player") \
    .show(truncate=False)

end_time = time.time()
print(f"✅ Batch execution completed in {end_time - start_time:.2f} seconds")

