from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, window, count, lit
from pyspark.sql.types import TimestampType, IntegerType
import time
import statistics
from datetime import datetime

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("FootballStatsBatch") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.default.parallelism", "1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Global variables for metrics
processing_times = {
    "yellow_card": 0,
    "player_performance": 0,
    "foul": 0
}

total_events_processed = {
    "yellow_card": 0,
    "player_performance": 0,
    "foul": 0
}

# Function to print final metrics
def print_final_metrics():
    print("\n" + "="*70)
    print(f"📊 FINAL METRICS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Calculate total events processed - using Python's built-in sum
    total_events = 0
    for count in total_events_processed.values():
        total_events += count
    
    print(f"📊 Total Events Processed: {total_events} events")
    
    print("\n📈 Events Processed by Type:")
    for event_type, count in total_events_processed.items():
        print(f"  - {event_type}: {count} events")
    
    print("\n⚡ Processing Times by Event Type (seconds):")
    for event_type, time_taken in processing_times.items():
        print(f"  - {event_type}: {time_taken:.4f}")
    
    # Calculate overall average processing time - using Python's built-in sum
    total_time = 0
    for time_taken in processing_times.values():
        total_time += time_taken
    
    print(f"\n🔄 Total Processing Time: {total_time:.4f} seconds")
    
    # Calculate processing efficiency
    if total_time > 0:
        efficiency = total_events / total_time
        print(f"📈 Processing Efficiency: {efficiency:.2f} events/second")
    
    print("="*70 + "\n")

# Main function
def main():
    #start_time = time.time()
    
    try:
        # Read match_events table from MySQL
        print("🔍 Reading data from MySQL...")
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
        
        # Cache the DataFrame to avoid repeated loading
        df.cache()
        # Force computation to ensure caching is complete
        df.count()
        start_time = time.time()
        # ===== YELLOW CARD PROCESSING =====
        print("\n🟨 Processing Yellow Card Events...")
        yellow_card_start = time.time()
        
        # Filter only yellow card events
        yellow_cards = df.filter(col("event_type") == "yellow_card")
        total_events_processed["yellow_card"] = yellow_cards.count()
        
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
        
        # Show result on console
        print("\n📊 Yellow Card Statistics:")
        windowed.select("team", "player", "yellow_cards", "window_start", "window_end", "player_alert") \
            .orderBy("window_start", "team", "player") \
            .show(truncate=False, n=windowed.count())
        
        yellow_card_end = time.time()
        processing_times["yellow_card"] = yellow_card_end - yellow_card_start
        print(f"✅ Yellow Card processing completed in {processing_times['yellow_card']:.2f} seconds")
        
        # ===== PLAYER PERFORMANCE PROCESSING =====
        print("\n👤 Processing Player Performance Events...")
        player_perf_start = time.time()
        
        # Filter player performance events (goals, assists, shots)
        player_events = df.filter(
            (col("event_type") == "goal") | 
            (col("event_type") == "assist") | 
            (col("event_type") == "shot")
        )
        total_events_processed["player_performance"] = player_events.count()
        
        # Aggregate player performance
        aggregated = player_events.groupBy("team", "player").agg(
            sum("shots_taken").alias("total_shots_taken"),
            sum("goals_scored").alias("total_goals_scored"),
            sum(when(col("event_type") == "assist", 1).otherwise(0)).alias("total_assists")
        )
        
        # Calculate scoring percentage
        aggregated = aggregated.withColumn(
            "scoring_percentage",
            when(col("total_shots_taken") > 0, (col("total_goals_scored") / col("total_shots_taken")) * 100).otherwise(0)
        )
        
        # Show results on screen
        print("\n📊 Player Performance Statistics:")
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
        
        player_perf_end = time.time()
        processing_times["player_performance"] = player_perf_end - player_perf_start
        print(f"✅ Player Performance processing completed in {processing_times['player_performance']:.2f} seconds")
        
        # ===== FOUL PROCESSING =====
        print("\n⚠️ Processing Foul Events...")
        foul_start = time.time()
        
        # Filter for foul events
        foul_events = df.filter(col("event_type") == "foul")
        total_events_processed["foul"] = foul_events.count()
        
        # Group by 1-minute tumbling window and team
        windowed_fouls = foul_events.groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("team")
        ).count().withColumnRenamed("count", "fouls") \
         .withColumn("window_start", col("window.start")) \
         .withColumn("window_end", col("window.end")) \
         .drop("window")
        
        # Show results on screen
        print("\n📊 Foul Statistics:")
        windowed_fouls.select("team", "fouls", "window_start", "window_end").show(truncate=False)
        
        foul_end = time.time()
        processing_times["foul"] = foul_end - foul_start
        print(f"✅ Foul processing completed in {processing_times['foul']:.2f} seconds")
        
        # Print final metrics
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\n✅ All batch processing completed in {total_time:.2f} seconds")
        print_final_metrics()
        
    except Exception as e:
        print(f"❌ Error in batch processing: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 