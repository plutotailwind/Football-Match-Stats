from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, when, count
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
import mysql.connector
import time
import statistics
from datetime import datetime
import subprocess
import sys
import json
import os
import threading
from kafka import KafkaConsumer
import signal
from kafka.structs import TopicPartition

# Global variables for metrics
batch_processing_times = {
    "yellow_card": [],
    "player_performance": [],
    "foul": []
}
total_events_processed = {
    "yellow_card": 0,
    "player_performance": 0,
    "foul": 0
}

# Flag to track if metrics have been printed
metrics_printed = False

# Score consumer variables
score_file = 'score_state.json'
processed_events_file = 'processed_events.json'

# Function to check if Kafka topics exist
def check_kafka_topics():
    print("🔍 Checking if Kafka topics exist...")
    required_topics = ["live_card", "player_performance", "team_stats_1min"]
    missing_topics = []
    
    try:
        # Run kafka-topics.sh to list all topics
        result = subprocess.run(
            ["kafka-topics.sh", "--list", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True,
            check=True
        )
        
        existing_topics = result.stdout.strip().split('\n')
        
        # Check which required topics are missing
        for topic in required_topics:
            if topic not in existing_topics:
                missing_topics.append(topic)
        
        if missing_topics:
            print(f"⚠ The following Kafka topics are missing: {', '.join(missing_topics)}")
            print("Creating missing topics...")
            
            # Create missing topics
            for topic in missing_topics:
                print(f"Creating topic: {topic}")
                subprocess.run(
                    ["kafka-topics.sh", "--create", "--topic", topic, 
                     "--bootstrap-server", "localhost:9092", "--partitions", "1", "--replication-factor", "1"],
                    check=True
                )
            
            print("✅ All required Kafka topics have been created.")
        else:
            print("✅ All required Kafka topics exist.")
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Error checking Kafka topics: {e}")
        print("Make sure Kafka is running and accessible.")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ kafka-topics.sh not found. Make sure Kafka is installed and in your PATH.")
        sys.exit(1)

# Function to print final metrics
def print_final_metrics():
    global metrics_printed
    if metrics_printed:
        return
    
    metrics_printed = True
    print("\n" + "="*70)
    print(f"📊 FINAL METRICS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Calculate total events processed - manually sum the values
    total_events = 0
    for count in total_events_processed.values():
        total_events += count
    
    print(f"📊 Total Events Processed: {total_events} events")
    
    print("\n📈 Events Processed by Type:")
    for event_type, count in total_events_processed.items():
        print(f"  - {event_type}: {count} events")
    
    print("\n⚡ Processing Times by Event Type (seconds):")
    for event_type, times in batch_processing_times.items():
        if times:
            avg_time = statistics.mean(times)
            max_time = max(times)
            min_time = min(times)
            print(f"  - {event_type}:")
            print(f"    • Average: {avg_time:.4f}")
            print(f"    • Min: {min_time:.4f}")
            print(f"    • Max: {max_time:.4f}")
    
    # Calculate overall average processing time
    all_times = []
    for times in batch_processing_times.values():
        all_times.extend(times)
    
    if all_times:
        avg_total = statistics.mean(all_times)
        print(f"\n🔄 Overall Average Processing Time: {avg_total:.4f} seconds")
    
    print("="*70 + "\n")

# Score consumer functions
def initialize_score():
    if not os.path.exists(score_file) or os.stat(score_file).st_size == 0:
        initial_state = {
            "FC Python": 0,
            "AI United": 0
        }
        with open(score_file, 'w') as f:
            json.dump(initial_state, f)

def initialize_processed_events():
    if not os.path.exists(processed_events_file):
        processed_events = {}
        with open(processed_events_file, 'w') as f:
            json.dump(processed_events, f)

def update_score(event):
    team = event.get("team")
    goals_scored = event.get("goals_scored", 0)
    timestamp = event.get("timestamp")

    try:
        # Read the current scores from the file
        with open(score_file, 'r') as f:
            scores = json.load(f)
    except Exception:
        scores = {"FC Python": 0, "AI United": 0}

    # Read the processed events file
    with open(processed_events_file, 'r') as f:
        processed_events = json.load(f)

    # Check if this event has been processed already
    if timestamp not in processed_events:
        # Update the score by adding the goals scored from the event
        scores[team] += goals_scored

        # Save the updated scores back to the file
        with open(score_file, 'w') as f:
            json.dump(scores, f)

        # Add this event's timestamp to processed events to avoid re-processing it
        processed_events[timestamp] = True
        with open(processed_events_file, 'w') as f:
            json.dump(processed_events, f)

        # Print updated score
        print(f"Updated Score: {scores}")
    else:
        print(f"Event already processed: {event}")

# Function to run the score consumer in a separate thread
def run_score_consumer():
    # Initialize score and processed events files
    initialize_score()
    initialize_processed_events()
    
    consumer = KafkaConsumer(
        'live_score',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='scoreboard-group'
    )
    
    print("🎯 Score consumer started")
    
    # Listen to incoming Kafka messages
    for message in consumer:
        event = message.value
        
        # Process only 'goal' events and ignore others
        if event.get("event_type") == "goal":
            update_score(event)

# Function to check if producer has finished
def check_producer_finished():
    try:
        # Check if there are any messages in the Kafka topics
        result = subprocess.run(
            ["kafka-topics.sh", "--describe", "--topic", "live_card", "--bootstrap-server", "localhost:9092"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # If we can get the topic info, check if there are any messages
        if "Partition: 0" in result.stdout:
            # Check if there are any messages in the topic
            consumer = KafkaConsumer(
                'live_card',
                bootstrap_servers='localhost:9092',
                auto_offset_reset='latest',
                group_id='check-group'
            )
            
            # Get the latest offset
            partitions = consumer.partitions_for_topic('live_card')
            if partitions:
                # Convert set to list to access by index
                partition_list = list(partitions)
                # Create a proper TopicPartition object
                tp = TopicPartition('live_card', partition_list[0])
                latest_offsets = consumer.end_offsets([tp])
                latest_offset = latest_offsets[tp]
                
                # If the latest offset is 0, there are no messages
                if latest_offset == 0:
                    consumer.close()
                    return True
            
            consumer.close()
        
        return False
    except Exception as e:
        print(f"Error checking producer status: {e}")
        return False

# Signal handler for graceful shutdown
def signal_handler(signum, frame):
    print("\n⚠ Received shutdown signal. Stopping gracefully...")
    if 'yellow_card_query' in globals() and yellow_card_query is not None:
        yellow_card_query.stop()
    if 'player_performance_query' in globals() and player_performance_query is not None:
        player_performance_query.stop()
    if 'foul_query' in globals() and foul_query is not None:
        foul_query.stop()
    if 'spark' in globals():
        spark.stop()
    print_final_metrics()
    sys.exit(0)

# ===== YELLOW CARD PROCESSING =====
def process_yellow_cards():
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "live_card") \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), base_schema).alias("data")) \
            .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player") \
            .withColumn("event_time", col("timestamp").cast(TimestampType())) \
            .filter("event_type = 'yellow_card'")

        # Add watermark to handle late-arriving data
        watermarked = parsed.withWatermark("event_time", "2 minutes")

        windowed = watermarked.groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("team"),
            col("player")
        ).count().withColumnRenamed("count", "yellow_cards") \
         .withColumn("window_start", col("window.start")) \
         .withColumn("window_end", col("window.end")) \
         .drop("window")

        def write_yellow_cards_to_mysql(df, batch_id):
            if df.isEmpty():
                return
            
            batch_start_time = time.time()
            
            pdf = df.toPandas()
            total_events_processed["yellow_card"] += len(pdf)

            pdf['window_start'] = pdf['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
            pdf['window_end'] = pdf['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Set player alert based on individual yellow cards
            pdf['player_alert'] = pdf['yellow_cards'] >= 2

            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root",
                database="football"
            )
            cursor = conn.cursor()

            query = """
                INSERT INTO yellow_card_stats (team, player, yellow_cards, window_start, window_end, player_alert)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    yellow_cards = VALUES(yellow_cards),
                    player_alert = VALUES(player_alert),
                    window_end = VALUES(window_end)
            """

            for _, row in pdf.iterrows():
                cursor.execute(query, (
                    row['team'],
                    row['player'],
                    int(row['yellow_cards']),
                    row['window_start'],
                    row['window_end'],
                    bool(row['player_alert'])
                ))

            conn.commit()
            cursor.close()
            conn.close()

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time
            batch_processing_times["yellow_card"].append(processing_time)
            
            # Print batch metrics immediately (like friend's code)
            print(f"✅ Yellow Card Batch {batch_id} processed in {processing_time:.2f} seconds")
            print(f"   • Events in batch: {len(pdf)}")
            print(f"   • Total yellow card events processed: {total_events_processed['yellow_card']}")

        return windowed.writeStream \
            .outputMode("update") \
            .foreachBatch(write_yellow_cards_to_mysql) \
            .start()
    except Exception as e:
        print(f"❌ Error in yellow card processing: {e}")
        return None

# ===== PLAYER PERFORMANCE PROCESSING =====
def process_player_performance():
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "player_performance") \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), player_performance_schema).alias("data")) \
            .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player", 
                       "data.shots_taken", "data.goals_scored", "data.assists") \
            .withColumn("event_time", col("timestamp").cast(TimestampType()))

        # Remove watermark for player performance as it doesn't use time-based windows
        aggregated = parsed.groupBy(
            col("team"),
            col("player")
        ).agg(
            spark_sum("shots_taken").alias("total_shots_taken"),
            spark_sum("goals_scored").alias("total_goals_scored"),
            spark_sum(when(col("event_type") == "assist", 1).otherwise(0)).alias("total_assists")
        )

        aggregated = aggregated.withColumn(
            "scoring_percentage",
            when(col("total_shots_taken") > 0, (col("total_goals_scored") / col("total_shots_taken")) * 100).otherwise(0)
        )

        def write_player_performance_to_mysql(df, batch_id):
            if df.isEmpty():
                return
            
            batch_start_time = time.time()
            
            pdf = df.toPandas()
            total_events_processed["player_performance"] += len(pdf)

            pdf.fillna({
                "total_shots_taken": 0,
                "total_goals_scored": 0,
                "total_assists": 0,
                "scoring_percentage": 0
            }, inplace=True)

            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root",
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

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time
            batch_processing_times["player_performance"].append(processing_time)
            
            # Print batch metrics immediately (like friend's code)
            print(f"✅ Player Performance Batch {batch_id} processed in {processing_time:.2f} seconds")
            print(f"   • Events in batch: {len(pdf)}")
            print(f"   • Total player performance events processed: {total_events_processed['player_performance']}")

        return aggregated.writeStream \
            .outputMode("complete") \
            .foreachBatch(write_player_performance_to_mysql) \
            .start()
    except Exception as e:
        print(f"❌ Error in player performance processing: {e}")
        return None

# ===== FOUL PROCESSING =====
def process_fouls():
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "team_stats_1min") \
            .load()

        parsed = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), base_schema).alias("data")) \
            .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player") \
            .withColumn("event_time", col("timestamp").cast(TimestampType())) \
            .filter("event_type = 'foul'")

        # Add watermark to handle late-arriving data
        watermarked = parsed.withWatermark("event_time", "2 minutes")

        windowed = watermarked.groupBy(
            window(col("event_time"), "1 minute"),
            col("team")
        ).count().withColumnRenamed("count", "fouls") \
         .withColumn("window_start", col("window.start")) \
         .withColumn("window_end", col("window.end")) \
         .drop("window")

        def write_fouls_to_mysql(df, batch_id):
            if df.isEmpty():
                return
            
            batch_start_time = time.time()
            
            pandas_df = df.toPandas()
            total_events_processed["foul"] += len(pandas_df)

            pandas_df['window_start'] = pandas_df['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
            pandas_df['window_end'] = pandas_df['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root",
                database="football"
            )
            cursor = conn.cursor()

            query = """
                INSERT INTO foul_stats (team, fouls, window_start, window_end)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    fouls = VALUES(fouls),
                    window_end = VALUES(window_end)
            """

            for _, row in pandas_df.iterrows():
                cursor.execute(query, (
                    row['team'],
                    int(row['fouls']),
                    row['window_start'],
                    row['window_end']
                ))

            conn.commit()
            cursor.close()
            conn.close()

            batch_end_time = time.time()
            processing_time = batch_end_time - batch_start_time
            batch_processing_times["foul"].append(processing_time)
            
            # Print batch metrics immediately (like friend's code)
            print(f"✅ Foul Batch {batch_id} processed in {processing_time:.2f} seconds")
            print(f"   • Events in batch: {len(pandas_df)}")
            print(f"   • Total foul events processed: {total_events_processed['foul']}")

        return windowed.writeStream \
            .outputMode("update") \
            .foreachBatch(write_fouls_to_mysql) \
            .start()
    except Exception as e:
        print(f"❌ Error in foul processing: {e}")
        return None

def main():
    global spark, yellow_card_query, player_performance_query, foul_query
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check Kafka topics before starting
    check_kafka_topics()
    
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("FootballStatsStreaming") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Define the schema for parsing Kafka JSON
    global base_schema, player_performance_schema
    base_schema = StructType() \
        .add("timestamp", StringType()) \
        .add("event_type", StringType()) \
        .add("team", StringType()) \
        .add("player", StringType())

    player_performance_schema = base_schema \
        .add("shots_taken", IntegerType()) \
        .add("goals_scored", IntegerType()) \
        .add("assists", IntegerType())
    
    # Start the score consumer in a separate thread
    score_consumer_thread = threading.Thread(target=run_score_consumer, daemon=True)
    score_consumer_thread.start()
    
    # Start all streaming queries
    print("🚀 Starting all streaming queries...")
    yellow_card_query = process_yellow_cards()
    player_performance_query = process_player_performance()
    foul_query = process_fouls()

    # Check if any queries failed to start
    if yellow_card_query is None or player_performance_query is None or foul_query is None:
        print("❌ One or more streaming queries failed to start. Exiting.")
        sys.exit(1)
    
    try:
        # Create a list of all queries
        queries = [yellow_card_query, player_performance_query, foul_query]
        
        # Wait for any query to terminate or for the producer to finish
        no_data_count = 0
        while any(query.isActive for query in queries):
            time.sleep(5)  # Check every 5 seconds
            
            # Check if producer has finished
            if check_producer_finished():
                no_data_count += 1
                if no_data_count >= 3:  # If no data for 15 seconds, assume producer is done
                    print("📊 Producer appears to have finished. Displaying final metrics and exiting...")
                    print_final_metrics()
                    break
            else:
                no_data_count = 0
            
        # If we get here, all queries have terminated or producer has finished
        print("✅ All streaming queries have completed.")
        print_final_metrics()
        
    except KeyboardInterrupt:
        print("\n⚠ Received keyboard interrupt. Shutting down gracefully...")
        print_final_metrics()
        yellow_card_query.stop()
        player_performance_query.stop()
        foul_query.stop()
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()