from kafka import KafkaProducer
import json
import csv
import time

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the CSV file path
csv_file = 'events_large.csv'

# Read data from the CSV file
with open(csv_file, mode='r') as file:
    csv_reader = csv.DictReader(file)
    
    for row in csv_reader:
        # Extract event details from CSV
        event = {
            "timestamp": row['timestamp'],
            "event_type": row['event_type'],
            "team": row['team'],
            "player": row['player'],
            "shots_taken": int(row['shots_taken']),
            "goals_scored": int(row['goals_scored'])
        }

        # Determine which Kafka topic to send the event to
        if event['event_type'] == "goal":
            topic = "live_score"
            # Send goal event to player_performance topic for tracking goals
            producer.send("player_performance", value=event)
        elif event['event_type'] == "assist":
            topic = "player_performance"
        elif event['event_type'] == "shot":
            topic = "player_performance"
        elif event['event_type'] == "foul":
            topic = "team_stats_1min"
        elif event['event_type'] == "yellow_card":
            topic = "live_card"
        
        # Send the event to the appropriate Kafka topic
        producer.send(topic, value=event)
        print(f"[SENT to {topic}]:", event)

        # Simulate a small delay between events (optional)
        time.sleep(1)  # Adjust delay as necessary for your use case