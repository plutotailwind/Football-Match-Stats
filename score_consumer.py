from kafka import KafkaConsumer
import json
import os

consumer = KafkaConsumer(
    'live_score',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='scoreboard-group'
)

score_file = 'score_state.json'
processed_events_file = 'processed_events.json'  # A file to store processed events

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

# Initialize score and processed events files
initialize_score()
initialize_processed_events()

# Function to update the score and prevent duplicate events
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

# Listen to incoming Kafka messages
for message in consumer:
    event = message.value
    
    # Process only 'goal' events and ignore others
    if event.get("event_type") == "goal":
        update_score(event)