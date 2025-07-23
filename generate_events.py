import csv
import datetime
import random

# Define teams and players
teams = ["AI United", "FC Python"]
players = {
    "AI United": ["Xander", "Yuki", "Zara"],
    "FC Python": ["Bob", "Charlie", "Alice"]
}

# Define event types and their properties
event_types = {
    "yellow_card": {"shots_taken": 0, "goals_scored": 0},
    "foul": {"shots_taken": 0, "goals_scored": 0},
    "goal": {"shots_taken": (8, 15), "goals_scored": (1, 3)},
    "assist": {"shots_taken": 0, "goals_scored": 0},
    "shot": {"shots_taken": (8, 15), "goals_scored": (1, 3)}
}

# Function to generate a random event
def generate_event(base_time, event_id):
    # Calculate timestamp (add event_id seconds to base time)
    timestamp = base_time + datetime.timedelta(seconds=event_id)
    
    # Select random team and player
    team = random.choice(teams)
    player = random.choice(players[team])
    
    # Select random event type
    event_type = random.choice(list(event_types.keys()))
    
    # Get event properties
    props = event_types[event_type]
    
    # Generate shots and goals based on event type
    if isinstance(props["shots_taken"], tuple):
        shots_taken = random.randint(props["shots_taken"][0], props["shots_taken"][1])
    else:
        shots_taken = props["shots_taken"]
        
    if isinstance(props["goals_scored"], tuple):
        goals_scored = random.randint(props["goals_scored"][0], props["goals_scored"][1])
    else:
        goals_scored = props["goals_scored"]
    
    return {
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
        "event_type": event_type,
        "team": team,
        "player": player,
        "shots_taken": shots_taken,
        "goals_scored": goals_scored
    }

# Generate events
def generate_events(num_events=500):
    # Start with a base time
    base_time = datetime.datetime(2025, 4, 15, 6, 12, 0)
    
    events = []
    for i in range(num_events):
        event = generate_event(base_time, i)
        events.append(event)
    
    return events

# Write events to CSV
def write_events_to_csv(events, filename="events_large.csv"):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'event_type', 'team', 'player', 'shots_taken', 'goals_scored']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for event in events:
            writer.writerow(event)
    
    print(f"Generated {len(events)} events and saved to {filename}")

# Main function
if __name__ == "__main__":
    events = generate_events(500)
    write_events_to_csv(events) 