import csv
import mysql.connector
from datetime import datetime

# --- MySQL DB connection ---
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="football"
)
cursor = conn.cursor()

# --- Create table (run only once) ---
create_table_query = """
CREATE TABLE IF NOT EXISTS match_events (
    event_timestamp DATETIME,
    event_type VARCHAR(50),
    team VARCHAR(100),
    player VARCHAR(100),
    shots_taken INT DEFAULT 0,
    goals_scored INT DEFAULT 0
);
"""
cursor.execute(create_table_query)

# --- Read CSV and insert ---
with open('events_large.csv', mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        timestamp = datetime.fromisoformat(row['timestamp'])
        event_type = row['event_type']
        team = row['team']
        player = row['player']
        shots_taken = int(row['shots_taken']) if row['shots_taken'] else 0
        goals_scored = int(row['goals_scored']) if row['goals_scored'] else 0

        insert_query = """
        INSERT INTO match_events (event_timestamp, event_type, team, player, shots_taken, goals_scored)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (timestamp, event_type, team, player, shots_taken, goals_scored))

# --- Commit and close ---
conn.commit()
cursor.close()
conn.close()
