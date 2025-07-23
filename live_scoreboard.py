import streamlit as st
import json
import os
import mysql.connector
import pandas as pd

# Set app configuration
st.set_page_config(page_title="⚽ Football Streaming Dashboard", layout="centered")

# Sidebar navigation
st.sidebar.title("📂 Navigation")
page = st.sidebar.radio("Go to", ["Live Scoreboard", "Foul Stats (1-min)", "🟨 Yellow Card Stats", "⚽ Player Performance"])


# LIVE SCOREBOARD SECTION
if page == "Live Scoreboard":
    st.title("⚽ Real-Time Football Score")

    score_file = 'score_state.json'

    if st.button("🔁 Refresh Scoreboard"):
        st.rerun()

    # Load scores from file
    if os.path.exists(score_file) and os.stat(score_file).st_size > 0:
        try:
            with open(score_file, 'r') as f:
                scores = json.load(f)
        except json.JSONDecodeError:
            scores = {"FC Python": 0, "AI United": 0}
    else:
        scores = {"FC Python": 0, "AI United": 0}

    col1, col2 = st.columns(2)
    col1.metric("FC Python", scores.get("FC Python", 0))
    col2.metric("AI United", scores.get("AI United", 0))

    st.info("Click the refresh button to update the scoreboard.")


# FOUL STATS SECTION
elif page == "Foul Stats (1-min)":
    st.title("💥 Team Fouls - 1 Minute Tumbling Window")

    if st.button("🔄 Refresh Foul Stats"):
        st.rerun()

    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",  # Replace with your actual password
            database="football"
        )
        query = "SELECT * FROM foul_stats ORDER BY window_end DESC LIMIT 10"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            st.warning("No foul stats data available yet.")
        else:
            st.dataframe(df)

    except mysql.connector.Error as e:
        st.error(f"Database connection failed: {e}")


# YELLOW CARD STATS SECTION
elif page == "🟨 Yellow Card Stats":
    st.title("🟨 Yellow Card Stats - Sliding Window")

    if st.button("🔄 Refresh Yellow Card Stats"):
        st.rerun()

    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",  # Replace with your actual password
            database="football"
        )
        query = """
            SELECT * FROM yellow_card_stats
            ORDER BY window_end DESC
            LIMIT 20
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            st.warning("No yellow card data available yet.")
        else:
            df['player_alert'] = df['player_alert'].map({1: "🚨 OUT", 0: ""})

            st.dataframe(df[["team", "player", "yellow_cards", "window_start", "window_end", "player_alert"]])

    except mysql.connector.Error as e:
        st.error(f"Database connection failed: {e}")


# PLAYER PERFORMANCE SECTION
elif page == "⚽ Player Performance":
    st.title("⚽ Player Performance - Goals, Assists, and Shots")

    if st.button("🔄 Refresh Player Stats"):
        st.rerun()

    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",  # Replace with your actual password
            database="football"
        )
        query = """
            SELECT * FROM player_performance_stats
            ORDER BY goals DESC, scoring_percentage DESC
            LIMIT 20
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            st.warning("No player performance data available yet.")
        else:
            # Format player stats for better readability
            df['scoring_percentage'] = df['scoring_percentage'].map(lambda x: f"{x:.2f}%")

            # Display the top player stats
            st.dataframe(df[["team", "player", "goals", "assists", "shots_taken", "scoring_percentage"]])

    except mysql.connector.Error as e:
        st.error(f"Database connection failed: {e}")
        