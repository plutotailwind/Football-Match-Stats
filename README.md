# ⚽ Football Match Statistics Dashboard

A real-time data analytics system for football match statistics built using **Apache Kafka**, **Apache Spark**, **Streamlit**, and **Python**. The system demonstrates **stream processing vs batch processing**, uses **sliding and tumbling windows**, and displays live match insights including **scoreboard**, **foul counts**, **yellow card stats**, and a **top player leaderboard**.

---

## 🚀 Key Features

### 🔄 Stream vs Batch Processing
- **Batch Mode**: Aggregates static data for post-match or half-time summaries.
- **Stream Mode**: Processes live match events in real-time using Spark Structured Streaming and Kafka.

### 🪟 Window Operations
- **Sliding Window**: Continuously updates statistics over overlapping time intervals (e.g., last 5 minutes).
- **Tumbling Window**: Processes fixed non-overlapping time intervals (e.g., every 10 minutes) for analytics.

### 📊 Real-Time Dashboard (via Streamlit)
- 🟢 **Live Scoreboard**: Tracks goals and updates the score instantly.
- 🟨 **Yellow Card Statistics**: Tracks and displays yellow cards by team and player.
- 🚫 **Foul Counts**: Displays total fouls committed by each team.
- 🏆 **Player Leaderboard**: Shows top-performing players based on **goals** and **assists**.
- ⏱️ All stats are updated in real-time from Kafka streams.

---
