from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, TimestampType
import mysql.connector
import time

spark = SparkSession.builder \
    .appName("YellowCardStreaming") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("event_type", StringType()) \
    .add("team", StringType()) \
    .add("player", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "live_card") \
    .load()


parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr("data.timestamp", "data.event_type", "data.team", "data.player") \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .filter("event_type = 'yellow_card'")

windowed = parsed.groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    col("team"),
    col("player")
).count().withColumnRenamed("count", "yellow_cards") \
 .withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .drop("window")

def write_to_mysql(df, batch_id):
    if df.isEmpty():
        return
    start_time=time.time()

    pdf = df.toPandas()

    pdf['window_start'] = pdf['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    pdf['window_end'] = pdf['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

    pdf['player_alert'] = pdf['yellow_cards'] >= 2
    pdf['team_momentum_lost'] = False

    team_card_counts = pdf.groupby(['team', 'window_start', 'window_end'])['yellow_cards'].sum().reset_index()

    for idx, row in team_card_counts.iterrows():
        if row['yellow_cards'] > 3:
            pdf.loc[(pdf['team'] == row['team']) & (pdf['window_start'] == row['window_start']), 'team_momentum_lost'] = True

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="football"
    )
    cursor = conn.cursor()

    query = """
        INSERT INTO yellow_card_stats (team, player, yellow_cards, window_start, window_end, player_alert, team_momentum_lost)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            yellow_cards = VALUES(yellow_cards),
            player_alert = VALUES(player_alert),
            team_momentum_lost = VALUES(team_momentum_lost),
            window_end = VALUES(window_end)
    """

    for _, row in pdf.iterrows():
        cursor.execute(query, (
            row['team'],
            row['player'],
            int(row['yellow_cards']),
            row['window_start'],
            row['window_end'],
            bool(row['player_alert']),
            bool(row['team_momentum_lost'])
        ))

    conn.commit()
    cursor.close()
    conn.close()

    end_time = time.time()  # ⏱ End timing
    print(f"✅ Batch {batch_id} processed in {end_time - start_time:.2f} seconds")

query = windowed.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
