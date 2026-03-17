import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# CONFIG
# =========================
DB_FILE = "reddit.db"

# =========================
# LOAD FROM SQLITE
# =========================
conn = sqlite3.connect(DB_FILE)

query = """
SELECT
    id,
    subreddit,
    score,
    created_utc,
    sentiment,
    positive,
    neutral,
    negative
FROM posts
"""

df = pd.read_sql_query(query, conn)

conn.close()

# Convert datetime
df["created_utc"] = pd.to_datetime(df["created_utc"])

print(f"Loaded {len(df)} rows from SQLite")

# =========================
# 1. SENTIMENT DISTRIBUTION
# =========================
plt.figure()
plt.hist(df["sentiment"], bins=40)
plt.title("Sentiment Distribution (SQLite)")
plt.xlabel("Sentiment")
plt.ylabel("Frequency")
plt.show()

# =========================
# 2. SENTIMENT BREAKDOWN
# =========================
positive = (df["sentiment"] > 0.05).sum()
neutral = ((df["sentiment"] >= -0.05) & (df["sentiment"] <= 0.05)).sum()
negative = (df["sentiment"] < -0.05).sum()

labels = ["Positive", "Neutral", "Negative"]
values = [positive, neutral, negative]

plt.figure()
plt.bar(labels, values)
plt.title("Sentiment Breakdown")
plt.ylabel("Count")
plt.show()

# =========================
# 3. SENTIMENT OVER TIME
# =========================
df_time = df.sort_values("created_utc")

time_series = (
    df_time.set_index("created_utc")["sentiment"]
    .resample("H")
    .mean()
)

plt.figure()
plt.plot(time_series)
plt.title("Average Sentiment Over Time")
plt.xlabel("Time")
plt.ylabel("Avg Sentiment")
plt.xticks(rotation=45)
plt.show()

# =========================
# 4. SUBREDDIT COMPARISON
# =========================
subreddit_avg = (
    df.groupby("subreddit")["sentiment"]
    .mean()
    .sort_values()
)

plt.figure()
subreddit_avg.plot(kind="bar")
plt.title("Average Sentiment by Subreddit")
plt.ylabel("Avg Sentiment")
plt.xticks(rotation=45)
plt.show()

# =========================
# 5. INTENSITY VS ENGAGEMENT
# =========================
df["intensity"] = df["sentiment"].abs()

plt.figure()
plt.scatter(df["intensity"], df["score"])
plt.title("Emotional Intensity vs Score")
plt.xlabel("Absolute Sentiment (Intensity)")
plt.ylabel("Score")
plt.show()

# =========================
# 6. BONUS: TOP EXTREME POSTS
# =========================
top_negative = df.nsmallest(5, "sentiment")
top_positive = df.nlargest(5, "sentiment")

print("\n=== Most Negative Posts ===")
print(top_negative[["subreddit", "sentiment", "score"]])

print("\n=== Most Positive Posts ===")
print(top_positive[["subreddit", "sentiment", "score"]])