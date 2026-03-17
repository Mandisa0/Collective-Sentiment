import requests
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timezone
import time

# =========================
# CONFIG
# =========================
SUBREDDITS = [
    "worldnews",
    "anime_titties",
    "news",
    "inthenews",
   
    # Africa
    "Africa",
    "southafrica",
   
    # Europe
    "europe",
    "ukpolitics",
   
    # Americas
    "canada",
    "latinamerica",
   
    # Asia / Middle East
    "asia",
    "india",
    "china",
    "japan",
    "MiddleEast",
   
    # Global politics / geopolitics
    "geopolitics",
    "worldpolitics"
]
TOTAL_POSTS = 1000
BATCH_SIZE = 100
COOLDOWN = 30

DB_FILE = "reddit.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (sentiment pipeline)"
}

# =========================
# DB SETUP
# =========================
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    subreddit TEXT,
    title TEXT,
    score INTEGER,
    created_utc TEXT,
    text TEXT,
    sentiment REAL,
    positive REAL,
    neutral REAL,
    negative REAL
)
""")

conn.commit()

# =========================
# INIT
# =========================
analyzer = SentimentIntensityAnalyzer()
fetched = 0

print(f"Target new posts: {TOTAL_POSTS}")

# =========================
# MAIN LOOP
# =========================
for subreddit in SUBREDDITS:
    print(f"\n--- Processing r/{subreddit} ---")

    after = None

    while fetched < TOTAL_POSTS:
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={BATCH_SIZE}"

        if after:
            url += f"&after={after}"

        print(f"Fetching batch... (current new: {fetched})")

        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"Error {response.status_code} on r/{subreddit}")
            break

        data = response.json()
        posts = data["data"]["children"]
        after = data["data"]["after"]

        if not posts:
            print("No more posts.")
            break

        new_posts_in_batch = 0

        for item in posts:
            if fetched >= TOTAL_POSTS:
                break

            post = item["data"]
            post_id = str(post.get("id"))

            text = f"{post.get('title', '')} {post.get('selftext', '')}"

            sentiment = analyzer.polarity_scores(text)

            try:
                cursor.execute("""
                INSERT INTO posts (
                    id, subreddit, title, score, created_utc, text,
                    sentiment, positive, neutral, negative
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    post_id,
                    subreddit,
                    post.get("title"),
                    post.get("score"),
                    datetime.fromtimestamp(
                        post.get("created_utc"), tz=timezone.utc
                    ).isoformat(),
                    text,
                    sentiment["compound"],
                    sentiment["pos"],
                    sentiment["neu"],
                    sentiment["neg"]
                ))

                conn.commit()

                fetched += 1
                new_posts_in_batch += 1

            except sqlite3.IntegrityError:
                # Duplicate (PRIMARY KEY hit)
                continue

        print(f"New in batch: {new_posts_in_batch} | Total new: {fetched}")

        # =========================
        # EARLY EXIT
        # =========================
        if new_posts_in_batch == 0:
            print("No new posts in this batch. Moving to next subreddit.")
            break

        if not after:
            print("End of pagination.")
            break

        if fetched < TOTAL_POSTS:
            print(f"Sleeping {COOLDOWN}s...")
            time.sleep(COOLDOWN)

# =========================
# SUMMARY QUERY
# =========================
print("\n=== Database Summary ===")

cursor.execute("SELECT COUNT(*) FROM posts")
total = cursor.fetchone()[0]

cursor.execute("SELECT AVG(sentiment) FROM posts")
avg_sentiment = cursor.fetchone()[0]

print(f"Total stored posts: {total}")
print(f"Average sentiment: {avg_sentiment:.4f}")

conn.close()