import requests
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
import time

# =========================
# CONFIG
# =========================
SUBREDDIT = "all"
TOTAL_POSTS = 10000        # how many posts you want in total
BATCH_SIZE = 100          # max per request (Reddit limit ~100)
COOLDOWN = 30             # seconds between requests

OUTPUT_FILE = "reddit_sentiment_loop.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (sentiment loop script)"
}

# =========================
# INIT
# =========================
analyzer = SentimentIntensityAnalyzer()
posts_data = []

after = None
fetched = 0

print(f"Target: {TOTAL_POSTS} posts from r/{SUBREDDIT}")

# =========================
# LOOP
# =========================
while fetched < TOTAL_POSTS:
    url = f"https://www.reddit.com/r/{SUBREDDIT}/hot.json?limit={BATCH_SIZE}"
    
    if after:
        url += f"&after={after}"

    print(f"\nFetching batch... (current total: {fetched})")

    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print("Stopping early to avoid ban.")
        break

    data = response.json()

    posts = data["data"]["children"]
    after = data["data"]["after"]

    if not posts:
        print("No more posts available.")
        break

    for item in posts:
        if fetched >= TOTAL_POSTS:
            break

        post = item["data"]
        text = f"{post.get('title', '')} {post.get('selftext', '')}"

        sentiment = analyzer.polarity_scores(text)

        posts_data.append({
            "title": post.get("title"),
            "score": post.get("score"),
            "created_utc": datetime.utcfromtimestamp(post.get("created_utc")),
            "text": text,
            "sentiment": sentiment["compound"],
            "positive": sentiment["pos"],
            "neutral": sentiment["neu"],
            "negative": sentiment["neg"]
        })

        fetched += 1

    print(f"Fetched so far: {fetched}")

    if not after:
        print("Reached end of listing.")
        break

    if fetched < TOTAL_POSTS:
        print(f"Sleeping for {COOLDOWN} seconds...")
        time.sleep(COOLDOWN)

# =========================
# SAVE
# =========================
df = pd.DataFrame(posts_data)
df.to_csv(OUTPUT_FILE, index=False)

print(f"\nSaved {len(df)} posts to {OUTPUT_FILE}")

# =========================
# SUMMARY
# =========================
if not df.empty:
    avg_sentiment = df["sentiment"].mean()

    positive_pct = (df["sentiment"] > 0.05).mean() * 100
    neutral_pct = ((df["sentiment"] >= -0.05) & (df["sentiment"] <= 0.05)).mean() * 100
    negative_pct = (df["sentiment"] < -0.05).mean() * 100

    print("\n=== Sentiment Summary ===")
    print(f"Average Sentiment: {avg_sentiment:.4f}")
    print(f"Positive: {positive_pct:.2f}%")
    print(f"Neutral: {neutral_pct:.2f}%")
    print(f"Negative: {negative_pct:.2f}%")