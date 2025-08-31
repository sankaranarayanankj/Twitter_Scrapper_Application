import re
import json
import pandas as pd
from playwright.sync_api import sync_playwright
import logging
from pathlib import Path
from urllib.parse import quote

# ---------------- CONFIG ---------------- #
HASHTAGS = ["#nifty50", "#sensex", "#intraday", "#banknifty"]
TWEETS_PER_TAG = 2      # how many tweets per hashtag
MAX_SCROLLS = 3          # avoid infinite loops
OUTPUT_CSV = "data/indian_market_tweets.csv"
OUTPUT_PARQUET = "data/indian_market_tweets.parquet"
DEBUG = True              # save sample HTML for inspection

# ---------------- SETUP ---------------- #
Path("data").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/scraper.log"), logging.StreamHandler()],
)

def parse_count(text):
    """Convert 1.2K -> 1200, 3M -> 3000000"""
    if not text:
        return 0
    text = text.strip().upper().replace(",", "")
    m = re.match(r"([\d\.]+)([KM]?)", text)
    if not m:
        return 0
    number, suffix = m.groups()
    try:
        val = float(number)
    except:
        return 0
    if suffix == "K":
        return int(val * 1_000)
    if suffix == "M":
        return int(val * 1_000_000)
    return int(val)

def scrape_tweets():
    with sync_playwright() as p:
        # load saved login session
        with open("twitter_session.json") as f:
            storage = json.load(f)

        browser = p.chromium.launch(headless=True)  # headless=True after testing
        context = browser.new_context(storage_state=storage)
        page = context.new_page()

        all_tweets = []
        seen_ids = set()

        for tag in HASHTAGS:
            quoted_tag = quote(tag)
            url = f"https://x.com/search?q={quoted_tag}&src=typed_query&f=live"
            page.goto(url)
            try:
                page.wait_for_selector("article", timeout=7000)
            except:
                logging.warning(f"No tweets found for {tag}")
                continue

            scrolls = 0
            while len([t for t in all_tweets if tag in t["hashtags"]]) < TWEETS_PER_TAG and scrolls < MAX_SCROLLS:
                tweets = page.locator("article")
                logging.info(f"Found {tweets.count()} articles in DOM")

                for i in range(min(tweets.count(), 50)):  # only check latest 50
                    try:
                        tweet = tweets.nth(i)

                        # Unique tweet ID from permalink
                        time_tag = tweet.locator("time")
                        href = time_tag.locator("xpath=..").get_attribute("href") if time_tag.count() else None
                        if not href or href in seen_ids:
                            continue
                        seen_ids.add(href)

                        # Username
                        username_match = re.search(r"^/([A-Za-z0-9_]+)/status", href) if href else None
                        username = "@" + username_match.group(1) if username_match else None

                        # Timestamp
                        timestamp = time_tag.get_attribute("datetime") if time_tag.count() else None

                        # Content
                        content = ""
                        try:
                            content = tweet.locator("div[data-testid='tweetText']").inner_text()
                        except:
                            try:
                                content = tweet.locator("div[lang]").inner_text()
                            except:
                                content = tweet.inner_text()[:200]  # fallback

                        # Engagement metrics
                        def get_metric(sel):
                            try:
                                node = tweet.locator(sel + " span")
                                if node.count():
                                    return node.first.inner_text()
                                node2 = tweet.locator(sel)
                                return node2.inner_text()
                            except:
                                return ""
                        replies = parse_count(get_metric("div[data-testid='reply']"))
                        retweets = parse_count(get_metric("div[data-testid='retweet']"))
                        likes = parse_count(get_metric("div[data-testid='like']"))

                        mentions = re.findall(r"@\w+", content)
                        hashtags = re.findall(r"#\w+", content)

                        tweet_data = {
                            "username": username,
                            "timestamp": timestamp,
                            "content": content,
                            "replies": replies,
                            "retweets": retweets,
                            "likes": likes,
                            "mentions": mentions,
                            "hashtags": hashtags,
                            "permalink": href,
                            "source_tag": tag
                        }

                        all_tweets.append(tweet_data)

                        # Save raw HTML for debugging
                        if DEBUG and len(all_tweets) <= 2:
                            with open(f"logs/sample_tweet_{len(all_tweets)}.html", "w", encoding="utf-8") as f:
                                f.write(tweet.inner_html())

                    except Exception as e:
                        logging.debug(f"Tweet parse error: {e}")
                        continue

                collected = len([t for t in all_tweets if tag in t["hashtags"]])
                logging.info(f"{tag}: {collected}/{TWEETS_PER_TAG}")

                if collected >= TWEETS_PER_TAG:
                    break

                # Scroll
                page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                page.wait_for_timeout(1500)
                scrolls += 1

        browser.close()

    # Save results
    df = pd.DataFrame(all_tweets).drop_duplicates(subset=["permalink", "content"])
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    try:
        df.to_parquet(OUTPUT_PARQUET, index=False)
    except Exception:
        logging.warning("⚠️ Could not save Parquet (pyarrow missing). Install with `pip install pyarrow`.")
    return df

if __name__ == "__main__":
    scrape_tweets()
