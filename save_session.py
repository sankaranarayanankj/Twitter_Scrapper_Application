# save_session.py
from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # open browser visibly
    page = browser.new_page()
    page.goto("https://twitter.com/login")

    print("👉 Please log in manually in the opened browser window.")
    input("Press ENTER after you are logged in...")

    storage = page.context.storage_state()
    with open("twitter_session.json", "w") as f:
        f.write(json.dumps(storage))

    print("✅ Session saved to twitter_session.json")
    browser.close()
