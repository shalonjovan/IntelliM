import time
import random
import pandas as pd
from pytrends.request import TrendReq

# ---------- CONFIG ----------
KEYWORDS = [
    "wireless earbuds",
    "gaming mouse",
    "mechanical keyboard",
    "bluetooth speaker",
    "gaming laptop"
]

TIMEFRAME = 'today 3-m'
GEO = 'IN'
SLEEP_BETWEEN_REQUESTS = (10, 20)  # random delay range (seconds)
MAX_RETRIES = 5


# ---------- INIT ----------
pytrends = TrendReq(
    hl='en-US',
    tz=330,
    requests_args={
        'headers': {
            "User-Agent": "Mozilla/5.0"
        }
    }
)


# ---------- SAFE FETCH ----------
def fetch_trends(keywords):
    for attempt in range(MAX_RETRIES):
        try:
            pytrends.build_payload(
                kw_list=keywords,
                timeframe=TIMEFRAME,
                geo=GEO
            )

            data = pytrends.interest_over_time()

            if not data.empty:
                return data.drop(columns=['isPartial'], errors='ignore')

        except Exception as e:
            print(f"[Retry {attempt+1}] Error:", e)
            sleep_time = random.randint(20, 40)
            print(f"Sleeping {sleep_time}s before retry...")
            time.sleep(sleep_time)

    print("❌ Failed after retries")
    return None


# ---------- MAIN ----------
def main():
    print("🚀 Fetching Google Trends data...\n")

    data = fetch_trends(KEYWORDS)

    if data is not None:
        print("✅ Success!\n")
        print(data.tail())

        # Save to CSV (for your pipeline)
        filename = "trends_data.csv"
        data.to_csv(filename)
        print(f"\n💾 Saved to {filename}")

    else:
        print("❌ No data fetched.")


if __name__ == "__main__":
    main()