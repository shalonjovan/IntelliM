import time
from firecrawl import FirecrawlApp


class FirecrawlContextCrawler:
    def __init__(self, api_key, max_depth=2, limit=50):
        self.app = FirecrawlApp(api_key=api_key)
        self.max_depth = max_depth
        self.limit = limit
        self.results = []

    def crawl(self, seeds):
        for seed in seeds:
            print(f"[Firecrawl] Crawling → {seed['url']}")

            try:
                result = self.app.crawl(
                    seed["url"],
                    params={
                        "maxDepth": self.max_depth,
                        "limit": self.limit,
                        "scrapeOptions": {
                            "formats": ["markdown"]
                        }
                    }
                )

                pages = result.get("data", [])

                for page in pages:
                    self.results.append({
                        "url": page.get("url"),
                        "content": page.get("markdown"),
                        "category": seed.get("category", "unknown")
                    })

                print(f"→ Collected {len(pages)} pages")

            except Exception as e:
                print(f"Error while crawling {seed['url']}: {e}")

            time.sleep(1)  # small delay between seeds

        return self.results

    def save_to_txt(self, filename="output.txt"):
        with open(filename, "w", encoding="utf-8") as f:
            for item in self.results:
                f.write(f"{item['url']}\n")

        print(f"\nSaved {len(self.results)} URLs to {filename}")

    def save_to_json(self, filename="output.json"):
        import json
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)

        print(f"Saved full data to {filename}")


# 🚀 RUN
if __name__ == "__main__":
    seeds = [
        {
            "url": "https://example.com",
            "category": "test"
        }
    ]

    crawler = FirecrawlContextCrawler(
        api_key="YOUR_FIRECRAWL_API_KEY",
        max_depth=2,
        limit=50
    )

    crawler.crawl(seeds)
    crawler.save_to_txt("output.txt")
    crawler.save_to_json("output.json")