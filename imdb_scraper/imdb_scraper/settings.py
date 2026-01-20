# Author: Juliusz | Online Data Mining - Amsterdam UAS
"""Scrapy settings for IMDb scraper - optimized for 10k movies with Playwright + proxies."""

from pathlib import Path

BOT_NAME = "imdb_scraper"
SPIDER_MODULES = ["imdb_scraper.spiders"]
NEWSPIDER_MODULE = "imdb_scraper.spiders"

# Crawl settings
ROBOTSTXT_OBEY = False
COOKIES_ENABLED = False
FEED_EXPORT_ENCODING = "utf-8"

# Concurrency and throttling (reduced for proxy stability)
CONCURRENT_REQUESTS = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = 2  # 2 second delay between requests

# AutoThrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.25
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_TARGET_CONCURRENCY = 8.0

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 402]

# DNS and memory
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleLifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.LifoMemoryQueue'

# Pipelines
ITEM_PIPELINES = {
    "imdb_scraper.pipelines.CsvPipeline": 300,
    "imdb_scraper.pipelines.SqlitePipeline": 400,
}

# Bright Data proxy configuration
# Zone type determines the port:
#   - Datacenter: 22225
#   - Residential: 22225
#   - Web Unlocker: 33335
#   - Scraping Browser: 9515
BRIGHTDATA_USER = "brd-customer-hl_79cc5ce7-zone-group4"
BRIGHTDATA_PASS = "8xizh8sdpkq9"
BRIGHTDATA_HOST = "brd.superproxy.io"
BRIGHTDATA_PORT = "33335"  # Web Unlocker port

# Add country targeting (US recommended for IMDB)
# Append -country-us to username for US IPs
BRIGHTDATA_USER_WITH_COUNTRY = f"{BRIGHTDATA_USER}-country-us"

PROXY_URL = f"http://{BRIGHTDATA_USER_WITH_COUNTRY}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"

# Playwright settings
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "proxy": {
        "server": f"http://{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}",
        "username": BRIGHTDATA_USER_WITH_COUNTRY,
        "password": BRIGHTDATA_PASS,
    },
    "args": ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox", "--disable-extensions"],
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 240000  # 4 minutes for proxy connections
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "ignore_https_errors": True,
        "java_script_enabled": True,
        "bypass_csp": True,
    }
}
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 8
PLAYWRIGHT_ABORT_REQUEST = lambda req: req.resource_type in ["image", "media", "font", "stylesheet"]

# Logging
# Always output to PROJECT_ROOT/logs
# This assumes settings.py is in <root>/imdb_scraper/settings.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_FILE = str(PROJECT_ROOT / "logs" / "scraper.log")
LOG_LEVEL = "INFO"
