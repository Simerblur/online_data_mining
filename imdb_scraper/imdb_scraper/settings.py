# Scrapy settings for imdb_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "imdb_scraper"

SPIDER_MODULES = ["imdb_scraper.spiders"]
NEWSPIDER_MODULE = "imdb_scraper.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "imdb_scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False  # Disabled for scraping (using proxies)

# Concurrency and throttling settings - optimized for Playwright + proxies
CONCURRENT_REQUESTS = 8  # Increased from default
CONCURRENT_REQUESTS_PER_DOMAIN = 4  # Allow more concurrent requests to IMDb
DOWNLOAD_DELAY = 0.5  # Reduced delay (proxies handle rate limiting)

# Disable cookies to reduce bandwidth
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "imdb_scraper.middlewares.ImdbScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "imdb_scraper.middlewares.ImdbScraperDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    "imdb_scraper.pipelines.CsvPipeline": 300,
    "imdb_scraper.pipelines.SqlitePipeline": 400,
}

# Enable and configure the AutoThrottle extension
# Automatically adjusts delay based on server response times
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0  # Target concurrent requests per server
AUTOTHROTTLE_DEBUG = False

# Retry settings - reduce wasted time on failed requests
RETRY_ENABLED = True
RETRY_TIMES = 2  # Reduced from default 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# DNS caching for faster lookups
DNSCACHE_ENABLED = True
DNSCACHE_SIZE = 10000

# Reduce memory usage
DEPTH_PRIORITY = 1  # BFS instead of DFS
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

# Bright Data residential proxy settings
BRIGHTDATA_USER = "brd-customer-hl_79cc5ce7-zone-group4"
BRIGHTDATA_PASS = "8xizh8sdpkq9"
BRIGHTDATA_HOST = "brd.superproxy.io"
BRIGHTDATA_PORT = "33335"

PROXY_URL = f"http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"

# Playwright settings for JavaScript rendering
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
        "username": BRIGHTDATA_USER,
        "password": BRIGHTDATA_PASS,
    },
    # Performance optimizations
    "args": [
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--disable-setuid-sandbox",
        "--no-sandbox",
        "--disable-extensions",
    ],
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000  # Reduced from 60s to 30s

# Playwright context options - block unnecessary resources
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "ignore_https_errors": True,
        "java_script_enabled": True,
    }
}

# Max pages per browser context (memory optimization)
PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 4

# Abort unnecessary requests to save bandwidth and speed up scraping
PLAYWRIGHT_ABORT_REQUEST = lambda req: req.resource_type in ["image", "media", "font", "stylesheet"]
