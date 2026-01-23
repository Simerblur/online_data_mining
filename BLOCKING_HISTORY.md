# Project History: Overcoming IMDb Blocking

This document maintains a history of the strategies we implemented to scrape IMDb, the blocking issues we encountered, and the code changes made to resolve them.

---

## Phase 1: Residential Proxies (The "Standard" Approach)

**Status:** **FAILED** (Blocked immediately)

Initially, we attempted to use standard **Residential Proxies** with `scrapy-playwright`. We rotated IPs for every request, expecting this to be enough to bypass IMDb's defenses.

**The Implementation:**
We configured `settings.py` to route Playwright traffic through the proxy `brd.superproxy.io`.

```python
# settings.py (Phase 1 Code)
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "proxy": {
        "server": "http://brd.superproxy.io:22225", # Residential Port
        "username": "brd-customer-hl_79cc5ce7-zone-residential",
        "password": "**************",
    },
    "args": ["--disable-gpu", "--no-sandbox"],
}
```

**The Result:**
IMDb's WAF (Web Application Firewall) detected the automated browser fingerprint immediately, regardless of the IP address.
*   **Log Error:** `ERROR: WAF challenge did not resolve - page blocked`
*   **Outcome:** 403 Forbidden responses on almost every request.

---

## Phase 2: Bright Data "Web Unlocker" (The Intermediate Step)

**Status:**  **PARTIAL SUCCESS** (Bypassed WAF, but hard to integrate)

We briefly adjusted the port to `33335` (Web Unlocker), which is designed to handle WAFs automatically.

**The Implementation:**
```python
"server": "http://brd.superproxy.io:33335", # Web Unlocker Port
```

**The Result:**
This worked for simple HTTP requests but caused issues with Playwright's persistent context, as the Unlocker often resets connections or injects intermediate "Solving CAPTCHA" pages that confused our scraper logic.

---

## Phase 3: Bright Data Scraping Browser (The Current Solution)

**Status:** **WORKING** (But slow & resource intensive)

To fully mimic a human user, we switched to **Bright Data's Scraping Browser**. This is a remote browser instance hosted by Bright Data that handles all fingerprinting, CAPTCHAs, and WAF challenges server-side.

**The Implementation:**
We removed the local Playwright proxy settings and instead connected directly to the remote browser using the Chrome DevTools Protocol (CDP).

```python
# movie_scraper.py (Phase 3 Code)
class ImdbSpider(scrapy.Spider):
    # ...
    async def _get_browser(self):
        """Connect to remote Scraping Browser via CDP"""
        if self.browser is None:
            # CDP URL contains auth credentials
            cdp_url = "wss://brd-customer-hl_79cc5ce7-zone-scraping_browser4444:390f741dnqqu@brd.superproxy.io:9222"
            
            self.playwright = await async_playwright().start()
            # Connect to the remote browser instead of launching a local one
            self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)
        return self.browser
```

**The Trade-off:**
While this securely bypasses blocking, it introduces significant network overhead because we are streaming browser commands over the internet.
*   **Issue:** Frequent `net::ERR_TUNNEL_CONNECTION_FAILED` errors (especially during peak times/midnight).
*   **Fix:** We implemented  **retry logic** (reconnecting the browser on disconnect) to handle these infrastructure flaps.

```python
# Retry Logic snippet
if 'closed' in error_str or 'Browser' in error_str:
    self.logger.warning(f"Browser disconnected, reconnecting... (retry {retry_count + 1})")
    await self._reconnect_browser()
    return await self._scrape_movie_safe(url, retry_count + 1)
```
