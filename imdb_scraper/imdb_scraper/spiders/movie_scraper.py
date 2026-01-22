# Author: Juliusz | Online Data Mining - Amsterdam UAS
import asyncio
import random
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import scrapy
from playwright.async_api import async_playwright
from imdb_scraper.items import MovieItem, ReviewItem
from scrapy.selector import Selector
from scrapy.http import HtmlResponse


class ImdbSpider(scrapy.Spider):
    """
    IMDb Spider using Bright Data Scraping Browser via CDP.

    Scraping Browser handles all anti-bot protection automatically.
    We connect via WebSocket and control the browser directly.

    Speed optimizations:
    - Parallel movie scraping with multiple browser tabs (default: 5)
    - Optional review skipping for faster runs
    - Reduced delays between operations
    """

    name = "movie_scraper"
    allowed_domains = ["imdb.com"]

    # Configuration for targeting the number of movies to scrape
    max_movies = 20000

    # Limit reviews per movie to keep scraping fast; can be increased for more depth
    max_reviews_per_movie = 4

    # Number of parallel browser tabs (limited by Bright Data plan capacity)
    concurrent_pages = 3

    def __init__(self, max_movies=20000, concurrent_pages=3, skip_reviews=False, *args, **kwargs):
        """
        Initialize the spider with custom arguments.

        Args:
            max_movies: Target total number of movies to scrape.
            concurrent_pages: Number of browser tabs to run in parallel.
            skip_reviews: If True, skips scraping user reviews to speed up data collection.
        """
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.concurrent_pages = int(concurrent_pages)
        self.skip_reviews = skip_reviews in (True, 'true', 'True', '1', 1)
        self.movies_scraped = 0
        self.seen_movie_ids = set()
        self.browser = None
        self.playwright = None
        self._scrape_lock = asyncio.Lock()  # Ensures thread-safe updates to shared counters
        self._load_existing_movie_ids()

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'CONCURRENT_REQUESTS': 1,
        # Disable standard scrapy-playwright integration as we use direct CDP connection
        'DOWNLOAD_HANDLERS': {},
    }

    def _load_existing_movie_ids(self):
        """
        Load existing movie IDs from the local SQLite database to avoid duplicates.
        This prevents re-scraping movies we already have data for.
        """
        db_path = Path(__file__).parent.parent.parent / "output" / "movies.db"
        if not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT movie_id FROM movie")
            # Format IDs as 'tt1234567' for easy comparison
            existing_ids = {f"tt{row[0]:07d}" for row in cursor.fetchall()}
            conn.close()
            self.seen_movie_ids = existing_ids
            self._existing_count = len(existing_ids)
        except Exception:
            pass

    async def _get_browser(self):
        """
        Connect to or retrieve the active Bright Data Scraping Browser instance via CDP (Chrome DevTools Protocol).
        This single browser instance manages multiple tabs.
        """
        if self.browser is None:
            cdp_url = self.settings.get('BRIGHTDATA_CDP_URL')
            self.logger.info("Connecting to Bright Data Scraping Browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)
            self.logger.info("Connected successfully!")
        return self.browser

    async def _reconnect_browser(self):
        """
        Handle browser disconnections by properly closing and re-initializing the connection.
        Useful when the remote browser session times out or errors.
        """
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        self.browser = None
        await asyncio.sleep(2)
        return await self._get_browser()

    def start_requests(self):
        """
        Entry point for Scrapy. Starts the process by requesting the main IMDb search page.
        """
        yield scrapy.Request(
            "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc",
            callback=self.collect_and_scrape_movies,
            dont_filter=True
        )

    async def collect_and_scrape_movies(self, response):
        """
        Main orchestration method:
        1. Collects a large list of movie URLs using infinite scroll.
        2. Filters out movies that are already in the database.
        3. Scrapes the remaining movies sequentially (due to rate limits).
        """
        if hasattr(self, '_existing_count') and self._existing_count > 0:
            self.logger.info(f"Will skip {self._existing_count} movies already in database")

        self.logger.info(f"Speed settings: {self.concurrent_pages} parallel tabs, skip_reviews={self.skip_reviews}")

        # Step 1: Collect movie links
        movie_links = await self._collect_movies_with_infinite_scroll()

        if not movie_links:
            self.logger.error("No movies collected!")
            return

        # Step 2: Filter duplicates
        urls_to_scrape = []
        for link in movie_links:
            if self.movies_scraped >= self.max_movies:
                break
            movie_id_match = re.search(r'/title/(tt\d+)/', link)
            if movie_id_match:
                movie_id = movie_id_match.group(1)
                # Check against in-memory set of existing IDs
                if movie_id in self.seen_movie_ids:
                    continue
                self.seen_movie_ids.add(movie_id)
            full_url = f"https://www.imdb.com{link}" if link.startswith('/') else link
            urls_to_scrape.append(full_url)

        self.logger.info(f"Collected {len(movie_links)} movies, {len(urls_to_scrape)} new to scrape")

        # Step 3: Scrape details
        # We iterate through the list and scrape each movie.
        for i, url in enumerate(urls_to_scrape):
            if self.movies_scraped >= self.max_movies:
                self.logger.info(f"Reached max movies limit: {self.max_movies}")
                break

            items = await self._scrape_movie_safe(url)
            for item in items:
                yield item

            # Progress logging every 10 movies
            if (i + 1) % 10 == 0:
                self.logger.info(f"Progress: {self.movies_scraped}/{self.max_movies} movies scraped")

            # Introduce delay to respect Bright Data rate limits and avoid blocking
            await asyncio.sleep(random.uniform(3, 5))

    async def _collect_movies_with_infinite_scroll(self):
        """
        Navigates to the search page and repeatedly clicks 'Load More' to load movie entries.
        This is necessary because IMDb uses dynamic loading instead of standard pagination.
        """
        url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc"
        all_movie_ids = set()
        all_links = []

        try:
            browser = await self._get_browser()
            page = await browser.new_page()

            try:
                self.logger.info(f"Loading search page: {url}")
                await page.goto(url, timeout=120000, wait_until='domcontentloaded')
                await asyncio.sleep(3)

                # Estimate how many clicks we need based on target count (approx 50 movies per load)
                target_movies = self.max_movies + len(self.seen_movie_ids) + 100
                max_clicks = (target_movies // 50) + 5

                self.logger.info(f"Will click 'Load More' up to {max_clicks} times to get {target_movies} movies")

                for click_num in range(max_clicks):
                    # Scroll to bottom to trigger any lazy loading or visibility of the button
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)

                    # Extract current list of movies
                    content = await page.content()
                    selector = Selector(text=content)
                    links = selector.css('a.ipc-title-link-wrapper::attr(href)').getall()

                    # Deduplicate and store new links
                    new_count = 0
                    for link in links:
                        match = re.search(r'/title/(tt\d+)/', link)
                        if match and match.group(1) not in all_movie_ids:
                            all_movie_ids.add(match.group(1))
                            all_links.append(link)
                            new_count += 1

                    if click_num % 10 == 0 or new_count > 0:
                        self.logger.info(f"Click {click_num}: {len(all_links)} total movies (+{new_count} new)")

                    if len(all_links) >= target_movies:
                        self.logger.info(f"Collected enough movies: {len(all_links)}")
                        break

                    # Helper logic to find and click the 'Load More' button
                    prev_count = len(links)
                    btn_exists = False
                    for _ in range(10):
                        # Use JS to check for button existence and scroll it into view
                        btn_exists = await page.evaluate('''() => {
                            const btn = document.querySelector("button.ipc-see-more__button");
                            if (btn) {
                                btn.scrollIntoView({ behavior: "instant", block: "center" });
                                return true;
                            }
                            return false;
                        }''')
                        if btn_exists:
                            break
                        await asyncio.sleep(0.5)

                    if not btn_exists:
                        self.logger.info("No 'Load More' button found after waiting")
                        break

                    # Click the button using JS for reliability
                    try:
                        await asyncio.sleep(0.3)
                        await page.evaluate('document.querySelector("button.ipc-see-more__button")?.click()')
                    except Exception as e:
                        self.logger.warning(f"JS click failed: {e}")
                        break

                    # Wait for new content to load by observing list length increase
                    for _ in range(33): # Wait approx 10 seconds
                        await asyncio.sleep(0.3)
                        new_content = await page.content()
                        new_selector = Selector(text=new_content)
                        new_links = new_selector.css('a.ipc-title-link-wrapper::attr(href)').getall()
                        if len(new_links) > prev_count:
                            break
                    else:
                        self.logger.warning("Timeout waiting for new content")

                    # Small jitter to mimic human behavior
                    await asyncio.sleep(random.uniform(0.5, 1.0))

            finally:
                await page.close()

        except Exception as e:
            self.logger.error(f"Error collecting movies: {e}")
            await self._reconnect_browser()

        return all_links

    async def _scrape_movie_safe(self, url, retry_count=0):
        """
        Safely scrapes a movie URL with robust error handling and retries.
        catches browser disconnections and rate limit cooldowns.
        """
        max_retries = 3
        try:
            items = []
            async for item in self._scrape_movie(url):
                items.append(item)
            return items
        except Exception as e:
            error_str = str(e)

            # Retry logic for browser connection issues
            if 'closed' in error_str or 'Target page' in error_str or 'Browser' in error_str:
                if retry_count < max_retries:
                    self.logger.warning(f"Browser disconnected for {url}, reconnecting... (retry {retry_count + 1}/{max_retries})")
                    await self._reconnect_browser()
                    await asyncio.sleep(5)
                    return await self._scrape_movie_safe(url, retry_count + 1)
                else:
                    self.logger.error(f"Max retries reached for {url} (browser errors)")

            # Retry logic for specific provider 'cooldown' errors
            elif 'cooldown' in error_str or 'no_peers' in error_str:
                if retry_count < max_retries:
                    wait_time = 30 * (retry_count + 1)  # Exponential-ish backoff: 30s, 60s, 90s
                    self.logger.warning(f"Cooldown hit for {url}, waiting {wait_time}s (retry {retry_count + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    return await self._scrape_movie_safe(url, retry_count + 1)
                else:
                    self.logger.error(f"Max retries reached for {url}")
            else:
                self.logger.error(f"Error scraping {url}: {e}")
            return []

    async def _scrape_movie(self, url):
        """
        Scrapes a single movie details page including metadata and credits.
        """
        try:
            browser = await self._get_browser()
            page = await browser.new_page()

            try:
                # Long timeout because some proxy nodes can be slow
                await page.goto(url, timeout=45000, wait_until='domcontentloaded')

                # Wait for the main title to ensure core content is loaded
                try:
                    await page.wait_for_selector('span.hero__primary-text', timeout=10000)
                except Exception:
                    self.logger.warning(f"Title element not found on {url}, waiting...")
                    await asyncio.sleep(3)

                content = await page.content()
                response = HtmlResponse(url=url, body=content.encode('utf-8'), encoding='utf-8')

                item = MovieItem()

                # Extract Movie ID from URL
                imdb_id_match = re.search(r'/title/tt(\d+)/', url)
                movie_id = int(imdb_id_match.group(1)) if imdb_id_match else None
                item['movie_id'] = movie_id

                # Extract Title
                item['title'] = response.css('span.hero__primary-text::text').get()

                # Validation: Skip if title is missing (bad load)
                if not item['title']:
                    self.logger.warning(f"Skipping movie with no title: {url}")
                    return

                # Extract Year
                year_text = response.xpath(
                    '//ul[contains(@class, "ipc-inline-list")]//li//a[contains(@href, "releaseinfo")]/text()'
                ).get()
                if year_text:
                    year_match = re.search(r'(\d{4})', year_text)
                    item['year'] = int(year_match.group(1)) if year_match else None
                else:
                    item['year'] = None

                # Extract User Rating
                rating_text = response.css(
                    'div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text'
                ).get()
                try:
                    item['user_score'] = float(rating_text) if rating_text else None
                except ValueError:
                    item['user_score'] = None

                # Extract Box Office (using helper)
                item['box_office'] = self._extract_box_office(response)

                # Extract Metadata
                item['release_date'] = response.css(
                    "li[data-testid='title-details-releasedate'] a.ipc-metadata-list-item__list-content-item::text"
                ).get()

                runtime_text = response.css(
                    "li[data-testid='title-techspec_runtime'] span.ipc-metadata-list-item__list-content-item::text"
                ).get()
                item['runtime_minutes'] = self._parse_runtime(runtime_text)

                item['mpaa_rating'] = response.css('a[href*="parentalguide"]::text').get()

                item['production_companies'] = response.css(
                    "li[data-testid='title-details-companies'] a.ipc-metadata-list-item__list-content-item::text"
                ).getall()

                # Extract Genres
                genres = response.css('div.ipc-chip-list__scroller a span::text').getall()
                item['genres'] = ', '.join(genres) if genres else None
                item['genres_list'] = genres if genres else []

                # Extract Credits
                item['directors'] = self._extract_credits_by_role(response, "Director")
                item['writers'] = self._extract_credits_by_role(response, "Writer")
                item['composers'] = self._extract_credits_by_role(response, "Music by")
                item['cast'] = self._extract_cast(response)

                item['scraped_at'] = datetime.now().isoformat()

                # Thread-safe counter update for progress tracking
                async with self._scrape_lock:
                    self.movies_scraped += 1
                    current_count = self.movies_scraped

                self.logger.info(f"Scraped #{current_count}: {item['title']} ({item['year']})")
                yield item

                # Optionally scrape reviews if configured
                if not self.skip_reviews and movie_id:
                    async for review in self._scrape_reviews(movie_id):
                        yield review

            finally:
                await page.close()

        except Exception as e:
            error_str = str(e)
            # Re-raise known recoverable errors to be handled by the safe wrapper
            if any(x in error_str for x in ['cooldown', 'no_peers', 'closed', 'Target page', 'Browser']):
                raise
            self.logger.error(f"Error scraping {url}: {e}")

    async def _scrape_reviews(self, movie_id):
        """
        Navigates to the reviews sub-page and scrapes user reviews.
        """
        url = f'https://www.imdb.com/title/tt{movie_id:07d}/reviews/'

        try:
            browser = await self._get_browser()
            page = await browser.new_page()

            try:
                await page.goto(url, timeout=30000, wait_until='domcontentloaded')
                await asyncio.sleep(1)

                content = await page.content()
                response = HtmlResponse(url=url, body=content.encode('utf-8'), encoding='utf-8')

                reviews_scraped = 0
                for container in response.css('article.user-review-item'):
                    if reviews_scraped >= self.max_reviews_per_movie:
                        break

                    review = ReviewItem()
                    review['movie_id'] = movie_id
                    
                    # Author and Score extraction
                    review['author'] = container.css('[data-testid="author-link"]::text').get()
                    if not review['author']:
                        review['author'] = container.css('a.ipc-link--base::text').get()
                    review['score'] = container.css('span.ipc-rating-star--rating::text').get()
                    
                    # Review Text
                    text_parts = container.css('div.ipc-html-content-inner-div::text').getall()
                    review['text'] = ' '.join(text_parts).strip() if text_parts else None
                    review['is_critic'] = False
                    review['review_date'] = container.css('.review-date::text').get()
                    review['scraped_at'] = datetime.now().isoformat()

                    if review['author'] or review['text']:
                        reviews_scraped += 1
                        yield review

            finally:
                await page.close()

        except Exception as e:
            self.logger.warning(f"Error scraping reviews for {movie_id}: {e}")

    def _parse_runtime(self, text):
        """Converts runtime string (e.g., '2h 15m') to total minutes."""
        if not text:
            return None
        minutes_match = re.search(r'(\d+)\s*min', text)
        if minutes_match:
            return int(minutes_match.group(1))
        h_match = re.search(r'(\d+)\s*h', text)
        m_match = re.search(r'(\d+)\s*m', text)
        minutes = 0
        if h_match:
            minutes += int(h_match.group(1)) * 60
        if m_match:
            minutes += int(m_match.group(1))
        return minutes if minutes > 0 else None

    def _extract_box_office(self, response):
        """Extracts box office gross, handling different layouts."""
        box_office_value = response.xpath(
            '//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]'
            '//span[contains(@class, "ipc-metadata-list-item__list-content-item")]/text()'
        ).get()
        if not box_office_value:
            box_office_value = response.xpath(
                '//span[contains(text(), "Gross worldwide")]/following-sibling::span/text()'
            ).get()
        if box_office_value:
            cleaned = re.sub(r'[^\d]', '', box_office_value)
            if cleaned:
                return int(cleaned)
        return None

    def _extract_credits_by_role(self, response, role_name):
        """
        Extracts people (directors, writers, etc.) based on the role label.
        Handles IMDb's metadata list structure.
        """
        credits = []
        seen_ids = set()
        section = response.xpath(
            f'//li[@data-testid="title-pc-principal-credit"][.//span[contains(text(), "{role_name}")]]'
        )
        if not section:
            section = response.xpath(
                f'//li[contains(@class, "ipc-metadata-list__item")][.//span[contains(text(), "{role_name}")]]'
            )
        for link in section.css('a[href*="/name/"]'):
            name = link.css('::text').get()
            href = link.css('::attr(href)').get()
            person_id_match = re.search(r'/name/(nm\d+)/', href) if href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None
            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)
            if name:
                credits.append({'name': name.strip(), 'imdb_person_id': imdb_person_id})
        return credits

    def _extract_cast(self, response):
        """Extracts the top 10 cast members."""
        cast = []
        seen_ids = set()
        for order, item in enumerate(response.css('div[data-testid="title-cast-item"]')[:10], start=1):
            actor_link = item.css('a[data-testid="title-cast-item__actor"]')
            actor_name = actor_link.css('::text').get()
            actor_href = actor_link.css('::attr(href)').get()
            person_id_match = re.search(r'/name/(nm\d+)/', actor_href) if actor_href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None
            
            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)
                
            character_name = item.css('a[data-testid="cast-item-characters-link"] span::text').get()
            if not character_name:
                character_name = item.css('span[data-testid="cast-item-characters-link"] span::text').get()
                
            if actor_name:
                cast.append({
                    'name': actor_name.strip(),
                    'imdb_person_id': imdb_person_id,
                    'character_name': character_name.strip() if character_name else None,
                    'cast_order': order
                })
        return cast

    async def closed(self, reason):
        """
        Clean up resources when the spider finishes.
        Ensures the Playwright browser connection is closed properly.
        """
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info(f"Spider closed: {reason}. Total movies scraped: {self.movies_scraped}")
