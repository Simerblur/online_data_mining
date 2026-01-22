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

    max_movies = 20000
    max_reviews_per_movie = 4
    concurrent_pages = 3  # Number of parallel browser tabs (limited by Bright Data)

    def __init__(self, max_movies=20000, concurrent_pages=3, skip_reviews=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.concurrent_pages = int(concurrent_pages)
        self.skip_reviews = skip_reviews in (True, 'true', 'True', '1', 1)
        self.movies_scraped = 0
        self.seen_movie_ids = set()
        self.browser = None
        self.playwright = None
        self._scrape_lock = asyncio.Lock()  # For thread-safe counter updates
        self._load_existing_movie_ids()

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'CONCURRENT_REQUESTS': 1,
        # Disable scrapy-playwright - we use direct CDP
        'DOWNLOAD_HANDLERS': {},
    }

    def _load_existing_movie_ids(self):
        db_path = Path(__file__).parent.parent.parent / "output" / "movies.db"
        if not db_path.exists():
            return
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT movie_id FROM movie")
            existing_ids = {f"tt{row[0]:07d}" for row in cursor.fetchall()}
            conn.close()
            self.seen_movie_ids = existing_ids
            self._existing_count = len(existing_ids)
        except Exception:
            pass

    async def _get_browser(self):
        """Connect to Bright Data Scraping Browser via CDP."""
        if self.browser is None:
            cdp_url = self.settings.get('BRIGHTDATA_CDP_URL')
            self.logger.info("Connecting to Bright Data Scraping Browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)
            self.logger.info("Connected successfully!")
        return self.browser

    async def _reconnect_browser(self):
        """Reconnect browser after errors."""
        if self.browser:
            try:
                await self.browser.close()
            except:
                pass
        self.browser = None
        await asyncio.sleep(2)
        return await self._get_browser()

    def start_requests(self):
        """Scrapy entry point - start the scraping process."""
        yield scrapy.Request(
            "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc",
            callback=self.collect_and_scrape_movies,
            dont_filter=True
        )

    async def collect_and_scrape_movies(self, response):
        """Main scraping method - collect movies via infinite scroll then scrape in parallel."""
        if hasattr(self, '_existing_count') and self._existing_count > 0:
            self.logger.info(f"Will skip {self._existing_count} movies already in database")

        self.logger.info(f"Speed settings: {self.concurrent_pages} parallel tabs, skip_reviews={self.skip_reviews}")

        # Collect all movie links using infinite scroll
        movie_links = await self._collect_movies_with_infinite_scroll()

        if not movie_links:
            self.logger.error("No movies collected!")
            return

        # Filter out already seen movies
        urls_to_scrape = []
        for link in movie_links:
            if self.movies_scraped >= self.max_movies:
                break
            movie_id_match = re.search(r'/title/(tt\d+)/', link)
            if movie_id_match:
                movie_id = movie_id_match.group(1)
                if movie_id in self.seen_movie_ids:
                    continue
                self.seen_movie_ids.add(movie_id)
            full_url = f"https://www.imdb.com{link}" if link.startswith('/') else link
            urls_to_scrape.append(full_url)

        self.logger.info(f"Collected {len(movie_links)} movies, {len(urls_to_scrape)} new to scrape")

        # Scrape movies sequentially (Bright Data rate limits prevent parallel scraping)
        for i, url in enumerate(urls_to_scrape):
            if self.movies_scraped >= self.max_movies:
                self.logger.info(f"Reached max movies limit: {self.max_movies}")
                break

            # Scrape movie with retry on cooldown
            items = await self._scrape_movie_safe(url)
            for item in items:
                yield item

            # Progress logging every 10 movies
            if (i + 1) % 10 == 0:
                self.logger.info(f"Progress: {self.movies_scraped}/{self.max_movies} movies scraped")

            # Delay between movies (Bright Data requires spacing)
            await asyncio.sleep(random.uniform(3, 5))

    async def _collect_movies_with_infinite_scroll(self):
        """Load search page and click 'Load More' to collect all movie links."""
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

                # Calculate clicks needed
                target_movies = self.max_movies + len(self.seen_movie_ids) + 100
                max_clicks = (target_movies // 50) + 5

                self.logger.info(f"Will click 'Load More' up to {max_clicks} times to get {target_movies} movies")

                for click_num in range(max_clicks):
                    # Scroll down first
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(1)

                    # Get current movies
                    content = await page.content()
                    selector = Selector(text=content)
                    links = selector.css('a.ipc-title-link-wrapper::attr(href)').getall()

                    # Extract unique movie IDs
                    new_count = 0
                    for link in links:
                        match = re.search(r'/title/(tt\d+)/', link)
                        if match and match.group(1) not in all_movie_ids:
                            all_movie_ids.add(match.group(1))
                            all_links.append(link)
                            new_count += 1

                    # Log every 10 clicks to reduce noise
                    if click_num % 10 == 0 or new_count > 0:
                        self.logger.info(f"Click {click_num}: {len(all_links)} total movies (+{new_count} new)")

                    # Check if we have enough
                    if len(all_links) >= target_movies:
                        self.logger.info(f"Collected enough movies: {len(all_links)}")
                        break

                    # Check if Load More button exists and click via JavaScript
                    prev_count = len(links)

                    # Wait for button to appear (up to 5 seconds)
                    btn_exists = False
                    for _ in range(10):
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

                    # Click the button
                    try:
                        await asyncio.sleep(0.3)
                        await page.evaluate('document.querySelector("button.ipc-see-more__button")?.click()')
                    except Exception as e:
                        self.logger.warning(f"JS click failed: {e}")
                        break

                    # Wait for new content - check every 300ms for up to 10 seconds
                    for _ in range(33):
                        await asyncio.sleep(0.3)
                        new_content = await page.content()
                        new_selector = Selector(text=new_content)
                        new_links = new_selector.css('a.ipc-title-link-wrapper::attr(href)').getall()
                        if len(new_links) > prev_count:
                            break
                    else:
                        self.logger.warning("Timeout waiting for new content")

                    # Minimal delay between clicks
                    await asyncio.sleep(random.uniform(0.5, 1.0))

            finally:
                await page.close()

        except Exception as e:
            self.logger.error(f"Error collecting movies: {e}")
            await self._reconnect_browser()

        return all_links

    async def _scrape_movie_safe(self, url, retry_count=0):
        """Wrapper that catches exceptions and returns items as a list. Handles cooldown and browser errors."""
        max_retries = 3
        try:
            items = []
            async for item in self._scrape_movie(url):
                items.append(item)
            return items
        except Exception as e:
            error_str = str(e)

            # Handle browser closed/disconnected errors - reconnect and retry
            if 'closed' in error_str or 'Target page' in error_str or 'Browser' in error_str:
                if retry_count < max_retries:
                    self.logger.warning(f"Browser disconnected for {url}, reconnecting... (retry {retry_count + 1}/{max_retries})")
                    await self._reconnect_browser()
                    await asyncio.sleep(5)
                    return await self._scrape_movie_safe(url, retry_count + 1)
                else:
                    self.logger.error(f"Max retries reached for {url} (browser errors)")

            # Handle Bright Data cooldown errors with retry
            elif 'cooldown' in error_str or 'no_peers' in error_str:
                if retry_count < max_retries:
                    wait_time = 30 * (retry_count + 1)  # 30s, 60s, 90s
                    self.logger.warning(f"Cooldown hit for {url}, waiting {wait_time}s (retry {retry_count + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    return await self._scrape_movie_safe(url, retry_count + 1)
                else:
                    self.logger.error(f"Max retries reached for {url}")
            else:
                self.logger.error(f"Error scraping {url}: {e}")
            return []

    async def _scrape_movie(self, url):
        """Scrape a single movie page."""
        try:
            browser = await self._get_browser()
            page = await browser.new_page()

            try:
                await page.goto(url, timeout=45000, wait_until='domcontentloaded')

                # Wait for the title element to appear (indicates page is loaded)
                try:
                    await page.wait_for_selector('span.hero__primary-text', timeout=10000)
                except Exception:
                    self.logger.warning(f"Title element not found on {url}, waiting...")
                    await asyncio.sleep(3)

                content = await page.content()
                response = HtmlResponse(url=url, body=content.encode('utf-8'), encoding='utf-8')

                item = MovieItem()

                # Movie ID
                imdb_id_match = re.search(r'/title/tt(\d+)/', url)
                movie_id = int(imdb_id_match.group(1)) if imdb_id_match else None
                item['movie_id'] = movie_id

                # Title
                item['title'] = response.css('span.hero__primary-text::text').get()

                # Skip movies with no title (page didn't load properly)
                if not item['title']:
                    self.logger.warning(f"Skipping movie with no title: {url}")
                    return

                # Year
                year_text = response.xpath(
                    '//ul[contains(@class, "ipc-inline-list")]//li//a[contains(@href, "releaseinfo")]/text()'
                ).get()
                if year_text:
                    year_match = re.search(r'(\d{4})', year_text)
                    item['year'] = int(year_match.group(1)) if year_match else None
                else:
                    item['year'] = None

                # Rating
                rating_text = response.css(
                    'div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text'
                ).get()
                try:
                    item['user_score'] = float(rating_text) if rating_text else None
                except ValueError:
                    item['user_score'] = None

                # Box Office
                item['box_office'] = self._extract_box_office(response)

                # Metadata
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

                # Genres
                genres = response.css('div.ipc-chip-list__scroller a span::text').getall()
                item['genres'] = ', '.join(genres) if genres else None
                item['genres_list'] = genres if genres else []

                # Credits
                item['directors'] = self._extract_credits_by_role(response, "Director")
                item['writers'] = self._extract_credits_by_role(response, "Writer")
                item['composers'] = self._extract_credits_by_role(response, "Music by")
                item['cast'] = self._extract_cast(response)

                item['scraped_at'] = datetime.now().isoformat()

                # Thread-safe counter update
                async with self._scrape_lock:
                    self.movies_scraped += 1
                    current_count = self.movies_scraped

                self.logger.info(f"Scraped #{current_count}: {item['title']} ({item['year']})")
                yield item

                # Scrape reviews (unless skipped for speed)
                if not self.skip_reviews and movie_id:
                    async for review in self._scrape_reviews(movie_id):
                        yield review

            finally:
                await page.close()

        except Exception as e:
            error_str = str(e)
            # Re-raise retryable errors for handling in _scrape_movie_safe
            if any(x in error_str for x in ['cooldown', 'no_peers', 'closed', 'Target page', 'Browser']):
                raise
            self.logger.error(f"Error scraping {url}: {e}")

    async def _scrape_reviews(self, movie_id):
        """Scrape reviews for a movie."""
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
                    review['author'] = container.css('[data-testid="author-link"]::text').get()
                    if not review['author']:
                        review['author'] = container.css('a.ipc-link--base::text').get()
                    review['score'] = container.css('span.ipc-rating-star--rating::text').get()
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
        """Clean up browser on spider close."""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.logger.info(f"Spider closed: {reason}. Total movies scraped: {self.movies_scraped}")
