# Author: Juliusz | Online Data Mining - Amsterdam UAS
import asyncio
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import scrapy
from imdb_scraper.items import MovieItem, ReviewItem
from scrapy.selector import Selector


class ImdbSpider(scrapy.Spider):
    # Scrapy Spider to collect movie data from IMDb.
    #
    # This spider performs the following actions:
    # 1. Starts at the IMDb Advanced Search page, sorted by number of votes (popularity).
    # 2. Uses Playwright (headless browser) to render the search results which use infinite scrolling.
    # 3. Pagination: Manually calculates the 'start' parameter to visit subsequent pages (1-50, 51-100, etc.)
    #    because the "Next" button is unreliable.
    # 4. Extracts movie metadata (title, year, score, box office, cast, directors) from the search result items.
    #        Note: To speed up scraping, we extract most data directly from the search result list
    #        instead of visiting each movie page individually when possible.
    # 5. Visits the Reviews page for each movie to collect user reviews.

    name = "movie_scraper"
    allowed_domains = ["imdb.com"]

    # Configuration limits
    max_movies = 10000
    max_reviews_per_movie = 4  # Limit reviews to save time/bandwidth

    def __init__(self, max_movies=10000, *args, **kwargs):
        # Initialize the spider with configurable limits.
        #
        # Args:
        #     max_movies: Maximum number of movies to scrape (default 10000).
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.movies_scraped = 0
        self.seen_movie_ids = set()

        # Load already-scraped movie IDs from database to skip duplicates
        self._load_existing_movie_ids()

    # Custom settings to integrate with Playwright and set headers
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    def _load_existing_movie_ids(self):
        # Load movie IDs already in the database to skip re-scraping.
        # This allows resuming scraping without duplicating data.
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
            # Note: We use the logger after spider is initialized, so just store count
            self._existing_count = len(existing_ids)
        except Exception:
            pass  # If DB read fails, just start fresh

    async def start(self):
        # Entry point for the spider.
        # Generates the first request to the IMDb search page.

        # Log how many movies we're skipping from previous runs
        if hasattr(self, '_existing_count') and self._existing_count > 0:
            self.logger.info(f"Skipping {self._existing_count} movies already in database")

        # Base URL for feature films sorted by popularity (vote count)
        base_url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc"

        yield scrapy.Request(
            base_url,
            callback=self.parse_search_results,
            meta={
                'playwright': True,
                'playwright_include_page': True,  # Keep page open to interact with it if needed
                'playwright_page_goto_kwargs': {
                    'wait_until': 'domcontentloaded',
                    'timeout': 240000,
                }
            }
        )

    async def parse_search_results(self, response):
        # Parses the search results page.
        #
        # This method:
        # 1. Scrolls down to try and load lazy-loaded content.
        # 2. Extracts movie data from the list.
        # 3. Pagination: Calculates the URL for the next batch (page) of results.
        
        self.logger.info(f"Parsing search results from: {response.url}")
        
        page = response.meta.get('playwright_page')
        if not page:
            self.logger.error("No Playwright page object available!")
            return

        try:
            # Scroll to bottom to trigger any lazy loading mechanisms
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)

            # NOTE: Previous versions attempted to click "Load More" buttons.
            # We now rely on direct pagination via URL parameters for reliability,
            # but we still scroll to ensure the current 50 items are rendered.

            # Get the fully rendered HTML content
            content = await page.content()
            selector = Selector(text=content)

            # Extract movie links. Selector targets the specific container for movie titles.
            movie_links = selector.css('a.ipc-title-link-wrapper::attr(href)').getall()
            self.logger.info(f"Found {len(movie_links)} total movie links on this page")

            # Fallback: detailed XPath query if the primary CSS selector fails
            if not movie_links:
                movie_links = selector.xpath('//a[contains(@href, "/title/tt")]/@href').getall()
                # Deduplicate links found via fallback
                seen = set()
                unique_links = []
                for link in movie_links:
                    match = re.search(r'(/title/tt\d+/)', link)
                    if match and match.group(1) not in seen:
                        seen.add(match.group(1))
                        unique_links.append(link)
                movie_links = unique_links
                self.logger.info(f"Fallback xpath found: {len(movie_links)} links")

            # Debugging: Save page if empty (helps identify bot protection/layout changes)
            if not movie_links:
                self.logger.warning(f"No movie links found on {response.url}")
                with open('debug_response.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                self.logger.info("Saved response to debug_response.html")

            # Queue extraction for each movie found
            for link in movie_links:
                if self.max_movies > 0 and self.movies_scraped >= self.max_movies:
                    self.logger.info(f"Reached max movies: {self.movies_scraped}")
                    break

                # Extract generic IMDb ID (tt1234567)
                movie_id_match = re.search(r'/title/(tt\d+)/', link)
                if movie_id_match:
                    movie_id = movie_id_match.group(1)
                    if movie_id in self.seen_movie_ids:
                        continue
                    self.seen_movie_ids.add(movie_id)

                self.movies_scraped += 1
                full_url = response.urljoin(link)
                
                # Yield request for movie details. 
                # Use Playwright to ensure all metadata (data-testid) is rendered.
                yield scrapy.Request(
                    full_url,
                    callback=self.parse_movie,
                    meta={
                        'playwright': True,
                        'playwright_include_page': False,
                        'playwright_context_kwargs': {
                            'ignore_https_errors': True,
                        },
                    }
                )

            # Pagination Logic
            # IMDb lists 50 movies per page. We manually calculate the next 'start' index.
            if self.max_movies == 0 or self.movies_scraped < self.max_movies:
                # Find current start index from URL (default to 1)
                current_start_match = re.search(r'start=(\d+)', response.url)
                current_start = int(current_start_match.group(1)) if current_start_match else 1
                
                # Safety check: stop if we are looping on the same page without finding movies
                if not movie_links:
                    self.logger.warning(f"No new movies found on page start={current_start}. Stopping to avoid infinite loop.")
                    return

                # Calculate next page start (current + 50)
                next_start = current_start + 50
                
                # Construct the next URL
                if 'start=' in response.url:
                    next_page = re.sub(r'start=\d+', f'start={next_start}', response.url)
                else:
                    next_page = f"{response.url}&start={next_start}"

                self.logger.info(f"Generated next page link: {next_page}")
                
                yield scrapy.Request(
                    next_page,
                    callback=self.parse_search_results,
                    meta={
                        'playwright': True,
                        'playwright_include_page': False, # Page object not needed for next request until loaded
                        'playwright_page_goto_kwargs': {
                            'wait_until': 'domcontentloaded',
                            'timeout': 240000,
                        }
                    }
                )

        finally:
            # Critical: Close the Playwright page to prevent memory leaks
            await page.close()

    def parse_movie(self, response):
        # Extracts metadata from a single Movie page.
        #
        # Fields extracted:
        # - ID, Title, Year
        # - User Score (Rating)
        # - Box Office (Worldwide Gross)
        # - Genres, Directors, Cast

        item = MovieItem()

        # Extract numeric ID
        imdb_id_match = re.search(r'/title/tt(\d+)/', response.url)
        movie_id = int(imdb_id_match.group(1)) if imdb_id_match else None
        item['movie_id'] = movie_id

        # Title
        item['title'] = response.css('span.hero__primary-text::text').get()

        # Release Year (found in the header inline list)
        year_text = response.xpath(
            '//ul[contains(@class, "ipc-inline-list")]'
            '//li//a[contains(@href, "releaseinfo")]/text()'
        ).get()
        if year_text:
            year_match = re.search(r'(\d{4})', year_text)
            item['year'] = int(year_match.group(1)) if year_match else None
        else:
            item['year'] = None

        # User Score
        rating_text = response.css(
            'div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text'
        ).get()
        try:
            item['user_score'] = float(rating_text) if rating_text else None
        except ValueError:
            item['user_score'] = None

        # Box Office (Helper method used due to layout variations)
        item['box_office'] = self._extract_box_office(response)

        # New Metadata Fields
        item['release_date'] = response.css("li[data-testid='title-details-releasedate'] a.ipc-metadata-list-item__list-content-item::text").get()
        
        # Runtime
        # Matches "2h 22m" in the span
        runtime_text = response.css("li[data-testid='title-techspec_runtime'] span.ipc-metadata-list-item__list-content-item::text").get()
        item['runtime_minutes'] = self._parse_runtime(runtime_text)
        
        # MPAA Rating
        # Found in Hero section header: <a href="...parentalguide...">R</a>
        item['mpaa_rating'] = response.css('a[href*="parentalguide"]::text').get()
        
        # Production Companies (List)
        item['production_companies'] = response.css("li[data-testid='title-details-companies'] a.ipc-metadata-list-item__list-content-item::text").getall()

        # Genres
        genres = response.css('div.ipc-chip-list__scroller a span::text').getall()
        item['genres'] = ', '.join(genres) if genres else None
        item['genres_list'] = genres if genres else []

        # Directors & Cast (Helper methods)
        item['directors'] = self._extract_credits_by_role(response, "Director")
        item['writers'] = self._extract_credits_by_role(response, "Writer") # New
        item['composers'] = self._extract_credits_by_role(response, "Music by") # New
        item['cast'] = self._extract_cast(response)

        item['scraped_at'] = datetime.now().isoformat()

        yield item

        # Follow link to User Reviews
        full_tt_match = re.search(r'(tt\d+)', response.url)
        if full_tt_match:
            full_tt_id = full_tt_match.group(1)
            reviews_url = f'https://www.imdb.com/title/{full_tt_id}/reviews/'
            yield scrapy.Request(
                reviews_url,
                callback=self.parse_reviews,
                meta={'movie_id': movie_id, 'playwright': False}
            )

    def _parse_runtime(self, text):
        # Syntax Explanation: Parsing complex strings
        # Tries to find "175 min" or "2h 55m" and convert to integer minutes.
        if not text:
            return None
        
        # Match '123 min'
        minutes_match = re.search(r'(\d+)\s*min', text)
        if minutes_match:
            return int(minutes_match.group(1))
            
        # Match '2 hours 55 minutes'
        h_match = re.search(r'(\d+)\s*h', text)
        m_match = re.search(r'(\d+)\s*m', text)
        
        minutes = 0
        if h_match:
            minutes += int(h_match.group(1)) * 60
        if m_match:
            minutes += int(m_match.group(1))
            
        return minutes if minutes > 0 else None

    def _extract_box_office(self, response):
        # Helper to find the Box Office Gross.
        # Checks multiple possible locations/XPaths as IMDb layout varies.
        
        box_office_value = None

        # Strategy 1: Data-TestID (Specific Metadata Item)
        box_office_section = response.xpath(
            '//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]'
            '//span[contains(@class, "ipc-metadata-list-item__list-content-item")]/text()'
        ).get()

        if box_office_section:
            box_office_value = box_office_section

        # Strategy 2: Label search (Look for "Gross worldwide" text)
        if not box_office_value:
            box_office_value = response.xpath(
                '//span[contains(text(), "Gross worldwide")]/following-sibling::span/text()'
            ).get()

        # Strategy 3: Alternative Label search in list items
        if not box_office_value:
            box_office_value = response.xpath(
                '//li[contains(., "Gross worldwide")]//span[@class="ipc-metadata-list-item__list-content-item"]/text()'
            ).get()

        if box_office_value:
            # Clean string: "$2,923,706,026" -> 2923706026
            cleaned = re.sub(r'[^\d]', '', box_office_value)
            if cleaned:
                return int(cleaned)

        return None

    def _extract_credits_by_role(self, response, role_name):
        # Helper to extract people credits (Directors, Writers, Composers) by role name.
        # Syntax Explanation: Dynamic XPath construction
        # We look for a list item (li) with the specific class and text content matching the role.
        # This allows us to reuse logic for different roles.
        
        credits = []
        seen_ids = set()

        # Locate the specific section. Works for Director, Writer, Music by
        # Handles "Director" vs "Directors" pluralization implicitly by partial match if needed, 
        # but usually role headers are specific.
        # We use a flexible XPath to find the label span with the text, then get the parent li.
        
        # Note: IMDb groups these under 'title-pc-principal-credit'
        
        section = response.xpath(
            f'//li[@data-testid="title-pc-principal-credit"]'
            f'[.//span[contains(text(), "{role_name}")]]'
        )
        
        # If not found in principal credits, try extended credits list style (for less popular roles)
        if not section:
             section = response.xpath(
                f'//li[contains(@class, "ipc-metadata-list__item")]'
                f'[.//span[contains(text(), "{role_name}")]]'
            )

        links = section.css('a[href*="/name/"]')
        for link in links:
            name = link.css('::text').get()
            href = link.css('::attr(href)').get()

            person_id_match = re.search(r'/name/(nm\d+)/', href) if href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None

            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)

            if name:
                credits.append({
                    'name': name.strip(),
                    'imdb_person_id': imdb_person_id
                })

        return credits

    # Deprecated: _extract_directors (replaced by _extract_credits_by_role)

    def _extract_cast(self, response):
        # Helper to extract Cast members (Actors) and their Roles.
        cast = []
        seen_ids = set()

        cast_items = response.css('div[data-testid="title-cast-item"]')

        # Limit to top 10 actors to keep data manageable
        for order, item in enumerate(cast_items[:10], start=1):
            actor_link = item.css('a[data-testid="title-cast-item__actor"]')
            actor_name = actor_link.css('::text').get()
            actor_href = actor_link.css('::attr(href)').get()

            person_id_match = re.search(r'/name/(nm\d+)/', actor_href) if actor_href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None

            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)

            # Character Name (often nested differently)
            character_name = item.css(
                'a[data-testid="cast-item-characters-link"] span::text'
            ).get()
            if not character_name:
                character_name = item.css(
                    'span[data-testid="cast-item-characters-link"] span::text'
                ).get()

            if actor_name:
                cast.append({
                    'name': actor_name.strip(),
                    'imdb_person_id': imdb_person_id,
                    'character_name': character_name.strip() if character_name else None,
                    'cast_order': order
                })

        return cast

    def parse_reviews(self, response):
        # Extracts User Reviews from the reviews page.
        movie_id = response.meta.get('movie_id')
        reviews_scraped = 0

        review_containers = response.css('article.user-review-item')

        for container in review_containers:
            if reviews_scraped >= self.max_reviews_per_movie:
                break

            review = ReviewItem()
            review['movie_id'] = movie_id
            
            # Author Name
            review['author'] = container.css('[data-testid="author-link"]::text').get()
            if not review['author']:
                review['author'] = container.css('a.ipc-link--base::text').get()

            # Rating Score
            score_text = container.css('span.ipc-rating-star--rating::text').get()
            review['score'] = score_text

            # Review Body
            text_parts = container.css('div.ipc-html-content-inner-div::text').getall()
            review['text'] = ' '.join(text_parts).strip() if text_parts else None

            review['is_critic'] = False
            review['review_date'] = container.css('.review-date::text').get()
            review['scraped_at'] = datetime.now().isoformat()

            if review['author'] or review['text']:
                reviews_scraped += 1
                yield review

