# Author: Juliusz | Online Data Mining - Amsterdam UAS
# IMDb Spider: Scrapes movies, cast, directors, and reviews using infinite scroll

import asyncio
import re
from datetime import datetime

import scrapy
from imdb_scraper.items import MovieItem, ReviewItem
from scrapy.selector import Selector


class ImdbSpider(scrapy.Spider):
    """Spider to scrape IMDb movies with cast, directors, and reviews."""

    name = "movie_scraper"
    allowed_domains = ["imdb.com"]

    # scraping limits
    max_movies = 1000
    max_reviews_per_movie = 4  # reduced from 100 for performance

    def __init__(self, max_movies=1000, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.movies_scraped = 0
        self.seen_movie_ids = set()

    # browser user agent - removed concurrency overrides to use settings.py values
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    async def start(self):
        """Generate search URLs to get 1000+ movies sorted by number of votes."""
        # IMDb search returns 50 results per page, uses infinite scroll / "See more" button
        # Using feature films sorted by number of votes (most popular first)
        base_url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc"

        yield scrapy.Request(
            base_url,
            callback=self.parse_search_results,
            meta={
                'playwright': True,
                'playwright_include_page': True,  # Need page object for infinite scroll
                'playwright_page_goto_kwargs': {
                    'wait_until': 'domcontentloaded',
                    'timeout': 240000,
                }
            }
        )

    async def parse_search_results(self, response):
        """Parse IMDb search results page using infinite scroll to load all movies."""
        self.logger.info(f"Parsing search results from: {response.url}")
        self.logger.info(f"Response status: {response.status}")

        page = response.meta.get('playwright_page')
        if not page:
            self.logger.error("No Playwright page object available!")
            return

        try:
            # Calculate how many times we need to click "50 more" button
            # Each click loads 50 more movies
            clicks_needed = (self.max_movies // 50) + 1
            self.logger.info(f"Will attempt up to {clicks_needed} clicks to load {self.max_movies} movies")

            for click_num in range(clicks_needed):
                if self.movies_scraped >= self.max_movies:
                    self.logger.info(f"Reached max movies limit ({self.max_movies})")
                    break

                # Scroll to bottom to trigger lazy loading and find the button
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1)

                # Try to find and click the "50 more" or "See more" button
                # IMDb uses various button texts/selectors
                button_selectors = [
                    'button.ipc-see-more__button',
                    'button:has-text("50 more")',
                    'button:has-text("more")',
                    'span.ipc-see-more__text',
                    '[data-testid="adv-search-get-results"]',
                ]

                button_clicked = False
                for selector in button_selectors:
                    try:
                        button = page.locator(selector).first
                        if await button.is_visible(timeout=3000):
                            self.logger.info(f"Found button with selector: {selector}, clicking...")
                            await button.scroll_into_view_if_needed()
                            await asyncio.sleep(0.5)
                            await button.click()
                            button_clicked = True
                            self.logger.info(f"Click {click_num + 1}/{clicks_needed} completed")
                            # Wait for new content to load
                            await asyncio.sleep(2)
                            break
                    except Exception as e:
                        continue

                if not button_clicked:
                    self.logger.info(f"No more 'load more' button found after {click_num} clicks")
                    break

            # Get final page content after all scrolling/clicking
            content = await page.content()
            selector = Selector(text=content)

            # Extract movie links from the fully loaded page
            movie_links = selector.css('a.ipc-title-link-wrapper::attr(href)').getall()
            self.logger.info(f"Found {len(movie_links)} total movie links after infinite scroll")

            # Fallback selectors if primary didn't work
            if not movie_links:
                movie_links = selector.xpath('//a[contains(@href, "/title/tt")]/@href').getall()
                # Deduplicate
                seen = set()
                unique_links = []
                for link in movie_links:
                    match = re.search(r'(/title/tt\d+/)', link)
                    if match and match.group(1) not in seen:
                        seen.add(match.group(1))
                        unique_links.append(link)
                movie_links = unique_links
                self.logger.info(f"Fallback xpath found: {len(movie_links)} links")

            if not movie_links:
                self.logger.warning(f"No movie links found on {response.url}")
                # Save page for debugging
                with open('debug_response.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                self.logger.info("Saved response to debug_response.html")

            # Process all found movie links
            for link in movie_links:
                if self.movies_scraped >= self.max_movies:
                    self.logger.info(f"Reached max movies: {self.movies_scraped}")
                    break

                # Extract movie ID to avoid duplicates
                movie_id_match = re.search(r'/title/(tt\d+)/', link)
                if movie_id_match:
                    movie_id = movie_id_match.group(1)
                    if movie_id in self.seen_movie_ids:
                        continue
                    self.seen_movie_ids.add(movie_id)

                self.movies_scraped += 1
                full_url = response.urljoin(link)
                # Movie pages don't need Playwright - use regular request for speed
                yield scrapy.Request(
                    full_url,
                    callback=self.parse_movie,
                    meta={'playwright': False}
                )

            self.logger.info(f"Total movies queued for scraping: {self.movies_scraped}")

        finally:
            # Always close the page to free resources
            await page.close()

    def parse_movie(self, response):
        item = MovieItem()

        # extract numeric id from url
        imdb_id_match = re.search(r'/title/tt(\d+)/', response.url)
        movie_id = int(imdb_id_match.group(1)) if imdb_id_match else None
        item['movie_id'] = movie_id

        # get title
        item['title'] = response.css('span.hero__primary-text::text').get()

        # get release year
        year_text = response.xpath(
            '//ul[contains(@class, "ipc-inline-list")]'
            '//li//a[contains(@href, "releaseinfo")]/text()'
        ).get()
        if year_text:
            year_match = re.search(r'(\d{4})', year_text)
            item['year'] = int(year_match.group(1)) if year_match else None
        else:
            item['year'] = None

        # get imdb rating
        rating_text = response.css(
            'div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text'
        ).get()
        if rating_text:
            try:
                item['user_score'] = float(rating_text)
            except ValueError:
                item['user_score'] = None
        else:
            item['user_score'] = None

        # get box office (worldwide gross)
        item['box_office'] = self._extract_box_office(response)

        # get genres list
        genres = response.css('div.ipc-chip-list__scroller a span::text').getall()
        item['genres'] = ', '.join(genres) if genres else None
        item['genres_list'] = genres if genres else []

        # get directors
        item['directors'] = self._extract_directors(response)

        # get cast
        item['cast'] = self._extract_cast(response)

        # add timestamp
        item['scraped_at'] = datetime.now().isoformat()

        yield item

        # follow to reviews page (no Playwright needed)
        full_tt_match = re.search(r'(tt\d+)', response.url)
        if full_tt_match:
            full_tt_id = full_tt_match.group(1)
            reviews_url = f'https://www.imdb.com/title/{full_tt_id}/reviews/'
            yield scrapy.Request(
                reviews_url,
                callback=self.parse_reviews,
                meta={'movie_id': movie_id, 'playwright': False}
            )

    def _extract_box_office(self, response):
        """Extract worldwide gross box office from movie page."""
        # Try to find the box office section
        # Look for "Gross worldwide" or "Worldwide Gross" label
        box_office_value = None

        # Method 1: Look in the box office section by data-testid
        box_office_section = response.xpath(
            '//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]'
            '//span[contains(@class, "ipc-metadata-list-item__list-content-item")]/text()'
        ).get()

        if box_office_section:
            box_office_value = box_office_section

        # Method 2: Fallback - look for any element containing gross worldwide
        if not box_office_value:
            box_office_value = response.xpath(
                '//span[contains(text(), "Gross worldwide")]/following-sibling::span/text()'
            ).get()

        # Method 3: Another fallback pattern
        if not box_office_value:
            box_office_value = response.xpath(
                '//li[contains(., "Gross worldwide")]//span[@class="ipc-metadata-list-item__list-content-item"]/text()'
            ).get()

        if box_office_value:
            # Parse the value - remove currency symbol and convert to integer
            # e.g., "$2,923,706,026" -> 2923706026
            cleaned = re.sub(r'[^\d]', '', box_office_value)
            if cleaned:
                return int(cleaned)

        return None

    def _extract_directors(self, response):
        directors = []
        seen_ids = set()

        # find director section
        director_section = response.xpath(
            '//li[@data-testid="title-pc-principal-credit"]'
            '[.//span[contains(text(), "Director")]]'
        )

        # get director links
        director_links = director_section.css('a[href*="/name/"]')
        for link in director_links:
            name = link.css('::text').get()
            href = link.css('::attr(href)').get()

            # extract person id
            person_id_match = re.search(r'/name/(nm\d+)/', href) if href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None

            # skip duplicates
            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)

            if name:
                directors.append({
                    'name': name.strip(),
                    'imdb_person_id': imdb_person_id
                })

        return directors

    def _extract_cast(self, response):
        cast = []
        seen_ids = set()  # track seen actor IDs to avoid duplicates

        # get cast items
        cast_items = response.css('div[data-testid="title-cast-item"]')

        # limit to top 10 actors (reduced from 15 for performance)
        for order, item in enumerate(cast_items[:10], start=1):
            # get actor name and link
            actor_link = item.css('a[data-testid="title-cast-item__actor"]')
            actor_name = actor_link.css('::text').get()
            actor_href = actor_link.css('::attr(href)').get()

            # extract person id
            person_id_match = re.search(r'/name/(nm\d+)/', actor_href) if actor_href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None

            # skip duplicates (same actor with multiple roles)
            if imdb_person_id and imdb_person_id in seen_ids:
                continue
            if imdb_person_id:
                seen_ids.add(imdb_person_id)

            # get character name
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
        movie_id = response.meta.get('movie_id')
        reviews_scraped = 0

        # get review containers
        review_containers = response.css('article.user-review-item')

        for container in review_containers:
            # check limit
            if reviews_scraped >= self.max_reviews_per_movie:
                break

            review = ReviewItem()
            review['movie_id'] = movie_id

            # get author name
            review['author'] = container.css(
                '[data-testid="author-link"]::text'
            ).get()
            if not review['author']:
                review['author'] = container.css('a.ipc-link--base::text').get()

            # get rating score
            score_text = container.css('span.ipc-rating-star--rating::text').get()
            review['score'] = score_text

            # get review text
            text_parts = container.css('div.ipc-html-content-inner-div::text').getall()
            review['text'] = ' '.join(text_parts).strip() if text_parts else None

            # user reviews not critics
            review['is_critic'] = False

            # get review date
            review_date = container.css('.review-date::text').get()
            review['review_date'] = review_date

            # add timestamp
            review['scraped_at'] = datetime.now().isoformat()

            # yield if has content
            if review['author'] or review['text']:
                reviews_scraped += 1
                yield review
