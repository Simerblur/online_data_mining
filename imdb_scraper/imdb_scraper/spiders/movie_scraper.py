import re
from datetime import datetime

import scrapy
from imdb_scraper.items import MovieItem, ReviewItem


class ImdbSpider(scrapy.Spider):
    """Spider to scrape IMDb movies with cast, directors, and reviews."""

    name = "movie_scraper"
    allowed_domains = ["imdb.com"]

    # scraping limits
    max_movies = 1000
    max_reviews_per_movie = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.movies_scraped = 0
        self.seen_movie_ids = set()

    # browser user agent - removed concurrency overrides to use settings.py values
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        """Generate search URLs to get 1000+ movies sorted by number of votes."""
        # IMDb search returns 50 results per page, we need 20 pages for 1000 movies
        # Using feature films sorted by number of votes (most popular first)
        # Note: IMDb ignores the count parameter and always returns ~50 per page
        base_url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc&start={}"

        # Generate 20 pages (50 movies each = 1000 total)
        for start in range(1, 1001, 50):
            yield scrapy.Request(
                base_url.format(start),
                callback=self.parse_search_results,
                meta={
                    'playwright': True,
                    'playwright_include_page': False,
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'domcontentloaded',  # Faster than networkidle
                        'timeout': 30000,
                    },
                }
            )

    def parse_search_results(self, response):
        """Parse IMDb search results page and follow movie links."""
        # Log response for debugging
        self.logger.info(f"Parsing search results from: {response.url}")
        self.logger.info(f"Response status: {response.status}")

        # Get movie links - try multiple selectors for different IMDb layouts
        movie_links = []

        # Modern IMDb layout (2024+) - uses ipc-title-link-wrapper
        movie_links = response.css('a.ipc-title-link-wrapper::attr(href)').getall()
        self.logger.info(f"ipc-title-link-wrapper found: {len(movie_links)} links")

        # Fallback: dli-title class (search results list item)
        if not movie_links:
            movie_links = response.css('div.dli-title a::attr(href)').getall()
            self.logger.info(f"dli-title found: {len(movie_links)} links")

        # Fallback: older lister layout
        if not movie_links:
            movie_links = response.css('h3.lister-item-header a::attr(href)').getall()
            self.logger.info(f"lister-item-header found: {len(movie_links)} links")

        # Fallback: any link to /title/tt
        if not movie_links:
            movie_links = response.xpath('//a[contains(@href, "/title/tt")]/@href').getall()
            # Deduplicate and filter
            seen = set()
            unique_links = []
            for link in movie_links:
                # Extract just the title ID part
                match = re.search(r'(/title/tt\d+/)', link)
                if match and match.group(1) not in seen:
                    seen.add(match.group(1))
                    unique_links.append(link)
            movie_links = unique_links
            self.logger.info(f"xpath fallback found: {len(movie_links)} links")

        if not movie_links:
            self.logger.warning(f"No movie links found on {response.url}")
            # Save page for debugging
            with open('debug_response.html', 'w', encoding='utf-8') as f:
                f.write(response.text)
            self.logger.info("Saved response to debug_response.html")

        for link in movie_links:
            if self.movies_scraped >= self.max_movies:
                return

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

        # get cast items
        cast_items = response.css('div[data-testid="title-cast-item"]')

        # limit to top 15 actors
        for order, item in enumerate(cast_items[:15], start=1):
            # get actor name and link
            actor_link = item.css('a[data-testid="title-cast-item__actor"]')
            actor_name = actor_link.css('::text').get()
            actor_href = actor_link.css('::attr(href)').get()

            # extract person id
            person_id_match = re.search(r'/name/(nm\d+)/', actor_href) if actor_href else None
            imdb_person_id = person_id_match.group(1) if person_id_match else None

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
