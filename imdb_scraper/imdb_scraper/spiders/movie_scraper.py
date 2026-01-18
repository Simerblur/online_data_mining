"""
IMDb Movie Scraper - Scrapes movies, cast, directors and reviews.
Authors: Juliusz, Jeffrey, Lin | Course: Online Data Mining - Amsterdam UAS
"""

import re
from datetime import datetime

import scrapy
from scrapy import Selector

from imdb_scraper.items import MovieItem, ReviewItem


class ImdbSpider(scrapy.Spider):
    name = "movie_scraper"
    allowed_domains = ["imdb.com"]
    max_movies = 10000
    max_reviews_per_movie = 100

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.movies_scraped = 0
        self.seen_movie_ids = set()

    def start_requests(self):
        """Generate search URLs for 10000 movies sorted by votes."""
        base_url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc&start={}"

        for start in range(1, 10001, 50):
            yield scrapy.Request(
                base_url.format(start),
                callback=self.parse_search_results,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded', 'timeout': 60000},
                },
                errback=self.errback_close_page,
            )

    async def parse_search_results(self, response):
        """Parse search results and extract movie links using Playwright."""
        page = response.meta.get("playwright_page")
        if page:
            try:
                await page.wait_for_selector('a.ipc-title-link-wrapper', timeout=10000)
            except Exception:
                pass
            content = await page.content()
            await page.close()
            sel = Selector(text=content)
        else:
            sel = response

        # Try multiple selectors to find movie links
        movie_links = (
            sel.css('a.ipc-title-link-wrapper::attr(href)').getall() or
            sel.css('div.dli-title a::attr(href)').getall() or
            sel.css('h3.lister-item-header a::attr(href)').getall()
        )

        for link in movie_links:
            if self.movies_scraped >= self.max_movies:
                break

            # Extract IMDb ID to prevent duplicates
            match = re.search(r'/title/(tt\d+)/', link)
            if not match or match.group(1) in self.seen_movie_ids:
                continue

            self.seen_movie_ids.add(match.group(1))
            self.movies_scraped += 1

            yield scrapy.Request(response.urljoin(link), callback=self.parse_movie, priority=10)

    def parse_movie(self, response):
        """Parse movie page and extract all metadata."""
        # Extract movie ID from URL
        match = re.search(r'/title/tt(\d+)/', response.url)
        movie_id = int(match.group(1)) if match else None

        # Extract genres
        genres = response.css('div.ipc-chip-list__scroller a span::text').getall()

        # Extract year
        year_text = response.xpath('//a[contains(@href, "releaseinfo")]/text()').get()
        year_match = re.search(r'(\d{4})', year_text) if year_text else None
        year = int(year_match.group(1)) if year_match else None

        # Extract rating
        rating_text = response.css('div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text').get()
        try:
            rating = float(rating_text) if rating_text else None
        except ValueError:
            rating = None

        # Extract box office (try multiple selectors)
        box_office_text = response.xpath(
            '//li[@data-testid="title-boxoffice-cumulativeworldwidegross"]//span[contains(@class, "ipc-metadata-list-item__list-content-item")]/text()'
        ).get()
        box_office = int(re.sub(r'[^\d]', '', box_office_text)) if box_office_text else None

        yield MovieItem(
            movie_id=movie_id,
            title=response.css('span.hero__primary-text::text').get(),
            year=year,
            user_score=rating,
            box_office=box_office,
            genres=', '.join(genres) if genres else None,
            genres_list=genres or [],
            directors=self._extract_people(response, 'Director'),
            cast=self._extract_cast(response),
            scraped_at=datetime.now().isoformat(),
        )

        # Follow to reviews page
        if movie_id:
            imdb_id = re.search(r'(tt\d+)', response.url)
            if imdb_id:
                yield scrapy.Request(
                    f'https://www.imdb.com/title/{imdb_id.group(1)}/reviews/',
                    callback=self.parse_reviews,
                    meta={'movie_id': movie_id},
                    priority=20,
                )

    def _extract_people(self, response, role):
        """Extract directors or other crew by role."""
        section = response.xpath(f'//li[@data-testid="title-pc-principal-credit"][.//span[contains(text(), "{role}")]]')
        people = []
        seen = set()

        for link in section.css('a[href*="/name/"]'):
            name = link.css('::text').get()
            href = link.css('::attr(href)').get()
            match = re.search(r'/name/(nm\d+)/', href) if href else None
            person_id = match.group(1) if match else None

            if person_id and person_id not in seen and name:
                seen.add(person_id)
                people.append({'name': name.strip(), 'imdb_person_id': person_id})

        return people

    def _extract_cast(self, response):
        """Extract top 15 cast members."""
        cast = []
        for order, item in enumerate(response.css('div[data-testid="title-cast-item"]')[:15], 1):
            actor_link = item.css('a[data-testid="title-cast-item__actor"]')
            name = actor_link.css('::text').get()
            href = actor_link.css('::attr(href)').get()

            match = re.search(r'/name/(nm\d+)/', href) if href else None
            character = (
                item.css('a[data-testid="cast-item-characters-link"] span::text').get() or
                item.css('span[data-testid="cast-item-characters-link"] span::text').get()
            )

            if name:
                cast.append({
                    'name': name.strip(),
                    'imdb_person_id': match.group(1) if match else None,
                    'character_name': character.strip() if character else None,
                    'cast_order': order,
                })
        return cast

    def parse_reviews(self, response):
        """Parse reviews page and extract user reviews."""
        movie_id = response.meta.get('movie_id')
        count = 0

        for container in response.css('article.user-review-item'):
            if count >= self.max_reviews_per_movie:
                break

            author = (
                container.css('[data-testid="author-link"]::text').get() or
                container.css('a.ipc-link--base::text').get()
            )
            text_parts = container.css('div.ipc-html-content-inner-div::text').getall()
            text = ' '.join(text_parts).strip() if text_parts else None

            if author or text:
                count += 1
                yield ReviewItem(
                    movie_id=movie_id,
                    author=author,
                    score=container.css('span.ipc-rating-star--rating::text').get(),
                    text=text,
                    is_critic=False,
                    review_date=container.css('.review-date::text').get(),
                    scraped_at=datetime.now().isoformat(),
                )

    async def errback_close_page(self, failure):
        """Close Playwright page on error."""
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
