import re
from datetime import datetime

import scrapy
from imdb_scraper.items import MovieItem


class ImdbTop250Spider(scrapy.Spider):
    """Spider to scrape IMDb Top 250 movies."""

    name = "imdb_top250"
    allowed_domains = ["imdb.com"]
    start_urls = ["https://www.imdb.com/chart/top/"]

    # Limit to 5 movies for testing
    max_movies = 5
    movies_scraped = 0

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    def parse(self, response):
        """Parse the Top 250 chart page and follow links to individual movies."""
        # Get movie links from the chart
        movie_links = response.css('ul.ipc-metadata-list li.ipc-metadata-list-summary-item a.ipc-title-link-wrapper::attr(href)').getall()

        for link in movie_links:
            if self.movies_scraped >= self.max_movies:
                break

            self.movies_scraped += 1
            full_url = response.urljoin(link)
            yield scrapy.Request(full_url, callback=self.parse_movie)

    def parse_movie(self, response):
        """Parse individual movie page and extract details."""
        item = MovieItem()

        # Extract IMDb ID from URL
        imdb_id_match = re.search(r'/title/(tt\d+)/', response.url)
        item['imdb_id'] = imdb_id_match.group(1) if imdb_id_match else None

        # Title
        item['title'] = response.css('span.hero__primary-text::text').get()

        # Year - from release info link
        year_text = response.xpath('//ul[contains(@class, "ipc-inline-list")]//li//a[contains(@href, "releaseinfo")]/text()').get()
        if year_text:
            year_match = re.search(r'(\d{4})', year_text)
            item['year'] = int(year_match.group(1)) if year_match else None
        else:
            item['year'] = None

        # Runtime - look for pattern like "2h 22m" or "1h 30m"
        runtime_text = response.xpath('//ul[contains(@class, "ipc-inline-list--show-dividers")]//li/text()').getall()
        item['runtime_minutes'] = None
        for text in runtime_text:
            runtime_match = re.search(r'(?:(\d+)h)?\s*(?:(\d+)m)?', text)
            if runtime_match and (runtime_match.group(1) or runtime_match.group(2)):
                hours = int(runtime_match.group(1)) if runtime_match.group(1) else 0
                minutes = int(runtime_match.group(2)) if runtime_match.group(2) else 0
                if hours > 0 or minutes > 0:
                    item['runtime_minutes'] = hours * 60 + minutes
                    break

        # Rating
        rating_text = response.css('div[data-testid="hero-rating-bar__aggregate-rating__score"] span::text').get()
        item['rating'] = rating_text

        # User score (number of votes)
        user_score_text = response.css('div[data-testid="hero-rating-bar__aggregate-rating__score"] + div + div::text').get()
        if user_score_text:
            # Parse values like "2.9M" or "1.2K"
            user_score_match = re.search(r'([\d.]+)([MK])?', user_score_text)
            if user_score_match:
                value = float(user_score_match.group(1))
                multiplier = user_score_match.group(2)
                if multiplier == 'M':
                    value *= 1000000
                elif multiplier == 'K':
                    value *= 1000
                item['user_score'] = value
            else:
                item['user_score'] = None
        else:
            item['user_score'] = None

        # Plot
        item['plot'] = response.css('span[data-testid="plot-xl"]::text').get()
        if not item['plot']:
            item['plot'] = response.css('span[data-testid="plot-xs_to_m"]::text').get()

        # Genres - from the chip list scroller
        genres = response.css('div.ipc-chip-list__scroller a span::text').getall()
        item['genres'] = ', '.join(genres) if genres else None

        # Director - from the principal credits section
        director = response.xpath('//li[@data-testid="title-pc-principal-credit"]//a/text()').get()
        item['director'] = director

        # Metascore
        metascore_text = response.css('span.metacritic-score-box::text').get()
        item['metascore'] = int(metascore_text) if metascore_text and metascore_text.isdigit() else None

        # Budget and Box Office - these require parsing the "Box office" section
        # Usually not available on the main page, would need additional request
        item['budget'] = None
        item['box_office'] = None
        item['studio'] = None

        # Timestamp
        item['scraped_at'] = datetime.now().isoformat()

        yield item
