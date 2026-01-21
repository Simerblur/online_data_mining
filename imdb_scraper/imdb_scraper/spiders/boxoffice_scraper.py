# Author: Lin | Online Data Mining - Amsterdam UAS
"""
Box Office Mojo Scraper - fetches financial data for movies.

Box Office Mojo uses the same IMDB tt IDs, so we read movies from the IMDB
database and fetch their box office data directly using:
    https://www.boxofficemojo.com/title/tt{movie_id}/

Data scraped:
- production_budget: Production budget
- domestic_opening: Domestic opening weekend gross
- domestic_total: Domestic total gross
- international_total: International total gross
- worldwide_total: Worldwide total gross
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path

import scrapy
from imdb_scraper.items import BoxOfficeMojoItem


class BoxOfficeMojoSpider(scrapy.Spider):
    """Spider to scrape Box Office Mojo financial data for IMDB movies."""

    name = "boxoffice_scraper"
    allowed_domains = ["boxofficemojo.com"]

    # Use same settings as IMDB scraper - Box Office Mojo has same anti-bot protection
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 2,
    }

    def __init__(self, max_movies=1000, imdb_db_path="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.movies_processed = 0

        # Set IMDB database path (auto-detect if not provided)
        if imdb_db_path:
            self.imdb_db_path = Path(imdb_db_path)
        else:
            # Auto-detect: look in common locations
            possible_paths = [
                Path(__file__).parent.parent.parent / "output" / "movies.db",
                Path("output/movies.db"),
                Path("../output/movies.db"),
            ]
            self.imdb_db_path = None
            for p in possible_paths:
                if p.exists():
                    self.imdb_db_path = p
                    break

    def start_requests(self):
        """Read movies from IMDB database and request Box Office Mojo pages."""
        if not self.imdb_db_path or not self.imdb_db_path.exists():
            self.logger.error(f"IMDB database not found. Run movie_scraper first.")
            return

        self.logger.info(f"Reading movies from: {self.imdb_db_path}")

        conn = sqlite3.connect(self.imdb_db_path)
        cursor = conn.cursor()

        # Get movies from IMDB database
        cursor.execute("""
            SELECT movie_id, title, year
            FROM movie
            ORDER BY movie_id
            LIMIT ?
        """, (self.max_movies,))

        movies = cursor.fetchall()
        conn.close()

        self.logger.info(f"Found {len(movies)} movies to fetch from Box Office Mojo")

        for movie_id, title, year in movies:
            # Box Office Mojo URL uses the full tt ID format with 7-digit zero-padding
            # e.g., movie_id 111161 becomes tt0111161
            imdb_id = f"tt{movie_id:07d}"
            url = f"https://www.boxofficemojo.com/title/{imdb_id}/"

            self.logger.info(f"Queuing: {title} ({year}) -> {url}")

            yield scrapy.Request(
                url=url,
                callback=self.parse_boxoffice,
                meta={
                    'movie_id': movie_id,
                    'title': title,
                    'year': year,
                    # Use Playwright with proxy to bypass anti-bot protection
                    'playwright': True,
                    'playwright_include_page': False,
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'domcontentloaded',
                        'timeout': 60000,
                    },
                },
                errback=self.handle_error,
            )

    def parse_boxoffice(self, response):
        """Parse Box Office Mojo page for financial data."""
        movie_id = response.meta['movie_id']
        title = response.meta['title']

        self.movies_processed += 1
        self.logger.info(f"[{self.movies_processed}/{self.max_movies}] Parsing: {title}")

        # Get all text for regex parsing
        page_text = ' '.join(response.css('body *::text').getall())
        page_text = re.sub(r'\s+', ' ', page_text)

        item = BoxOfficeMojoItem()
        item['movie_id'] = movie_id
        item['scraped_at'] = datetime.now().isoformat()

        # Extract production budget
        item['production_budget'] = self._extract_budget(response, page_text)

        # Extract domestic opening weekend
        item['domestic_opening'] = self._extract_domestic_opening(response, page_text)

        # Extract regional totals from the summary section
        domestic, international, worldwide = self._extract_regional_totals(response, page_text)
        item['domestic_total'] = domestic
        item['international_total'] = international
        item['worldwide_total'] = worldwide

        # Extract domestic distributor
        item['domestic_distributor'] = self._extract_domestic_distributor(response, page_text)

        self.logger.info(
            f"  Budget: {item['production_budget']}, "
            f"Domestic: {item['domestic_total']}, "
            f"International: {item['international_total']}, "
            f"Worldwide: {item['worldwide_total']}, "
            f"Distributor: {item['domestic_distributor']}"
        )

        yield item

    def _parse_money(self, text):
        """Parse money string like '$25,000,000' to integer."""
        if not text:
            return None
        # Remove currency symbols and commas
        cleaned = re.sub(r'[^\d]', '', str(text))
        if cleaned:
            return int(cleaned)
        return None

    def _extract_budget(self, response, page_text):
        """Extract production budget."""
        # Method 1: Look for Budget in spans (common pattern)
        budget_spans = response.xpath(
            '//span[contains(text(), "Budget")]/following-sibling::span/text()'
        ).getall()
        for span in budget_spans:
            val = self._parse_money(span)
            if val and val > 100000:  # Sanity check: budget > $100k
                return val

        # Method 2: Regex on page text
        match = re.search(r'Budget\s*\$?([\d,]+)', page_text, re.IGNORECASE)
        if match:
            val = self._parse_money(match.group(1))
            if val and val > 100000:
                return val

        return None

    def _extract_domestic_opening(self, response, page_text):
        """Extract domestic opening weekend gross."""
        # Method 1: Look for "Domestic Opening" or "Opening" section
        opening_spans = response.xpath(
            '//span[contains(text(), "Opening")]/following-sibling::span/text()'
        ).getall()
        for span in opening_spans:
            val = self._parse_money(span)
            if val and val > 1000:  # Sanity check
                return val

        # Method 2: Look for "Domestic Opening" with dollar amount
        match = re.search(r'Domestic Opening\s*\$?([\d,]+)', page_text, re.IGNORECASE)
        if match:
            return self._parse_money(match.group(1))

        # Method 3: Look in performance table for opening weekend link
        match = re.search(r'Opening\s+\$?([\d,]+)', page_text)
        if match:
            return self._parse_money(match.group(1))

        return None

    def _extract_regional_totals(self, response, page_text):
        """Extract domestic, international, and worldwide totals."""
        domestic = None
        international = None
        worldwide = None

        # Method 1: Look for the summary box pattern
        # "Domestic (XX.X%)" followed by dollar amount
        # "International (XX.X%)" followed by dollar amount
        # "Worldwide" followed by dollar amount

        # Try to find money amounts near these labels
        domestic_match = re.search(
            r'Domestic\s*(?:\([^)]+\))?\s*\$?([\d,]+)',
            page_text,
            re.IGNORECASE
        )
        if domestic_match:
            domestic = self._parse_money(domestic_match.group(1))

        international_match = re.search(
            r'International\s*(?:\([^)]+\))?\s*\$?([\d,]+)',
            page_text,
            re.IGNORECASE
        )
        if international_match:
            international = self._parse_money(international_match.group(1))

        worldwide_match = re.search(
            r'Worldwide\s*\$?([\d,]+)',
            page_text,
            re.IGNORECASE
        )
        if worldwide_match:
            worldwide = self._parse_money(worldwide_match.group(1))

        # Method 2: Look for spans with money class or data attributes
        money_spans = response.css('span.money::text, span[class*="mojo-"]::text').getall()
        for span in money_spans:
            val = self._parse_money(span)
            if val and val > worldwide if worldwide else 0:
                worldwide = val

        # Method 3: Parse from table if present
        # Box Office Mojo often has a "By Region" table
        table_rows = response.css('table tr')
        for row in table_rows:
            cells = row.css('td::text, td span::text').getall()
            row_text = ' '.join(cells)

            if 'Domestic' in row_text and not domestic:
                for cell in cells:
                    val = self._parse_money(cell)
                    if val:
                        domestic = val
                        break

            if 'International' in row_text and not international:
                for cell in cells:
                    val = self._parse_money(cell)
                    if val:
                        international = val
                        break

            if 'Worldwide' in row_text and not worldwide:
                for cell in cells:
                    val = self._parse_money(cell)
                    if val:
                        worldwide = val
                        break

        return domestic, international, worldwide

    def _extract_domestic_distributor(self, response, page_text):
        """Extract domestic distributor name."""
        # Method 1: Look for "Domestic Distributor" or "Distributor" in spans
        distributor = response.xpath(
            '//span[contains(text(), "Distributor")]/following-sibling::span/text()'
        ).get()
        if distributor:
            return distributor.strip()

        # Method 2: Regex search for "Domestic Distributor" pattern
        # "Domestic Distributor Warner Bros."
        match = re.search(r'Domestic Distributor\s+([A-Za-z0-9 .,&]+?)(?:\s+See full command|(?=\s+See full)|$)', page_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
            
        return None

    def handle_error(self, failure):
        """Handle request errors (404, timeout, etc.)."""
        request = failure.request
        title = request.meta.get('title', 'Unknown')
        self.logger.warning(f"Failed to fetch Box Office data for: {title} - {failure.value}")
