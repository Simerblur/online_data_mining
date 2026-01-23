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

import re  # This imports the 're' module, which stands for regular expressions. Regular expressions are special patterns used to search for and manipulate text.
import sqlite3  # This imports the 'sqlite3' module, which lets us connect to and query SQLite databases.
from datetime import datetime  # This imports the 'datetime' class from the 'datetime' module. 'datetime' helps us work with dates and times in Python.
from pathlib import Path  # This imports the 'Path' class from the 'pathlib' module. 'Path' makes it easy to handle file and folder paths in a way that works on different computers.

import scrapy  # This imports the Scrapy framework, which is a powerful library for web scraping in Python.
from imdb_scraper.items import BoxOfficeMojoItem  # This imports custom data classes (called Items) from our project's items.py file.


class BoxOfficeMojoSpider(scrapy.Spider):  # This defines our custom spider class that inherits from Scrapy's base Spider class.
    """Spider to scrape Box Office Mojo financial data for IMDB movies."""

    name = "boxoffice_scraper"  # This gives our spider a unique name. Scrapy uses this name to identify and run the spider.
    allowed_domains = ["boxofficemojo.com"]  # This restricts the spider to only scrape pages from boxofficemojo.com.

    # Use same settings as IMDB scraper - Box Office Mojo has same anti-bot protection
    custom_settings = {  # These are special settings that override Scrapy's default behavior for this spider.
        # The User-Agent is like an ID that tells the website what kind of browser we're pretending to be.
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 2,  # This adds a 2-second delay between requests to the same website.
        'CONCURRENT_REQUESTS': 2,  # This limits the spider to making only 2 requests at the same time.
    }

    def __init__(self, max_movies=20000, imdb_db_path="", *args, **kwargs):  # This is the constructor method that runs when we create a new spider instance.
        """
        Constructor called when spider starts.
        Args:
            max_movies: Limit total movies to process.
            imdb_db_path: Optional custom path to the existing movies.db.
            *args, **kwargs: Standard Scrapy arguments passed to parent class.
        """
        super().__init__(*args, **kwargs)  # This calls the parent class's __init__ method to set up standard Scrapy features.
        self.max_movies = int(max_movies)  # We store the max_movies limit as an integer.
        self.movies_processed = 0  # We initialize a counter to track how many movies we have processed.

        # Set IMDB database path (auto-detect if not provided)
        if imdb_db_path:  # Check if the user provided a specific path.
            self.imdb_db_path = Path(imdb_db_path)  # If so, convert it to a Path object.
        else:  # If no path was provided, use a default list to find it.
            # Auto-detect: look in common locations relative to this file
            possible_paths = [  # Define a list of possible locations for the database file.
                Path(__file__).parent.parent.parent / "output" / "movies.db",  # Look in ../../../output/movies.db
                Path("output/movies.db"),  # Look in ./output/movies.db
                Path("../output/movies.db"),  # Look in ../output/movies.db
            ]
            self.imdb_db_path = None  # Initialize the path variable as None.
            for p in possible_paths:  # Loop through each possible path.
                if p.exists():  # Check if the file actually exists at this path.
                    self.imdb_db_path = p  # If found, save the path.
                    break  # Stop searching.

    def start_requests(self):  # This method is the entry point for the spider; it generates the initial requests.
        """
        Entry point for the spider. 
        Instead of a static list of URLs, we generate requests dynamically 
        by reading the movie IDs we already collected in our local database.
        """
        if not self.imdb_db_path or not self.imdb_db_path.exists():  # Check if we successfully found the database.
            self.logger.error(f"IMDB database not found. Run movie_scraper first.")  # Log an error if missing.
            return  # Stop execution.

        self.logger.info(f"Reading movies from: {self.imdb_db_path}")  # Log which database file we are reading.

        # Connect to SQLite to get the list of known movies
        conn = sqlite3.connect(self.imdb_db_path)  # Open a connection to the SQLite database.
        cursor = conn.cursor()  # Create a cursor to execute SQL commands.

        # Get movies from IMDB database using SQL query
        cursor.execute("""
            SELECT movie_id, title, year
            FROM movie
            ORDER BY movie_id
            LIMIT ?
        """, (self.max_movies,))  # Execute a query to select movie ID, title, and year for the top 'max_movies'.

        movies = cursor.fetchall()  # Fetch all results from the query into a list.
        # Important: Close the connection to release the file lock
        conn.close()  # Close the database connection.

        self.logger.info(f"Found {len(movies)} movies to fetch from Box Office Mojo")  # Log how many movies we found.

        # Loop through each movie found in DB
        for movie_id, title, year in movies:  # Iterate over each movie tuple (id, title, year).
            # Format: 'tt' + 7 digits (zero-padded). e.g., 123 -> tt0000123
            imdb_id = f"tt{movie_id:07d}"  # Format the integer ID into the standard "tt1234567" string format.
            
            # Construct the target URL
            url = f"https://www.boxofficemojo.com/title/{imdb_id}/"  # Build the Box Office Mojo URL.

            self.logger.info(f"Queuing: {title} ({year}) -> {url}")  # Log the URL we are about to request.

            # Yield a new Scraping Request to the engine
            yield scrapy.Request(  # Create a new Request object.
                url=url,  # The URL to visit.
                callback=self.parse_boxoffice,  # The method to call when the page is downloaded.
                meta={  # 'meta' dictionary passes data between requests.
                    'movie_id': movie_id,  # Pass the movie ID.
                    'title': title,  # Pass the movie title.
                    'year': year,  # Pass the movie year.
                    # Flag to use Playwright with proxy to bypass anti-bot protection if configured
                    'playwright': True,  # Enable Playwright (if middleware is set up).
                    'playwright_include_page': False,  # We don't need the full Page object, just HTML.
                    'playwright_page_goto_kwargs': {  # Arguments for the page loading.
                        'wait_until': 'domcontentloaded', # Wait until HTML is parsed.
                        'timeout': 60000,  # 60-second timeout.
                    },
                },
                errback=self.handle_error,  # Function to handle errors (like 404s).
            )

    def parse_boxoffice(self, response):  # This method handles the response from the server.
        """Parse the downloaded Box Office Mojo page for financial data."""
        movie_id = response.meta['movie_id']  # Retrieve movie ID from meta data.
        title = response.meta['title']  # Retrieve title from meta data.

        self.movies_processed += 1  # Increment the processed counter.
        self.logger.info(f"[{self.movies_processed}/{self.max_movies}] Parsing: {title}")  # Log progress.

        # Get all text content from body for easier regex parsing
        # ' '.join() combines the list of text snippets into one long string
        page_text = ' '.join(response.css('body *::text').getall())  # Extract all visible text from the page body.
        page_text = re.sub(r'\s+', ' ', page_text)  # Collapse multiple spaces into one space using regex.

        item = BoxOfficeMojoItem()  # Create a new item object to store our data.
        item['movie_id'] = movie_id  # Set the movie ID.
        item['scraped_at'] = datetime.now().isoformat()  # Set the current timestamp.

        # Extract specific financial fields using helper methods
        item['production_budget'] = self._extract_budget(response, page_text)  # Extract budget.
        item['domestic_opening'] = self._extract_domestic_opening(response, page_text)  # Extract opening weekend.

        # Extract regional totals (Domestic/International/Worldwide)
        domestic, international, worldwide = self._extract_regional_totals(response, page_text)  # Extract totals.
        item['domestic_total'] = domestic  # Store domestic total.
        item['international_total'] = international  # Store international total.
        item['worldwide_total'] = worldwide  # Store worldwide total.

        # Extract distributor name
        item['domestic_distributor'] = self._extract_domestic_distributor(response, page_text)  # Extract distributor.

        self.logger.info(  # Log the extracted data for debugging.
            f"  Budget: {item['production_budget']}, "
            f"Domestic: {item['domestic_total']}, "
            f"International: {item['international_total']}, "
            f"Worldwide: {item['worldwide_total']}, "
            f"Distributor: {item['domestic_distributor']}"
        )

        yield item  # Send the populated item to the pipeline.

    def _parse_money(self, text):  # Helper method to convert money strings to integers.
        """Helper: Parse money string like '$25,000,000' to integer 25000000."""
        if not text:  # If text is None or empty, return None.
            return None
        # Regex substitution: remove anything that is NOT a digit (0-9)
        cleaned = re.sub(r'[^\d]', '', str(text))  # Remove all non-numeric characters (like $,).
        if cleaned:  # If we have any digits left...
            return int(cleaned)  # Convert to integer.
        return None  # Otherwise return None.

    def _extract_budget(self, response, page_text):  # Helper method to find the budget.
        """Extract production budget."""
        # Method 1: Use XPath to find the label 'Budget' and get the text following it
        # XPath is a language for selecting nodes in XML/HTML documents
        budget_spans = response.xpath(  # Use XPath to find spans containing "Budget".
            '//span[contains(text(), "Budget")]/following-sibling::span/text()'
        ).getall()  # Get all matching text nodes.
        for span in budget_spans:  # Loop through matches.
            val = self._parse_money(span)  # Parse the money string.
            if val and val > 100000:  # Sanity check: budget > $100k.
                return val  # Return the valid budget.

        # Method 2: Regex fallback. Look for "Budget" followed by optional $ and numbers
        match = re.search(r'Budget\s*\$?([\d,]+)', page_text, re.IGNORECASE)  # Search for pattern in text.
        if match:  # If pattern matches...
            val = self._parse_money(match.group(1))  # Parse the captured group.
            if val and val > 100000:  # Sanity check.
                return val  # Return result.

        return None  # Return None if nothing found.

    def _extract_domestic_opening(self, response, page_text):  # Helper method for opening weekend.
        """Extract domestic opening weekend gross."""
        # Method 1: XPath Strategy
        opening_spans = response.xpath(  # Use XPath to find "Opening".
            '//span[contains(text(), "Opening")]/following-sibling::span/text()'
        ).getall()
        for span in opening_spans:  # Loop through results.
            val = self._parse_money(span)  # Parse money.
            if val and val > 1000:  # Sanity check.
                return val  # Return result.

        # Method 2: Regex Strategy ("Domestic Opening $10,000")
        match = re.search(r'Domestic Opening\s*\$?([\d,]+)', page_text, re.IGNORECASE)  # Regex search.
        if match:
            return self._parse_money(match.group(1))

        # Method 3: Regex Strategy ("Opening $10,000")
        match = re.search(r'Opening\s+\$?([\d,]+)', page_text)  # Broader regex search.
        if match:
            return self._parse_money(match.group(1))

        return None

    def _extract_regional_totals(self, response, page_text):  # Helper method for totals.
        """Extract domestic, international, and worldwide totals."""
        domestic = None  # Initialize domestic.
        international = None  # Initialize international.
        worldwide = None  # Initialize worldwide.

        # Method 1: Regex Search for patterns "Domestic (XX%) <Amount>"
        # (?:...) is a non-capturing group.
        # [\d,]+ captures numbers with commas.
        domestic_match = re.search(
            r'Domestic\s*(?:\([^)]+\))?\s*\$?([\d,]+)',  # Pattern for Domestic total.
            page_text,
            re.IGNORECASE
        )
        if domestic_match:
            domestic = self._parse_money(domestic_match.group(1))  # Parse domestic.

        international_match = re.search(
            r'International\s*(?:\([^)]+\))?\s*\$?([\d,]+)',  # Pattern for International total.
            page_text,
            re.IGNORECASE
        )
        if international_match:
            international = self._parse_money(international_match.group(1))  # Parse international.

        worldwide_match = re.search(
            r'Worldwide\s*\$?([\d,]+)',  # Pattern for Worldwide total.
            page_text,
            re.IGNORECASE
        )
        if worldwide_match:
            worldwide = self._parse_money(worldwide_match.group(1))  # Parse worldwide.

        # Method 2: Check standard CSS classes often used for money
        money_spans = response.css('span.money::text, span[class*="mojo-"]::text').getall()  # CSS selector for money spans.
        for span in money_spans:  # Loop through spans.
            val = self._parse_money(span)  # Parse value.
            # Logic: If this value is bigger than current worldwide, update worldwide
            # (Assuming worldwide is usually the largest number on page)
            if val and val > (worldwide if worldwide else 0):
                worldwide = val  # Update max value found.

        # Method 3: Parse from HTML table if present
        table_rows = response.css('table tr')  # Select table rows.
        for row in table_rows:  # Loop through rows.
            cells = row.css('td::text, td span::text').getall()  # Get cell text.
            row_text = ' '.join(cells)  # Join cell text.

            # Check rows for keywords along with numbers
            if 'Domestic' in row_text and not domestic:  # If row contains "Domestic"...
                for cell in cells:  # Check cells for money.
                    val = self._parse_money(cell)
                    if val:
                        domestic = val  # Found it.
                        break

            if 'International' in row_text and not international:  # If row contains "International"...
                for cell in cells:
                    val = self._parse_money(cell)
                    if val:
                        international = val  # Found it.
                        break

            if 'Worldwide' in row_text and not worldwide:  # If row contains "Worldwide"...
                for cell in cells:
                    val = self._parse_money(cell)
                    if val:
                        worldwide = val  # Found it.
                        break

        return domestic, international, worldwide  # Return the tuple of values.

    def _extract_domestic_distributor(self, response, page_text):  # Helper method for distributor.
        """Extract domestic distributor name."""
        # Method 1: XPath - Find "Distributor" label, get next sibling span
        distributor = response.xpath(
            '//span[contains(text(), "Distributor")]/following-sibling::span/text()'
        ).get()  # Get distributor text.
        if distributor:
            return distributor.strip()  # Return stripped text.

        # Method 2: Regex - Look for "Domestic Distributor <Name> See full..."
        match = re.search(r'Domestic Distributor\s+([A-Za-z0-9 .,&]+?)(?:\s+See full command|(?=\s+See full)|$)', page_text, re.IGNORECASE)  # Regex search.
        if match:
            return match.group(1).strip()  # Return captured name.
            
        return None  # Return None.

    def handle_error(self, failure):  # Error handler callback.
        """Handle request errors (404, timeout, etc.)."""
        request = failure.request  # Get the failed request object.
        # Retrieve the title we stored in meta to log a helpful error message
        title = request.meta.get('title', 'Unknown')  # Get title from meta.
        self.logger.warning(f"Failed to fetch Box Office data for: {title} - {failure.value}")  # Log warning.
