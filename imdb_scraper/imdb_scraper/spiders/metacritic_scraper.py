# Author: Jeffrey | Online Data Mining - Amsterdam UAS
# Metacritic Scraper - Simplified version that only scrapes reviews.
#
# This spider yields:
# - MetacriticMovieItem (basic movie info + metascore)
# - MetacriticCriticReviewItem (critic reviews)
# - MetacriticUserReviewItem (user reviews)
#
# Reads movies from IMDB SQLite database and matches them on Metacritic.

import re  # This imports the 're' module, which stands for regular expressions. Regular expressions are special patterns used to search for and manipulate text. For example, we use 're' to clean movie titles by removing special characters and to find scores or dates in the scraped web page text.
import sqlite3  # This imports the 'sqlite3' module, which lets us connect to and query SQLite databases. SQLite is a lightweight database that stores data in a single file. We use it to read movie information (like titles and years) from our local IMDB database that we created earlier.
import zlib  # This imports the 'zlib' module, which provides tools for compressing data. We use its 'adler32' function to create unique, stable IDs for reviews by hashing strings into numbers. This helps us avoid duplicates and track reviews consistently.
from datetime import datetime  # This imports the 'datetime' class from the 'datetime' module. 'datetime' helps us work with dates and times in Python. We use it to record the exact time when we scraped each piece of data, so we know when the information was collected.
from pathlib import Path  # This imports the 'Path' class from the 'pathlib' module. 'Path' makes it easy to handle file and folder paths in a way that works on different computers (like Mac, Windows, or Linux). We use it to find the location of our database file and other output files.
from typing import Any, Dict, List, Optional  # This imports type hints from the 'typing' module. Type hints are like labels that tell Python what kind of data a variable should hold, making our code easier to understand and less prone to bugs. 'Any' means any type of data, 'Dict' is for dictionaries (key-value pairs), 'List' is for lists (ordered collections), and 'Optional' means the data can be that type or nothing (None).

import scrapy  # This imports the Scrapy framework, which is a powerful library for web scraping in Python. Scrapy provides tools to download web pages, follow links, extract data, and handle things like delays and retries automatically, making it much easier than writing everything from scratch.
from scrapy import Selector  # This imports the Selector class from Scrapy. Selector is used to navigate and extract data from HTML pages using CSS selectors or XPath. For example, we can use it to find specific elements like movie titles or scores on a web page.

from imdb_scraper.items import (  # This imports custom data classes (called Items) from our project's items.py file. Items are like blueprints that define the structure of the data we want to scrape, such as movie details or review information. They help organize the scraped data before saving it to files or databases.
    MetacriticMovieItem,  # This Item holds basic movie information from Metacritic, like the movie's title, Metascore, and user score.
    MetacriticCriticReviewItem,  # This Item stores data about critic reviews, including the reviewer's name, publication, score, and review text.
    MetacriticUserReviewItem,  # This Item contains user review data, such as the username, rating, and full review text.
)


# Syntax Explanation: class inheritance
# Class MetacriticSpider inherits from scrapy.Spider, giving it scraping capabilities.
class MetacriticSpider(scrapy.Spider):  # This defines our custom spider class that inherits from Scrapy's base Spider class. Inheritance means it gets all the built-in scraping features from scrapy.Spider, and we add our own methods for Metacritic-specific scraping.
    name = "metacritic_scraper"  # This gives our spider a unique name. Scrapy uses this name to identify and run the spider from the command line.
    allowed_domains = ["metacritic.com"]  # This restricts the spider to only scrape pages from metacritic.com, preventing it from accidentally crawling other websites.

    custom_settings = {  # These are special settings that override Scrapy's default behavior for this spider. They help us be respectful to the website and avoid getting blocked.
        "USER_AGENT": (  # The User-Agent is like an ID that tells the website what kind of browser we're pretending to be. This makes our requests look like they're from a real web browser.
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "DOWNLOAD_DELAY": 2,  # This adds a 2-second delay between requests to the same website. It's like waiting politely before asking for the next page, so we don't overwhelm the server.
        "CONCURRENT_REQUESTS": 2,  # This limits the spider to making only 2 requests at the same time. It helps prevent overloading the website and getting our IP address blocked.
    }

    # Syntax Explanation: *args and **kwargs
    # *args collects extra positional arguments into a tuple.
    # **kwargs collects extra keyword arguments into a dictionary.
    # This allows passing arbitrary arguments from the command line (e.g., -a max_movies=10)
    def __init__(  # This is the constructor method that runs when we create a new spider instance. It sets up the spider with initial values and configurations.
        self,  # 'self' refers to the spider object itself, allowing us to store data and methods on it.
        max_movies: int = 20000,  # This parameter sets the maximum number of movies to scrape from the IMDB database. The default is 20,000, but we can change it when running the spider to limit how much data we collect.
        max_review_pages: int = 1,  # This controls how many pages of reviews to scrape per movie. Default is 1 page, but we can increase it to get more reviews if needed.
        max_reviews_per_movie: int = 10,  # This limits the number of reviews to collect per movie. Default is 10, helping us control the amount of data and avoid scraping too much.
        imdb_db_path: str = "",  # This allows us to specify a custom path to the IMDB database file. If left empty, the spider will look for it in default locations.
        *args,  # *args collects any extra positional arguments (like numbers or strings) passed to the spider, in case we need them later.
        **kwargs,  # **kwargs collects any extra keyword arguments (like key=value pairs) passed to the spider, giving us flexibility for custom options.
    ):
        super().__init__(*args, **kwargs)  # This calls the parent class's __init__ method to set up the basic spider functionality, passing along any extra arguments.
        self.max_movies = int(max_movies)  # We store the max_movies value as an instance variable so other methods in the spider can use it.
        self.max_review_pages = int(max_review_pages)  # Store the max_review_pages setting for use in review scraping.
        self.max_reviews_per_movie = int(max_reviews_per_movie)  # Store the limit for reviews per movie.
        self.movies_scraped = 0  # This counter tracks how many movies we've successfully scraped so far. It starts at 0 and increases as we process each movie.
        self.seen_slugs = set()  # A set to keep track of movie URL slugs we've already seen. Sets are like lists but only store unique items, helping us avoid duplicate requests.

        # Find IMDB database
        if imdb_db_path:  # If a custom database path was provided, use it directly.
            self.imdb_db_path = Path(imdb_db_path)  # Convert the string path to a Path object for easier file handling.
        else:  # If no custom path, look for the database in common locations.
            # Syntax Explanation: Path / "string"
            # The / operator is overloaded in pathlib to join paths across operating systems.
            possible_paths = [  # A list of possible locations where the IMDB database might be stored.
                Path(__file__).parent.parent.parent / "output" / "movies.db",  # Look in the output folder relative to the spider's location.
                Path("output/movies.db"),  # Look in the output folder from the current working directory.
            ]
            self.imdb_db_path = None  # Start with no path found.
            for p in possible_paths:  # Check each possible path in order.
                if p.exists():  # If the file exists at this path, use it.
                    self.imdb_db_path = p
                    break  # Stop looking once we find the database.

    def start_requests(self):  # This method is called when the spider starts. It generates the initial requests to scrape, like the first URLs to visit.
        # Read movies from IMDB database and request Metacritic pages.
        
        if not self.imdb_db_path or not self.imdb_db_path.exists():  # Check if we found a valid database file.
            self.logger.error("IMDB database not found. Run movie_scraper first.")  # Log an error message if the database is missing.
            return  # Stop the spider if we can't find the data source.

        self.logger.info(f"Reading movies from: {self.imdb_db_path}")  # Log which database file we're using.

        # Syntax Explanation: sqlite3 connection
        # Connects to the SQLite database file to query data.
        conn = sqlite3.connect(self.imdb_db_path)  # Open a connection to the SQLite database file.
        cursor = conn.cursor()  # Create a cursor object to execute SQL queries on the database.
        # Execute a SQL query to select movie data.
        # The query gets movie ID, title, and year, sorted by ID, limited to max_movies.
        cursor.execute("""
            SELECT movie_id, title, year FROM movie ORDER BY movie_id LIMIT ?
        """, (self.max_movies,))
        movies = cursor.fetchall()  # Fetch all the results from the query as a list of tuples.
        # Always close database connections to release file locks.
        conn.close()  # Close the database connection to free up resources and allow other programs to access the file.

        self.logger.info(f"Found {len(movies)} movies to fetch from Metacritic")  # Log how many movies we found in the database.

        # Syntax Explanation: Unpacking tuple
        # 'movies' is a list of tuples. We unpack each tuple into individual variables.
        for imdb_movie_id, title, year in movies:  # Loop through each movie tuple, unpacking it into separate variables.
            slug = self._title_to_slug(title)  # Convert the movie title to a URL-friendly slug for Metacritic.
            if not slug or slug in self.seen_slugs:  # Skip if the slug is invalid or we've already processed this movie.
                continue  # Go to the next movie without processing this one.
            self.seen_slugs.add(slug)  # Add the slug to our set of seen movies to avoid duplicates.

            movie_url = f"https://www.metacritic.com/movie/{slug}/"  # Build the full URL for the movie's Metacritic page.
            
            # Syntax Explanation: yield keyword
            # 'yield' turns this method into a generator. It pauses execution and produces
            # a value (Request object) to Scrapy's engine, then resumes later.
            yield scrapy.Request(  # Create and send a request to scrape the movie page.
                url=movie_url,  # The URL to request.
                callback=self.parse_movie,  # The method to call when the page is downloaded (parse_movie).
                # Syntax Explanation: meta dictionary
                # 'meta' allows passing data (movie_id, title, year) from this request
                # to the callback function (parse_movie) so it's available there.
                meta={"imdb_movie_id": imdb_movie_id, "title": title, "year": year},  # Pass movie data to the callback method.
                errback=self._handle_error,  # If the request fails, call this error-handling method.
            )
            
            # Syntax Explanation: yield keyword
            # 'yield' turns this method into a generator. It pauses execution and produces
            # a value (Request object) to Scrapy's engine, then resumes later.
            yield scrapy.Request(  # This creates a request for Scrapy to download the movie's Metacritic page. Scrapy will handle the download and then call the callback method.
                url=movie_url,  # The URL of the movie page to scrape.
                callback=self.parse_movie,  # The method to call when the page is downloaded and ready to parse.
                # Syntax Explanation: meta dictionary
                # 'meta' allows passing data (movie_id, title, year) from this request
                # to the callback function (parse_movie) so it's available there.
                meta={"imdb_movie_id": imdb_movie_id, "title": title, "year": year},  # Pass the movie's ID, title, and year to the parse_movie method so it knows which movie this is.
                errback=self._handle_error,  # If the request fails (like a 404 error), call this method to handle the error.
            )

    def _title_to_slug(self, title: str) -> Optional[str]:  # This helper method takes a movie title string and converts it into a clean, URL-friendly slug that matches Metacritic's format. For example, "The Dark Knight" becomes "the-dark-knight".
        # Convert movie title to Metacritic URL slug.
        if not title:  # Check if the title is empty or None. If so, we can't create a valid slug, so return None to indicate failure.
            return None
        slug = title.lower()  # Convert the entire title to lowercase, as Metacritic URLs use lowercase letters.
        # Syntax Explanation: re.sub
        # Replaces patterns matching the regex with ' '.
        # [^\w\s-] matches any character that is NOT a word char, whitespace, or hyphen.
        slug = re.sub(r'[^\w\s-]', ' ', slug)  # Use regular expressions to replace any special characters (like punctuation marks !@#) with spaces, keeping only letters, numbers, spaces, and hyphens.
        slug = re.sub(r'\s+', ' ', slug).strip()  # Replace any sequence of multiple spaces with a single space, and remove spaces from the beginning and end.
        slug = slug.replace(' ', '-')  # Replace all spaces with hyphens to create the slug format that URLs use.
        slug = re.sub(r'-+', '-', slug).strip('-')  # Replace multiple consecutive hyphens with a single hyphen, and remove hyphens from the start and end.
        return slug if slug else None  # Return the cleaned slug if it's not empty, otherwise return None.

    def _handle_error(self, failure):  # This method is called when a request to Metacritic fails, like when a movie page doesn't exist (404 error). It logs a warning so we know which movie couldn't be found.
        # Handle 404 errors for movies not on Metacritic.
        title = failure.request.meta.get("title", "Unknown")  # Get the movie title from the request's metadata, or use "Unknown" if not available.
        self.logger.warning(f"Failed to fetch Metacritic for: {title}")  # Log a warning message with the movie title so we can see which movies were skipped.

    def parse_movie(self, response):  # This method is called automatically by Scrapy when a movie page is downloaded. It extracts key information like scores and review counts, then creates requests for the review pages.
        # Parse movie page and queue review pages.
        movie_id = response.meta.get("imdb_movie_id")  # Retrieve the movie ID that was passed from the start_requests method.
        slug = self._extract_slug(response.url)  # Extract the movie's slug from the current URL to use for building review page URLs.

        if not slug:  # If we can't extract a slug from the URL, something went wrong, so stop processing this page.
            return

        self.movies_scraped += 1  # Increase the counter of how many movies we've successfully started processing.
        self.logger.info(f"[{self.movies_scraped}] Parsing: {response.meta.get('title')}")  # Log a message showing which movie we're currently parsing and how many we've done so far.

        # Extract basic info
        page_text = self._page_text(response)  # Get all the visible text from the web page as a single string.
        metascore = self._extract_metascore(response, page_text)  # Extract the critic score (Metascore) from the page.
        user_score = self._extract_userscore(page_text)  # Extract the average user score from the page.

        # Yield movie item with scores to the pipeline
        yield MetacriticMovieItem(  # Create a data item containing the movie's information and send it to the pipeline for saving to files or databases.
            movie_id=movie_id,  # The unique ID of the movie from our IMDB database.
            metacritic_url=response.url,  # The full URL of the Metacritic page we scraped.
            metacritic_slug=slug,  # The clean slug extracted from the URL.
            # Syntax Explanation: response.css(...).get()
            # Uses CSS selector to find the first h1 element and gets its text content.
            title_on_site=response.css("h1::text").get(),  # Use a CSS selector to find the main title (in the h1 tag) on the page.
            metascore=metascore,  # The extracted Metascore (critic rating).
            user_score=user_score,  # The extracted user score (average rating from users).
            critic_review_count=self._extract_review_count(page_text, "critic"),  # Count how many critic reviews are mentioned on the page.
            user_rating_count=self._extract_review_count(page_text, "user"),  # Count how many user ratings are mentioned on the page.
            scraped_at=datetime.now().isoformat(),  # Record the exact date and time we scraped this data, formatted as a string.
        )

        # Queue critic review pages
        # Syntax Explanation: range(start, stop)
        # Iterates from 0 up to (but not including) max_review_pages.
        for p in range(self.max_review_pages):  # Loop from 0 to max_review_pages-1 to create requests for multiple pages of critic reviews.
            url = f"https://www.metacritic.com/movie/{slug}/critic-reviews/"  # Build the base URL for critic reviews using the movie slug.
            if p > 0:  # For the first page (p=0), no page parameter is needed. For later pages, add ?page=1, ?page=2, etc.
                url += f"?page={p}"  # Append the page number to the URL.
            yield scrapy.Request(  # Create a request to download the critic reviews page.
                url=url,  # The URL of the reviews page.
                callback=self.parse_critic_reviews,  # Call this method when the page is downloaded.
                meta={"movie_id": movie_id},  # Pass the movie ID to the parsing method.
            )

        # Queue user review pages
        for p in range(self.max_review_pages):  # Same as above, but for user review pages.
            url = f"https://www.metacritic.com/movie/{slug}/user-reviews/"  # Build the base URL for user reviews.
            if p > 0:  # Add page parameter for pages after the first.
                url += f"?page={p}"
            yield scrapy.Request(  # Create request for user reviews page.
                url=url,
                callback=self.parse_user_reviews,  # Call the user reviews parsing method.
                meta={"movie_id": movie_id},  # Pass movie ID.
            )

    def parse_critic_reviews(self, response):  # This method processes a page of critic reviews. It extracts individual reviews and yields them as data items.
        # Parse critic reviews page.
        movie_id = response.meta.get("movie_id")  # Get the movie ID from the request metadata.
        tokens = self._tokens(response)  # Convert the page's text into a list of tokens (words/phrases) for easier parsing.
        reviews = self._parse_critic_reviews_from_tokens(tokens)  # Parse the tokens to extract structured review data.

        # Syntax Explanation: enumerate(iterable, start=1)
        # Returns pairs of (index, item) starting the index at 1.
        # reviews[:limit] slices the list to take only the first 'limit' items.
        for idx, r in enumerate(reviews[:self.max_reviews_per_movie], start=1):  # Loop through the extracted reviews, limiting to max_reviews_per_movie, and number them starting from 1.
            yield MetacriticCriticReviewItem(  # Create and yield a critic review item for each review.
                critic_review_id=self._stable_id(f"critic:{movie_id}:{idx}:{response.url}"),  # Generate a unique ID for this review using a hash.
                movie_id=movie_id,  # The ID of the movie this review is for.
                publication_name=r.get("publication_name"),  # The name of the publication (like "The New York Times") that published the review.
                critic_name=r.get("critic_name"),  # The name of the critic who wrote the review.
                score=r.get("score"),  # The numerical score given in the review (0-100).
                review_date=r.get("review_date"),  # The date the review was published.
                excerpt=r.get("excerpt"),  # A short excerpt or summary of the review text.
                scraped_at=datetime.now().isoformat(),  # The date and time we scraped this review.
            )

    def parse_user_reviews(self, response):  # This method processes a page of user reviews, similar to the critic reviews method but for user-generated content.
        # Parse user reviews page.
        movie_id = response.meta.get("movie_id")  # Get movie ID from metadata.
        tokens = self._tokens(response)  # Get tokens from the page.
        reviews = self._parse_user_reviews_from_tokens(tokens)  # Parse tokens into user review data.

        for idx, r in enumerate(reviews[:self.max_reviews_per_movie], start=1):  # Loop through user reviews with limit and numbering.
            yield MetacriticUserReviewItem(  # Yield a user review item.
                user_review_id=self._stable_id(f"user:{movie_id}:{idx}:{response.url}"),  # Unique ID for the user review.
                movie_id=movie_id,  # Movie ID.
                username=r.get("username"),  # The username of the person who wrote the review.
                score=r.get("score"),  # The score given by the user (0-10).
                review_date=r.get("review_date"),  # Date the review was posted.
                review_text=r.get("review_text"),  # The full text of the user's review.
                scraped_at=datetime.now().isoformat(),  # Scraping timestamp.
            )

    # -------------------------
    # Helper methods
    # -------------------------

    def _extract_slug(self, url: str) -> Optional[str]:  # This helper extracts the movie slug from a Metacritic URL. For example, from "/movie/the-dark-knight/" it gets "the-dark-knight".
        # Syntax Explanation: regex grouping
        # ([^/]+) captures one or more characters that are NOT a slash.
        # m.group(1) gives us the content of that first capture group.
        m = re.search(r"/movie/([^/]+)/?", url)  # Use regex to find the part between "/movie/" and the next "/" in the URL.
        return m.group(1) if m else None  # Return the captured slug if found, otherwise None.

    def _stable_id(self, text: str) -> int:  # This creates a unique, consistent ID number from any text string using a hash function. This ensures the same text always gets the same ID.
        # Generates a stable integer ID from a string using Adler32 hash.
        # 10_000... is added to avoid small number collisions or confusion with auto-increment IDs.
        return int(10_000_000_000 + zlib.adler32(text.encode("utf-8")))  # Hash the text with Adler32, add a large number to make it a big unique ID.

    def _page_text(self, response) -> str:  # This extracts all the visible text from a web page response and joins it into a single string for searching.
        # Syntax Explanation: Simple List Comprehension
        # [p.strip() for p in parts if p.strip()] creates a new list by stripping whitespace
        # from each 'p' in 'parts', but only if p.strip() is not empty.
        parts = response.css("body *::text").getall() or []  # Get all text content from elements inside the body tag.
        return " ".join(p.strip() for p in parts if p.strip())  # Strip whitespace from each text piece, filter out empty ones, and join with spaces.

    def _tokens(self, response) -> List[str]:  # This converts page text into a list of cleaned tokens (words/phrases) for parsing reviews.
        parts = response.css("body *::text").getall() or []  # Get all text parts.
        return [re.sub(r"\s+", " ", p).strip() for p in parts if p.strip()]  # Clean each part by normalizing spaces and stripping.

    def _extract_metascore(self, response, text: str) -> Optional[int]:  # This tries to find and extract the Metascore (0-100) from the page, first using CSS selectors, then regex as backup.
        # Try CSS selector first
        score = response.css('.c-productScoreInfo_scoreNumber span::text').get()  # Look for the score using a specific CSS class.
        if score and score.strip().isdigit():  # If found and it's a number, convert to int.
            return int(score.strip())
        # Regex fallback
        m = re.search(r"\bMetascore\b\s*(\d{1,3})(?!\s*reviews)", text, re.IGNORECASE)  # Search for "Metascore" followed by 1-3 digits.
        if m and 0 <= int(m.group(1)) <= 100:  # If found and in valid range, return it.
            return int(m.group(1))
        return None  # If not found, return None.

    def _extract_userscore(self, text: str) -> Optional[float]:  # This extracts the user score (0.0-10.0) from the page text using regex.
        # Extracts 6.4 from "User Score ... 6.4"
        m = re.search(r"\bUser Score\b.*?\b(\d{1,2}\.?\d?)\b", text, re.IGNORECASE)  # Search for "User Score" followed by a number like 6.4.
        if m:  # If found,
            val = float(m.group(1))  # Convert to float.
            if 0.0 <= val <= 10.0:  # Check if in valid range.
                return val
        return None  # Otherwise, return None.

    def _extract_review_count(self, text: str, kind: str) -> Optional[int]:  # This extracts the count of reviews or ratings from the text, either for critics or users.
        if kind == "critic":  # If looking for critic reviews,
            m = re.search(r"\bBased on\s+([\d,]+)\s+Critic Reviews\b", text, re.IGNORECASE)  # Search for "Based on X Critic Reviews".
        else:  # For user ratings,
            m = re.search(r"\bBased on\s+([\d,]+)\s+User Ratings\b", text, re.IGNORECASE)  # Search for "Based on X User Ratings".
        return int(m.group(1).replace(",", "")) if m else None  # If found, remove commas and convert to int.

    def _parse_critic_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:  # This method parses a list of text tokens to extract critic review data, handling the page's layout.
        # Parsing logic using token stream (robust against HTML layout changes)
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")  # Regex to match dates like "Jan 15, 2023".
        score_re = re.compile(r"^\d{1,3}$")  # Regex to match scores 0-999.
        out = []  # List to store extracted reviews.
        i = 0  # Index to track position in tokens.

        while i < len(tokens):  # Loop through all tokens.
            if not date_re.match(tokens[i]):  # If current token is not a date, skip to next.
                i += 1
                continue

            review_date = tokens[i]  # Save the date.
            i += 1  # Move to next token.

            # Find score
            score = None  # Initialize score as None.
            for j in range(i, min(i + 6, len(tokens))):  # Look for score in next few tokens.
                if score_re.match(tokens[j]):  # If token matches score pattern,
                    val = int(tokens[j])  # Convert to int.
                    if 0 <= val <= 100:  # Check valid range.
                        score = val  # Save score.
                        i = j + 1  # Move index past the score.
                        break

            if score is None:  # If no score found, skip this review.
                continue

            # Find publication name
            pub_name = None  # Initialize publication name.
            for j in range(i, min(i + 4, len(tokens))):  # Look for publication in next few tokens.
                if not score_re.match(tokens[j]) and not date_re.match(tokens[j]):  # If not score or date,
                    if tokens[j].lower() not in ("read more", "report"):  # And not unwanted words,
                        pub_name = tokens[j]  # Save as publication name.
                        i = j + 1  # Move past it.
                        break

            # Collect excerpt
            excerpt_parts = []  # List to collect excerpt pieces.
            while i < len(tokens):  # Continue from current position.
                if tokens[i].lower() == "read more" or date_re.match(tokens[i]):  # Stop at "read more" or next date.
                    break
                excerpt_parts.append(tokens[i])  # Add token to excerpt.
                i += 1  # Move to next.

            out.append({  # Add the extracted review to the list.
                "review_date": review_date,
                "score": score,
                "publication_name": pub_name,
                "excerpt": " ".join(excerpt_parts).strip() or None,  # Join excerpt parts.
                "critic_name": None,  # Not extracted in this version.
            })

        return out  # Return the list of reviews.

    def _parse_user_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:  # Similar to critic parsing, but for user reviews with different structure.
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")  # Date regex.
        score_re = re.compile(r"^\d{1,2}$")  # Score regex for 0-99.
        out = []  # Reviews list.
        i = 0  # Index.

        while i < len(tokens):  # Loop through tokens.
            if not date_re.match(tokens[i]):  # Skip if not date.
                i += 1
                continue

            review_date = tokens[i]  # Save date.
            i += 1

            if i >= len(tokens) or not score_re.match(tokens[i]):  # Check for score after date.
                continue

            score = int(tokens[i])  # Convert score to int.
            if not (0 <= score <= 10):  # Check range 0-10.
                continue
            i += 1  # Move past score.

            username = tokens[i] if i < len(tokens) else None  # Get username.
            i += 1  # Move past username.

            # Collect review text
            review_parts = []  # List for review text.
            while i < len(tokens):  # Collect until stop words or next date.
                if tokens[i].lower() in ("read more", "report") or date_re.match(tokens[i]):
                    break
                review_parts.append(tokens[i])  # Add to review.
                i += 1

            out.append({  # Add to reviews list.
                "review_date": review_date,
                "score": score,
                "username": username,
                "review_text": " ".join(review_parts).strip() or None,  # Join review parts.
            })

        return out  # Return user reviews.
