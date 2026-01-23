import json
import re
from datetime import datetime
import scrapy
from scrapy_playwright.page import PageMethod

print("### RT SPIDER LOADED V12 (SIMPLIFIED) ###")


class RottenTomatoesSpider(scrapy.Spider):
    name = "rottentomatoes_scraper"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "Mozilla/5.0",
        "HTTPERROR_ALLOW_ALL": True,
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 4,
    }

    def start_requests(self):
        # Use the popular movies page - easier to scrape
        yield scrapy.Request(
            "https://www.rottentomatoes.com/browse/movies_at_home/sort:popular",
            callback=self.parse_list,
            dont_filter=True,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                ],
            },
        )

    async def parse_list(self, response):
        """Parse the listing page by scrolling to load more content"""
        page = response.meta["playwright_page"]
        
        # Dismiss cookie popup
        try:
            accept_button = await page.query_selector("button#onetrust-accept-btn-handler")
            if accept_button:
                await accept_button.click()
                await page.wait_for_timeout(1000)
                print("✓ Cookie popup dismissed!")
        except:
            pass
        
        # Scroll to load more movies (10 scrolls = ~100-150 movies)
        print("Starting to scroll and load movies...")
        for i in range(10):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            
            # Count current movies
            current_links = await page.query_selector_all("a[href^='/m/']")
            print(f"   Scroll {i+1}/10 - Found {len(current_links)} movies so far")
        
        # Get the final page content
        content = await page.content()
        await page.close()
        
        # Create new response with loaded content
        from scrapy.http import HtmlResponse
        loaded_response = HtmlResponse(
            url=response.url,
            body=content,
            encoding='utf-8'
        )
        
        # Extract all movie links
        hrefs = loaded_response.css('a[href^="/m/"]::attr(href)').getall()
        hrefs = list(dict.fromkeys(hrefs))  # Remove duplicates
        
        print(f"\n Movies FOUND {len(hrefs)} TOTAL MOVIE LINKS\n")

        # Scrape each movie (limit to 200 for speed)
        for idx, href in enumerate(hrefs[:200], 1):
            full_url = response.urljoin(href)
            print(f"   [{idx}/{len(hrefs[:200])}] Queueing: {full_url}")
            yield scrapy.Request(
                full_url,
                callback=self.parse_movie,
                meta={"playwright": True, "rt_slug": href.split("/m/")[-1].strip("/")}
            )

    def parse_movie(self, response):
        """Parse movie detail page"""
        rt_slug = response.meta["rt_slug"]
        
        # Extract basic movie data
        movie_data = {
            "movie_id": self.generate_id(rt_slug),
            "title": response.css('h1[slot="titleIntro"]::text').get() or 
                     response.css('h1::text').get(),
            "year": self.extract_year(response),
            "release_date": None,
            "runtime_minutes": self.extract_runtime(response),
            "mpaa_rating": response.css('[data-qa="mpaa-rating"]::text').get(),
            "genres": self.extract_genres(response),
            "summary": response.css('[data-qa="movie-info-synopsis"]::text').get(),
            "production_company": None,
            "awards": None,
            "music_composer": None,
            "user_score": self.extract_score(response, "audiencescore"),
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract RT-specific scores
        rt_data = {
            "rt_id": self.generate_id(rt_slug),
            "movie_id": self.generate_id(rt_slug),
            "tomatometer_score": self.extract_score(response, "tomatometer"),
            "audience_score": self.extract_score(response, "audiencescore"),
            "certified_fresh": self.is_certified_fresh(response),
            "scraped_at": datetime.now().isoformat(),
        }

        yield {
            "type": "movie",
            "data": movie_data,
            "rt_data": rt_data,
        }

        # Follow to reviews pages
        reviews_url = f"{response.url.rstrip('/')}/reviews"
        yield scrapy.Request(
            reviews_url,
            callback=self.parse_critic_reviews,
            meta={"playwright": True, "movie_id": self.generate_id(rt_slug)}
        )

        user_reviews_url = f"{response.url.rstrip('/')}/reviews?type=user"
        yield scrapy.Request(
            user_reviews_url,
            callback=self.parse_user_reviews,
            meta={"playwright": True, "movie_id": self.generate_id(rt_slug)}
        )

    def parse_critic_reviews(self, response):
        """Parse critic reviews page"""
        movie_id = response.meta["movie_id"]
        
        # Try to find reviews in HTML
        for review in response.css('[data-qa="review-row"]'):
            publication = review.css('[data-qa="review-publication"]::text').get()
            critic = review.css('[data-qa="review-critic-name"]::text').get()
            text = review.css('[data-qa="review-text"]::text').get()
            
            if publication or critic or text:
                review_id = self.generate_id(f"{movie_id}{critic}{publication}")
                
                yield {
                    "type": "critic_review",
                    "data": {
                        "critic_review_id": review_id,
                        "movie_id": movie_id,
                        "publication_name": publication,
                        "critic_name": critic,
                        "score": self.extract_review_sentiment(review),
                        "review_date": review.css('[data-qa="review-date"]::text').get(),
                        "review_text": text,
                        "scraped_at": datetime.now().isoformat(),
                    }
                }

    def parse_user_reviews(self, response):
        """Parse user reviews page"""
        movie_id = response.meta["movie_id"]

        for review in response.css('[data-qa="user-review"]'):
            user = review.css('[data-qa="user-name"]::text').get()
            text = review.css('[data-qa="review-text"]::text').get()
            
            if user or text:
                review_id = self.generate_id(f"{movie_id}{user}{text[:50] if text else ''}")
                
                yield {
                    "type": "user_review",
                    "data": {
                        "user_review_id": review_id,
                        "movie_id": movie_id,
                        "user_name": user,
                        "score": self.extract_user_rating(review),
                        "review_text": text,
                        "review_date": review.css('[data-qa="review-date"]::text').get(),
                        "scraped_at": datetime.now().isoformat(),
                    }
                }

    # Helper methods
    def generate_id(self, text):
        """Generate numeric ID from text"""
        import hashlib
        if not text:
            return None
        hash_obj = hashlib.md5(str(text).encode())
        return int(hash_obj.hexdigest()[:8], 16) % 2147483647

    def extract_score(self, response, score_type):
        """Extract tomatometer or audience score"""
        score = response.css(f'[data-qa="{score_type}"]::text').get()
        if score:
            score = re.sub(r'[^\d.]', '', score)
            try:
                return float(score)
            except ValueError:
                pass
        return None

    def is_certified_fresh(self, response):
        """Check if movie is certified fresh"""
        return bool(response.css('[data-qa="certified-fresh"]'))

    def extract_year(self, response):
        """Extract release year"""
        year_text = response.css('[data-qa="movie-info-item-year"]::text').get()
        if year_text:
            match = re.search(r'(\d{4})', year_text)
            if match:
                return int(match.group(1))
        return None

    def extract_runtime(self, response):
        """Extract runtime in minutes"""
        runtime = response.css('[data-qa="movie-info-runtime"]::text').get()
        if runtime:
            match = re.search(r'(\d+)h\s*(\d+)m|(\d+)m', runtime)
            if match:
                if match.group(1):  # Hours and minutes
                    return int(match.group(1)) * 60 + int(match.group(2))
                else:  # Minutes only
                    return int(match.group(3))
        return None

    def extract_genres(self, response):
        """Extract genres as comma-separated string"""
        genres = response.css('[data-qa="movie-info-item-genre"]::text').getall()
        return ", ".join(g.strip() for g in genres) if genres else None

    def extract_review_sentiment(self, review_element):
        """Determine if review is fresh/rotten"""
        if review_element.css('[data-qa="fresh"]'):
            return 1.0
        elif review_element.css('[data-qa="rotten"]'):
            return 0.0
        return None

    def extract_user_rating(self, review_element):
        """Extract user star rating"""
        stars = review_element.css('[data-qa="star-rating"]::attr(aria-label)').get()
        if stars:
            match = re.search(r'([\d.]+)', stars)
            if match:
                return float(match.group(1))
        return None