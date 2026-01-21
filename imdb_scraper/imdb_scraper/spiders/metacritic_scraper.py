# Author: Jeffrey | Online Data Mining - Amsterdam UAS
"""
Metacritic Scraper - Simplified version that only scrapes reviews.

This spider yields:
- MetacriticMovieItem (basic movie info + metascore)
- MetacriticCriticReviewItem (critic reviews)
- MetacriticUserReviewItem (user reviews)

Reads movies from IMDB SQLite database and matches them on Metacritic.
"""

import re
import sqlite3
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import scrapy
from scrapy import Selector

from imdb_scraper.items import (
    MetacriticMovieItem,
    MetacriticCriticReviewItem,
    MetacriticUserReviewItem,
)


class MetacriticSpider(scrapy.Spider):
    name = "metacritic_scraper"
    allowed_domains = ["metacritic.com"]

    custom_settings = {
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS": 2,
    }

    def __init__(
        self,
        max_movies: int = 1000,
        max_review_pages: int = 1,
        max_reviews_per_movie: int = 10,
        imdb_db_path: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_movies = int(max_movies)
        self.max_review_pages = int(max_review_pages)
        self.max_reviews_per_movie = int(max_reviews_per_movie)
        self.movies_scraped = 0
        self.seen_slugs = set()

        # Find IMDB database
        if imdb_db_path:
            self.imdb_db_path = Path(imdb_db_path)
        else:
            possible_paths = [
                Path(__file__).parent.parent.parent / "output" / "movies.db",
                Path("output/movies.db"),
            ]
            self.imdb_db_path = None
            for p in possible_paths:
                if p.exists():
                    self.imdb_db_path = p
                    break

    def start_requests(self):
        """Read movies from IMDB database and request Metacritic pages."""
        if not self.imdb_db_path or not self.imdb_db_path.exists():
            self.logger.error("IMDB database not found. Run movie_scraper first.")
            return

        self.logger.info(f"Reading movies from: {self.imdb_db_path}")

        conn = sqlite3.connect(self.imdb_db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT movie_id, title, year FROM movie ORDER BY movie_id LIMIT ?
        """, (self.max_movies,))
        movies = cursor.fetchall()
        conn.close()

        self.logger.info(f"Found {len(movies)} movies to fetch from Metacritic")

        for imdb_movie_id, title, year in movies:
            slug = self._title_to_slug(title)
            if not slug or slug in self.seen_slugs:
                continue
            self.seen_slugs.add(slug)

            movie_url = f"https://www.metacritic.com/movie/{slug}/"
            yield scrapy.Request(
                url=movie_url,
                callback=self.parse_movie,
                meta={"imdb_movie_id": imdb_movie_id, "title": title, "year": year},
                errback=self._handle_error,
            )

    def _title_to_slug(self, title: str) -> Optional[str]:
        """Convert movie title to Metacritic URL slug."""
        if not title:
            return None
        slug = title.lower()
        slug = re.sub(r'[^\w\s-]', ' ', slug)
        slug = re.sub(r'\s+', ' ', slug).strip()
        slug = slug.replace(' ', '-')
        slug = re.sub(r'-+', '-', slug).strip('-')
        return slug if slug else None

    def _handle_error(self, failure):
        """Handle 404 errors for movies not on Metacritic."""
        title = failure.request.meta.get("title", "Unknown")
        self.logger.warning(f"Failed to fetch Metacritic for: {title}")

    def parse_movie(self, response):
        """Parse movie page and queue review pages."""
        movie_id = response.meta.get("imdb_movie_id")
        slug = self._extract_slug(response.url)

        if not slug:
            return

        self.movies_scraped += 1
        self.logger.info(f"[{self.movies_scraped}] Parsing: {response.meta.get('title')}")

        # Extract basic info
        page_text = self._page_text(response)
        metascore = self._extract_metascore(response, page_text)
        user_score = self._extract_userscore(page_text)

        # Yield movie item with scores
        yield MetacriticMovieItem(
            movie_id=movie_id,
            metacritic_url=response.url,
            metacritic_slug=slug,
            title_on_site=response.css("h1::text").get(),
            metascore=metascore,
            user_score=user_score,
            critic_review_count=self._extract_review_count(page_text, "critic"),
            user_rating_count=self._extract_review_count(page_text, "user"),
            scraped_at=datetime.now().isoformat(),
        )

        # Queue critic review pages
        for p in range(self.max_review_pages):
            url = f"https://www.metacritic.com/movie/{slug}/critic-reviews/"
            if p > 0:
                url += f"?page={p}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_critic_reviews,
                meta={"movie_id": movie_id},
            )

        # Queue user review pages
        for p in range(self.max_review_pages):
            url = f"https://www.metacritic.com/movie/{slug}/user-reviews/"
            if p > 0:
                url += f"?page={p}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_user_reviews,
                meta={"movie_id": movie_id},
            )

    def parse_critic_reviews(self, response):
        """Parse critic reviews page."""
        movie_id = response.meta.get("movie_id")
        tokens = self._tokens(response)
        reviews = self._parse_critic_reviews_from_tokens(tokens)

        for idx, r in enumerate(reviews[:self.max_reviews_per_movie], start=1):
            yield MetacriticCriticReviewItem(
                critic_review_id=self._stable_id(f"critic:{movie_id}:{idx}:{response.url}"),
                movie_id=movie_id,
                publication_name=r.get("publication_name"),
                critic_name=r.get("critic_name"),
                score=r.get("score"),
                review_date=r.get("review_date"),
                excerpt=r.get("excerpt"),
                scraped_at=datetime.now().isoformat(),
            )

    def parse_user_reviews(self, response):
        """Parse user reviews page."""
        movie_id = response.meta.get("movie_id")
        tokens = self._tokens(response)
        reviews = self._parse_user_reviews_from_tokens(tokens)

        for idx, r in enumerate(reviews[:self.max_reviews_per_movie], start=1):
            yield MetacriticUserReviewItem(
                user_review_id=self._stable_id(f"user:{movie_id}:{idx}:{response.url}"),
                movie_id=movie_id,
                username=r.get("username"),
                score=r.get("score"),
                review_date=r.get("review_date"),
                review_text=r.get("review_text"),
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # Helper methods
    # -------------------------

    def _extract_slug(self, url: str) -> Optional[str]:
        m = re.search(r"/movie/([^/]+)/?", url)
        return m.group(1) if m else None

    def _stable_id(self, text: str) -> int:
        return int(10_000_000_000 + zlib.adler32(text.encode("utf-8")))

    def _page_text(self, response) -> str:
        parts = response.css("body *::text").getall() or []
        return " ".join(p.strip() for p in parts if p.strip())

    def _tokens(self, response) -> List[str]:
        parts = response.css("body *::text").getall() or []
        return [re.sub(r"\s+", " ", p).strip() for p in parts if p.strip()]

    def _extract_metascore(self, response, text: str) -> Optional[int]:
        # Try CSS selector first
        score = response.css('.c-productScoreInfo_scoreNumber span::text').get()
        if score and score.strip().isdigit():
            return int(score.strip())
        # Regex fallback
        m = re.search(r"\bMetascore\b\s*(\d{1,3})(?!\s*reviews)", text, re.IGNORECASE)
        if m and 0 <= int(m.group(1)) <= 100:
            return int(m.group(1))
        return None

    def _extract_userscore(self, text: str) -> Optional[float]:
        m = re.search(r"\bUser Score\b.*?\b(\d{1,2}\.?\d?)\b", text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= 10.0:
                return val
        return None

    def _extract_review_count(self, text: str, kind: str) -> Optional[int]:
        if kind == "critic":
            m = re.search(r"\bBased on\s+([\d,]+)\s+Critic Reviews\b", text, re.IGNORECASE)
        else:
            m = re.search(r"\bBased on\s+([\d,]+)\s+User Ratings\b", text, re.IGNORECASE)
        return int(m.group(1).replace(",", "")) if m else None

    def _parse_critic_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")
        score_re = re.compile(r"^\d{1,3}$")
        out = []
        i = 0

        while i < len(tokens):
            if not date_re.match(tokens[i]):
                i += 1
                continue

            review_date = tokens[i]
            i += 1

            # Find score
            score = None
            for j in range(i, min(i + 6, len(tokens))):
                if score_re.match(tokens[j]):
                    val = int(tokens[j])
                    if 0 <= val <= 100:
                        score = val
                        i = j + 1
                        break

            if score is None:
                continue

            # Find publication name
            pub_name = None
            for j in range(i, min(i + 4, len(tokens))):
                if not score_re.match(tokens[j]) and not date_re.match(tokens[j]):
                    if tokens[j].lower() not in ("read more", "report"):
                        pub_name = tokens[j]
                        i = j + 1
                        break

            # Collect excerpt
            excerpt_parts = []
            while i < len(tokens):
                if tokens[i].lower() == "read more" or date_re.match(tokens[i]):
                    break
                excerpt_parts.append(tokens[i])
                i += 1

            out.append({
                "review_date": review_date,
                "score": score,
                "publication_name": pub_name,
                "excerpt": " ".join(excerpt_parts).strip() or None,
                "critic_name": None,
            })

        return out

    def _parse_user_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")
        score_re = re.compile(r"^\d{1,2}$")
        out = []
        i = 0

        while i < len(tokens):
            if not date_re.match(tokens[i]):
                i += 1
                continue

            review_date = tokens[i]
            i += 1

            if i >= len(tokens) or not score_re.match(tokens[i]):
                continue

            score = int(tokens[i])
            if not (0 <= score <= 10):
                continue
            i += 1

            username = tokens[i] if i < len(tokens) else None
            i += 1

            # Collect review text
            review_parts = []
            while i < len(tokens):
                if tokens[i].lower() in ("read more", "report") or date_re.match(tokens[i]):
                    break
                review_parts.append(tokens[i])
                i += 1

            out.append({
                "review_date": review_date,
                "score": score,
                "username": username,
                "review_text": " ".join(review_parts).strip() or None,
            })

        return out
