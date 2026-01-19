"""
Metacritic Scraper (compact, almost-ERD complete).
I wrote this in a simple style so I can explain it during the defense.

This spider yields ERD items:
- metacritic_movie
- metacritic_score_summary
- metacritic_user + metacritic_user_review
- metacritic_publication + metacritic_critic_review
- person + movie_person_role
- production_company + movie_production_company
- award_org + movie_award_summary (best-effort)

Author: Jeffrey | Online Data Mining - Amsterdam UAS.
"""

# json is used to parse JSON-LD if Metacritic provides it.
import json

# re is used for regex patterns like slug, numbers, and dates.
import re

# zlib is used to create stable numeric IDs from text keys.
import zlib

# datetime is used for scraped_at timestamps.
from datetime import datetime

# typing is only used to make the code easier to read.
from typing import Any, Dict, List, Optional, Tuple

# Scrapy is our crawler framework.
import scrapy

# Selector is used to turn small HTML fragments into text.
from scrapy import Selector

# Import ERD-aligned items from items.py.
from imdb_scraper.items import (
    MetacriticMovieItem,
    MetacriticScoreSummaryItem,
    MetacriticUserItem,
    MetacriticUserReviewItem,
    MetacriticPublicationItem,
    MetacriticCriticReviewItem,
    PersonItem,
    MoviePersonRoleItem,
    AwardOrgItem,
    MovieAwardSummaryItem,
    ProductionCompanyItem,
    MovieProductionCompanyItem,
)


class MetacriticSpider(scrapy.Spider):
    # Name for running: scrapy crawl metacritic_scraper
    name = "metacritic_scraper"

    # Safety: only allow this domain.
    allowed_domains = ["metacritic.com"]

    # Use a normal browser User-Agent to reduce blocking.
    custom_settings = {
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }

    def __init__(
        self,
        # Limit movies for testing, example: -a max_movies=10
        max_movies: int = 1000,
        # Browse pages limit (Metacritic browse is paginated), example: -a max_pages=2
        max_pages: int = 3,
        # Review pages per movie (critic + user), example: -a max_review_pages=2
        max_review_pages: int = 2,
        # Reviews per page to keep, example: -a max_reviews_per_movie=25
        max_reviews_per_movie: int = 100,
        # Seed urls for quick testing (comma-separated).
        seed_urls: str = "",
        # Use Playwright on browse pages (browse sometimes needs JS).
        use_playwright_on_browse: str = "true",
        # Use Playwright on detail pages if blocked.
        use_playwright_on_detail: str = "false",
        *args,
        **kwargs,
    ):
        # Call the parent constructor.
        super().__init__(*args, **kwargs)

        # Convert numeric args (Scrapy passes them as strings).
        self.max_movies = int(max_movies)
        self.max_pages = int(max_pages)
        self.max_review_pages = int(max_review_pages)
        self.max_reviews_per_movie = int(max_reviews_per_movie)

        # Parse seed urls list.
        self.seed_urls = [u.strip() for u in (seed_urls or "").split(",") if u.strip()]

        # Parse playwright flags.
        self.use_playwright_on_browse = str(use_playwright_on_browse).lower() == "true"
        self.use_playwright_on_detail = str(use_playwright_on_detail).lower() == "true"

        # Track number of scraped movies.
        self.movies_scraped = 0

        # Dedupe sets.
        self.seen_slugs = set()
        self.seen_users = set()
        self.seen_publications = set()
        self.seen_persons = set()
        self.seen_prodcos = set()
        self.seen_awards = set()

    # -------------------------
    # Start requests
    # -------------------------

    def start_requests(self):
        # If seed urls are provided, we scrape only those.
        if self.seed_urls:
            # Loop seed URLs but limit by max_movies.
            for url in self.seed_urls[: self.max_movies]:
                # Request the movie page.
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_movie,
                    meta=self._detail_meta(),
                    priority=50,
                )
            # Stop here to avoid browsing.
            return

        # Otherwise scrape from browse pages.
        base_url = "https://www.metacritic.com/browse/movie/?page={}"

        # Loop pages from 0 to max_pages-1.
        for page in range(0, self.max_pages):
            # Build URL for this browse page.
            url = base_url.format(page)

            # Request browse page.
            yield scrapy.Request(
                url=url,
                callback=self.parse_browse,
                meta=self._browse_meta(),
                priority=10,
            )

    # -------------------------
    # Browse parsing
    # -------------------------

    def parse_browse(self, response):
        # Stop if we hit our movie limit.
        if self.movies_scraped >= self.max_movies:
            return

        # Collect links that look like /movie/<slug>
        hrefs = response.css('a[href^="/movie/"]::attr(href)').getall() or []

        # Loop all hrefs.
        for href in hrefs:
            # Stop if we hit limit.
            if self.movies_scraped >= self.max_movies:
                break

            # Skip empty.
            if not href:
                continue

            # Keep only direct movie pages, not review pages.
            if not re.match(r"^/movie/[^/]+/?$", href):
                continue

            # Extract slug.
            slug = self._extract_slug(href)

            # Skip if slug missing.
            if not slug:
                continue

            # Skip if we already scraped it.
            if slug in self.seen_slugs:
                continue

            # Mark slug as seen.
            self.seen_slugs.add(slug)

            # Increase movie counter.
            self.movies_scraped += 1

            # Follow the movie link.
            yield response.follow(
                href,
                callback=self.parse_movie,
                meta=self._detail_meta(),
                priority=30,
            )

    # -------------------------
    # Movie parsing
    # -------------------------

    def parse_movie(self, response):
        # Extract slug from URL.
        slug = self._extract_slug(response.url)

        # Stop if slug missing.
        if not slug:
            return

        # Create stable numeric movie_id.
        movie_id = self._stable_id(f"movie:{slug}")

        # Parse JSON-LD for stable fields if present.
        ld_movie = self._extract_jsonld_movie(response)

        # Extract title from JSON-LD if available.
        title_on_site = self._safe_strip((ld_movie or {}).get("name"))

        # Fallback title from h1.
        if not title_on_site:
            title_on_site = self._safe_strip(response.css("h1::text").get())

        # Make one clean text version of the whole page (easier parsing).
        page_text = self._page_text(response)

        # Release date from JSON-LD or page text.
        release_date = self._extract_release_date(page_text, ld_movie)

        # Runtime minutes from JSON-LD or patterns like "1 h 36 m".
        runtime_minutes = self._extract_runtime_minutes(page_text, ld_movie)

        # Content rating from JSON-LD or page text.
        content_rating = self._extract_content_rating(page_text, ld_movie)

        # Extract summary from JSON-LD description if possible.
        summary = self._safe_strip((ld_movie or {}).get("description"))

        # Fallback summary from description area.
        if not summary:
            summary = self._safe_strip(" ".join(response.css('div[class*="description"] *::text').getall() or []))

        # Metascore as 0-100.
        metascore = self._extract_metascore(page_text)

        # User score as 0.0-10.0.
        user_score = self._extract_userscore(page_text)

        # Critic review count (Based on X Critic Reviews).
        critic_review_count = self._extract_based_on_count(page_text, kind="critic")

        # User rating count (Based on Y User Ratings).
        user_rating_count = self._extract_based_on_count(page_text, kind="user")

        # Extract distributions (positive/mixed/negative), best-effort.
        c_pos, c_mix, c_neg = self._extract_distribution_counts(page_text, kind="critic")
        u_pos, u_mix, u_neg = self._extract_distribution_counts(page_text, kind="user")

        # Yield metacritic_movie item.
        yield MetacriticMovieItem(
            movie_id=movie_id,
            metacritic_url=response.url,
            metacritic_slug=slug,
            title_on_site=title_on_site,
            release_date=release_date,
            runtime_minutes=runtime_minutes,
            content_rating=content_rating,
            summary=summary,
            metascore=metascore,
            critic_review_count=critic_review_count,
            user_score=user_score,
            user_rating_count=user_rating_count,
            scraped_at=datetime.now().isoformat(),
        )

        # Yield metacritic_score_summary item.
        yield MetacriticScoreSummaryItem(
            movie_id=movie_id,
            critic_positive_count=c_pos,
            critic_mixed_count=c_mix,
            critic_negative_count=c_neg,
            user_positive_count=u_pos,
            user_mixed_count=u_mix,
            user_negative_count=u_neg,
            scraped_at=datetime.now().isoformat(),
        )

        # Yield people + roles.
        yield from self._yield_people_roles(response, movie_id)

        # Yield production companies.
        yield from self._yield_production_companies(page_text, movie_id)

        # Yield awards best-effort (if pattern exists).
        yield from self._yield_awards(page_text, movie_id)

        # Build review page URLs.
        critic_base = f"https://www.metacritic.com/movie/{slug}/critic-reviews/"
        user_base = f"https://www.metacritic.com/movie/{slug}/user-reviews/"

        # Queue critic review pages.
        for p in range(self.max_review_pages):
            # Build URL for page p.
            url = critic_base if p == 0 else f"{critic_base}?page={p}"
            # Request critic reviews.
            yield scrapy.Request(
                url=url,
                callback=self.parse_critic_reviews,
                meta={**self._detail_meta(), "movie_id": movie_id},
                priority=40,
            )

        # Queue user review pages.
        for p in range(self.max_review_pages):
            # Build URL for page p.
            url = user_base if p == 0 else f"{user_base}?page={p}"
            # Request user reviews.
            yield scrapy.Request(
                url=url,
                callback=self.parse_user_reviews,
                meta={**self._detail_meta(), "movie_id": movie_id},
                priority=40,
            )

    # -------------------------
    # Critic reviews parsing
    # -------------------------

    def parse_critic_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Get cleaned text tokens from the page.
        tokens = self._tokens(response)

        # Parse critic reviews from token stream.
        reviews = self._parse_critic_reviews_from_tokens(tokens)

        # Limit how many we store.
        for idx, r in enumerate(reviews[: self.max_reviews_per_movie], start=1):
            # Publication handling.
            publication_id = None
            pub_name = r.get("publication_name")
            pub_url = r.get("publication_url")

            # If we have publication name, create stable ID and yield it once.
            if pub_name:
                pub_key = pub_name.lower().strip()
                publication_id = self._stable_id(f"publication:{pub_key}")
                if pub_key not in self.seen_publications:
                    self.seen_publications.add(pub_key)
                    yield MetacriticPublicationItem(
                        publication_id=publication_id,
                        name=pub_name,
                        publication_url=pub_url,
                    )

            # Yield critic review row.
            yield MetacriticCriticReviewItem(
                critic_review_id=self._stable_id(f"critic_review:{movie_id}:{response.url}:{idx}"),
                movie_id=movie_id,
                publication_id=publication_id,
                critic_name=r.get("critic_name"),
                score=r.get("score"),
                review_date=r.get("review_date"),
                excerpt=r.get("excerpt"),
                full_review_url=r.get("full_review_url"),
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # User reviews parsing
    # -------------------------

    def parse_user_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Get cleaned text tokens from the page.
        tokens = self._tokens(response)

        # Parse user reviews from token stream.
        reviews = self._parse_user_reviews_from_tokens(tokens)

        # Limit how many we store.
        for idx, r in enumerate(reviews[: self.max_reviews_per_movie], start=1):
            # User handling.
            metacritic_user_id = None
            username = r.get("username")
            profile_url = r.get("profile_url")

            # Yield user only once.
            if username:
                user_key = username.lower().strip()
                metacritic_user_id = self._stable_id(f"user:{user_key}")
                if user_key not in self.seen_users:
                    self.seen_users.add(user_key)
                    yield MetacriticUserItem(
                        metacritic_user_id=metacritic_user_id,
                        username=username,
                        profile_url=profile_url,
                        scraped_at=datetime.now().isoformat(),
                    )

            # Yield user review row.
            yield MetacriticUserReviewItem(
                user_review_id=self._stable_id(f"user_review:{movie_id}:{metacritic_user_id}:{response.url}:{idx}"),
                movie_id=movie_id,
                metacritic_user_id=metacritic_user_id,
                score=r.get("score"),
                review_date=r.get("review_date"),
                review_text=r.get("review_text"),
                helpful_count=r.get("helpful_count"),
                unhelpful_count=r.get("unhelpful_count"),
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # Meta and Playwright helpers
    # -------------------------

    def _browse_meta(self) -> Dict[str, Any]:
        # If browse Playwright disabled, return empty.
        if not self.use_playwright_on_browse:
            return {}
        # Otherwise enable Playwright for browse.
        return {
            "playwright": True,
            "playwright_include_page": False,
            "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 30000},
        }

    def _detail_meta(self) -> Dict[str, Any]:
        # If detail Playwright disabled, return empty.
        if not self.use_playwright_on_detail:
            return {}
        # Otherwise enable Playwright for detail pages.
        return {
            "playwright": True,
            "playwright_include_page": False,
            "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 30000},
        }

    # -------------------------
    # ERD yield helpers
    # -------------------------

    def _yield_people_roles(self, response, movie_id: int):
        # Directors can be found on the page as plain text.
        directors = self._split_list(self._detail_value(response, "Directed By"))

        # Writers can be found on the page as plain text.
        writers = self._split_list(self._detail_value(response, "Written By"))

        # Actors appear in "Top Cast" with "Name Character".
        actors = self._extract_top_cast_pairs(response)

        # Yield directors.
        for name in directors:
            yield from self._yield_person_role(movie_id, name, "director", None, None)

        # Yield writers.
        for name in writers:
            yield from self._yield_person_role(movie_id, name, "writer", None, None)

        # Yield actors with billing order.
        for idx, (actor_name, character_name) in enumerate(actors[:15], start=1):
            yield from self._yield_person_role(movie_id, actor_name, "actor", character_name, idx)

    def _yield_person_role(
        self,
        movie_id: int,
        person_name: str,
        role_type: str,
        character_name: Optional[str],
        billing_order: Optional[int],
    ):
        # Normalize person name.
        key = (person_name or "").strip().lower()

        # Skip empty.
        if not key:
            return

        # Create stable person_id.
        person_id = self._stable_id(f"person:{key}")

        # Yield person only once.
        if key not in self.seen_persons:
            self.seen_persons.add(key)
            yield PersonItem(
                person_id=person_id,
                name=person_name.strip(),
                metacritic_person_url=None,
                scraped_at=datetime.now().isoformat(),
            )

        # Yield role link row.
        yield MoviePersonRoleItem(
            movie_person_role_id=self._stable_id(f"mpr:{movie_id}:{person_id}:{role_type}:{billing_order or 0}"),
            movie_id=movie_id,
            person_id=person_id,
            role_type=role_type,
            character_name=character_name,
            billing_order=billing_order,
            scraped_at=datetime.now().isoformat(),
        )

    def _yield_production_companies(self, page_text: str, movie_id: int):
        # Metacritic often shows the distributor or production label in a bullet list.
        # Example token: "Blue Harbor Entertainment"
        # We keep this best-effort, because the site is not always consistent.

        # Try a pattern that matches a likely company name near the year/runtime list.
        candidates = re.findall(r"\b([A-Z][A-Za-z0-9&.' -]{2,60} Entertainment)\b", page_text)
        companies = []
        for c in candidates:
            c = (c or "").strip()
            if c and c not in companies:
                companies.append(c)

        # If we found nothing, do not yield anything.
        if not companies:
            return

        # Loop companies.
        for name in companies[:5]:
            # Normalize.
            key = name.strip().lower()

            # Create stable ID.
            prodco_id = self._stable_id(f"prodco:{key}")

            # Yield company only once.
            if key not in self.seen_prodcos:
                self.seen_prodcos.add(key)
                yield ProductionCompanyItem(
                    production_company_id=prodco_id,
                    name=name.strip(),
                    scraped_at=datetime.now().isoformat(),
                )

            # Yield link table row.
            yield MovieProductionCompanyItem(
                movie_id=movie_id,
                production_company_id=prodco_id,
                scraped_at=datetime.now().isoformat(),
            )

    def _yield_awards(self, page_text: str, movie_id: int):
        # Best-effort pattern like: "Oscars: 2 wins, 5 nominations"
        matches = re.findall(
            r"([A-Za-z0-9 '&-]{3,60}):\s*(\d+)\s*wins?,\s*(\d+)\s*nominations?",
            page_text,
            flags=re.IGNORECASE,
        )

        # Loop matches.
        for (org_name, wins, noms) in matches:
            # Clean org name.
            org_clean = (org_name or "").strip()

            # Normalize key.
            key = org_clean.lower()

            # Skip empty.
            if not key:
                continue

            # Create award org id.
            award_org_id = self._stable_id(f"award:{key}")

            # Yield award org only once.
            if key not in self.seen_awards:
                self.seen_awards.add(key)
                yield AwardOrgItem(
                    award_org_id=award_org_id,
                    name=org_clean,
                    award_org_url=None,
                    scraped_at=datetime.now().isoformat(),
                )

            # Yield movie award summary.
            yield MovieAwardSummaryItem(
                movie_award_summary_id=self._stable_id(f"ma:{movie_id}:{award_org_id}"),
                movie_id=movie_id,
                award_org_id=award_org_id,
                wins=int(wins),
                nominations=int(noms),
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # Core extraction helpers
    # -------------------------

    def _extract_slug(self, url_or_path: str) -> Optional[str]:
        # Match /movie/<slug> and also allow a trailing slash and query params.
        m = re.search(r"/movie/([^/]+)/?(?:\?.*)?$", url_or_path or "")
        return m.group(1) if m else None

    def _stable_id(self, text: str) -> int:
        # Use adler32 checksum for stable numeric id.
        checksum = zlib.adler32(text.encode("utf-8"))
        # Add offset to keep id positive and large.
        return int(10_000_000_000 + checksum)

    def _safe_strip(self, s: Any) -> Optional[str]:
        # None check.
        if s is None:
            return None
        # Convert to string.
        s = str(s)
        # Strip whitespace.
        s = s.strip()
        # Return None if empty.
        return s if s else None

    def _split_list(self, text: Optional[str]) -> List[str]:
        # Return empty list if no text.
        if not text:
            return []
        # Split on comma.
        parts = [p.strip() for p in text.split(",")]
        # Keep only non-empty.
        return [p for p in parts if p]

    def _page_text(self, response) -> str:
        # Get all visible text nodes.
        parts = response.css("body *::text").getall() or []
        # Strip each part and keep non-empty.
        parts = [p.strip() for p in parts if p and p.strip()]
        # Join into one string.
        text = " ".join(parts)
        # Normalize whitespace.
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _tokens(self, response) -> List[str]:
        # Get all text nodes as tokens.
        parts = response.css("body *::text").getall() or []
        # Clean tokens.
        tokens = []
        for p in parts:
            p = (p or "").strip()
            if not p:
                continue
            # Normalize internal whitespace.
            p = re.sub(r"\s+", " ", p).strip()
            if p:
                tokens.append(p)
        return tokens

    def _extract_jsonld_movie(self, response) -> Optional[Dict[str, Any]]:
        # Read JSON-LD scripts.
        scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall() or []
        # Loop scripts.
        for raw in scripts:
            # Clean.
            raw = (raw or "").strip()
            # Skip empty.
            if not raw:
                continue
            # Try parse JSON.
            try:
                data = json.loads(raw)
            except Exception:
                continue
            # JSON can be dict or list.
            candidates = data if isinstance(data, list) else [data]
            # Loop candidates.
            for obj in candidates:
                # Only dict is useful.
                if not isinstance(obj, dict):
                    continue
                # Graph case.
                if "@graph" in obj and isinstance(obj["@graph"], list):
                    for g in obj["@graph"]:
                        if isinstance(g, dict) and g.get("@type") in ("Movie", "Film"):
                            return g
                # Direct case.
                if obj.get("@type") in ("Movie", "Film"):
                    return obj
        # Return None if not found.
        return None

    def _detail_value(self, response, label: str) -> Optional[str]:
        # Best-effort: find a node that contains the label and return its text.
        xp = f'//*[self::li or self::div or self::span][.//text()[contains(., "{label}")]]'
        node_html = response.xpath(xp).get()
        if not node_html:
            return None
        txt = " ".join(Selector(text=node_html).css("::text").getall() or [])
        txt = re.sub(r"\s+", " ", txt).strip()
        txt = txt.replace(label, "").strip(" :|-")
        return txt if txt else None

    # -------------------------
    # Field parsing from text
    # -------------------------

    def _extract_release_date(self, text: str, ld_movie: Optional[Dict[str, Any]]) -> Optional[str]:
        # 1) JSON-LD datePublished.
        if ld_movie:
            dp = ld_movie.get("datePublished")
            if isinstance(dp, str) and dp.strip():
                return dp.strip()

        # 2) Look for "Release Date Apr 4, 2025".
        m = re.search(r"\bRelease Date\s+([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b", text)
        if m:
            return m.group(1).strip()

        return None

    def _extract_runtime_minutes(self, text: str, ld_movie: Optional[Dict[str, Any]]) -> Optional[int]:
        # 1) JSON-LD duration like "PT96M".
        if ld_movie:
            dur = ld_movie.get("duration")
            if isinstance(dur, str):
                m = re.search(r"PT(\d+)M", dur)
                if m:
                    return int(m.group(1))

        # 2) Pattern like "1 h 36 m" anywhere.
        m = re.search(r"\b(\d+)\s*h\s*(\d+)\s*m\b", text, flags=re.IGNORECASE)
        if m:
            return int(m.group(1)) * 60 + int(m.group(2))

        # 3) Pattern like "96 m" anywhere.
        m = re.search(r"\b(\d+)\s*m\b", text, flags=re.IGNORECASE)
        if m:
            val = int(m.group(1))
            # Avoid catching years.
            if 10 <= val <= 400:
                return val

        return None

    def _extract_content_rating(self, text: str, ld_movie: Optional[Dict[str, Any]]) -> Optional[str]:
        # 1) JSON-LD contentRating.
        if ld_movie:
            cr = ld_movie.get("contentRating")
            if isinstance(cr, str) and cr.strip():
                return cr.strip()

        # 2) Pattern like "Rating PG-13".
        m = re.search(r"\bRating\s+(G|PG|PG-13|R|NC-17|NR|Not Rated|TV-MA|TV-14|TV-PG)\b", text)
        if m:
            return m.group(1).strip()

        return None

    def _extract_metascore(self, text: str) -> Optional[int]:
        # Look for "Metascore ... 85".
        m = re.search(r"\bMetascore\b.*?\b(\d{1,3})\b", text, flags=re.IGNORECASE)
        if not m:
            return None
        val = int(m.group(1))
        if 0 <= val <= 100:
            return val
        return None

    def _extract_userscore(self, text: str) -> Optional[float]:
        # Look for "User Score ... 6.8".
        m = re.search(r"\bUser Score\b.*?\b(\d{1,2}\.\d)\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= 10.0:
                return val

        # Fallback integer like "User Score ... 7".
        m = re.search(r"\bUser Score\b.*?\b(\d{1,2})\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= 10.0:
                return val

        return None

    def _extract_based_on_count(self, text: str, kind: str) -> Optional[int]:
        # Critic: "Based on 7 Critic Reviews"
        if kind == "critic":
            m = re.search(r"\bBased on\s+([\d,]+)\s+Critic Reviews\b", text, flags=re.IGNORECASE)
            if not m:
                return None
            return int(m.group(1).replace(",", ""))

        # User: "Based on 28 User Ratings"
        m = re.search(r"\bBased on\s+([\d,]+)\s+User Ratings\b", text, flags=re.IGNORECASE)
        if not m:
            return None
        return int(m.group(1).replace(",", ""))

    def _extract_distribution_counts(self, text: str, kind: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        # Critic block example:
        # "86% Positive 6 Reviews 14% Mixed 1 Review 0% Negative 0 Reviews"
        if kind == "critic":
            m = re.search(
                r"\bPositive\b\s+(\d+)\s+Reviews?\b.*?\bMixed\b\s+(\d+)\s+Reviews?\b.*?\bNegative\b\s+(\d+)\s+Reviews?\b",
                text,
                flags=re.IGNORECASE,
            )
            if not m:
                return None, None, None
            return int(m.group(1)), int(m.group(2)), int(m.group(3))

        # User block example:
        # "68% Positive 19 Ratings 14% Mixed 4 Ratings 18% Negative 5 Ratings"
        m = re.search(
            r"\bPositive\b\s+(\d+)\s+Ratings?\b.*?\bMixed\b\s+(\d+)\s+Ratings?\b.*?\bNegative\b\s+(\d+)\s+Ratings?\b",
            text,
            flags=re.IGNORECASE,
        )
        if not m:
            return None, None, None
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    # -------------------------
    # Cast parsing
    # -------------------------

    def _extract_top_cast_pairs(self, response) -> List[Tuple[str, Optional[str]]]:
        # Find /person/ links, because they usually contain actor + character.
        raw = response.css('a[href*="/person/"]::text').getall() or []
        raw = [re.sub(r"\s+", " ", (x or "").strip()) for x in raw if (x or "").strip()]

        # Heuristic: many entries look like "Karan Soni Naveen Gavaskar"
        # So we split the string and assume last 1-3 words is character if it looks like a role.
        pairs = []
        seen = set()

        for t in raw:
            key = t.lower()
            if key in seen:
                continue
            seen.add(key)

            # Skip headings that are not names.
            if t.lower() in ("view all", "view all cast & crew"):
                continue

            # If it is a single token, assume only name.
            parts = t.split(" ")
            if len(parts) <= 2:
                pairs.append((t, None))
                continue

            # Otherwise, take first 2 words as name candidate, rest as character candidate.
            # This is best-effort because names vary.
            actor_name = " ".join(parts[:2]).strip()
            character_name = " ".join(parts[2:]).strip()

            # If character looks too short, ignore it.
            if len(character_name) < 2:
                character_name = None

            pairs.append((actor_name, character_name))

        return pairs

    # -------------------------
    # Review parsing from tokens
    # -------------------------

    def _parse_critic_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:
        # Critic entries usually look like:
        # Date, Score, Publication, Excerpt, Read More, By Name, FULL REVIEW ...
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")
        score_re = re.compile(r"^\d{1,3}$")

        out = []
        i = 0

        while i < len(tokens):
            # Look for a date token.
            if not date_re.match(tokens[i]):
                i += 1
                continue

            review_date = tokens[i]
            i += 1

            # Next find a score within the next few tokens.
            score = None
            score_pos = None
            for j in range(i, min(i + 6, len(tokens))):
                if score_re.match(tokens[j]):
                    val = int(tokens[j])
                    if 0 <= val <= 100:
                        score = val
                        score_pos = j
                        break

            if score is None or score_pos is None:
                continue

            # Publication is usually the next non-numeric token after score.
            pub_name = None
            for j in range(score_pos + 1, min(score_pos + 6, len(tokens))):
                if not score_re.match(tokens[j]) and not date_re.match(tokens[j]):
                    if tokens[j].lower() not in ("read more", "report"):
                        pub_name = tokens[j]
                        break

            # Excerpt is usually after publication, until "Read More".
            excerpt_parts = []
            j = (score_pos + 2) if pub_name else (score_pos + 1)

            while j < len(tokens):
                if tokens[j].lower() == "read more":
                    break
                if date_re.match(tokens[j]):
                    break
                if tokens[j].lower() in ("critic reviews", "user reviews", "view all"):
                    break
                excerpt_parts.append(tokens[j])
                j += 1

            excerpt = " ".join(excerpt_parts).strip()
            if not excerpt:
                excerpt = None

            # Critic name often appears as "By Name".
            critic_name = None
            look = " ".join(tokens[max(0, j - 5): min(len(tokens), j + 5)])
            m = re.search(r"\bBy\s+([A-Z][A-Za-z .,'-]{2,80})\b", look)
            if m:
                critic_name = m.group(1).strip()

            out.append(
                {
                    "review_date": review_date,
                    "score": score,
                    "publication_name": pub_name,
                    "publication_url": None,
                    "excerpt": excerpt,
                    "critic_name": critic_name,
                    "full_review_url": None,
                }
            )

            # Continue scanning from where we stopped.
            i = j + 1

        return out

    def _parse_user_reviews_from_tokens(self, tokens: List[str]) -> List[Dict[str, Any]]:
        # User entries usually look like:
        # Date, Score(0-10), Username, Review text..., Read More, Report
        date_re = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}$")
        score_re = re.compile(r"^\d{1,2}$")

        out = []
        i = 0

        while i < len(tokens):
            # Find a date.
            if not date_re.match(tokens[i]):
                i += 1
                continue

            review_date = tokens[i]
            i += 1

            # Score is usually next.
            if i >= len(tokens) or not score_re.match(tokens[i]):
                continue

            val = int(tokens[i])
            if not (0 <= val <= 10):
                continue

            score = val
            i += 1

            # Next token is often username.
            username = tokens[i] if i < len(tokens) else None
            i += 1

            # Review text until "Read More" or "Report" or next date.
            review_parts = []
            while i < len(tokens):
                low = tokens[i].lower()
                if low in ("read more", "report"):
                    break
                if date_re.match(tokens[i]):
                    break
                if low in ("user reviews", "critic reviews", "view all"):
                    break
                review_parts.append(tokens[i])
                i += 1

            review_text = " ".join(review_parts).strip()
            if not review_text:
                review_text = None

            out.append(
                {
                    "review_date": review_date,
                    "score": score,
                    "username": username,
                    "profile_url": None,
                    "review_text": review_text,
                    "helpful_count": None,
                    "unhelpful_count": None,
                }
            )

            i += 1

        return out
