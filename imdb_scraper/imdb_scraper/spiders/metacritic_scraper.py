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

# re is used for regex patterns like slug, numbers, dates.
import re

# zlib is used to create stable numeric IDs from text keys.
import zlib

# datetime is used for scraped_at timestamps.
from datetime import datetime

# typing is only used to make the code easier to read.
from typing import Any, Dict, List, Optional, Tuple

# Scrapy is our crawler framework.
import scrapy

# Selector is used to parse HTML fragments when needed.
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
        max_movies: int = 50,
        # Browse pages limit (Metacritic browse is paginated), example: -a max_pages=2
        max_pages: int = 3,
        # Review pages per movie (critic + user), example: -a max_review_pages=2
        max_review_pages: int = 2,
        # Reviews per page to keep, example: -a max_reviews_per_movie=25
        max_reviews_per_movie: int = 50,
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
                yield scrapy.Request(url=url, callback=self.parse_movie, meta=self._detail_meta(), priority=50)
            # Stop here to avoid browsing.
            return

        # Otherwise scrape from browse pages.
        base_url = "https://www.metacritic.com/browse/movie/?page={}"

        # Loop pages from 0 to max_pages-1.
        for page in range(0, self.max_pages):
            # Build URL for this browse page.
            url = base_url.format(page)

            # Request browse page.
            yield scrapy.Request(url=url, callback=self.parse_browse, meta=self._browse_meta(), priority=10)

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
            yield response.follow(href, callback=self.parse_movie, meta=self._detail_meta(), priority=30)

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

        # Extract release date from details.
        release_date = self._detail_value(response, "Release Date")

        # Extract runtime in minutes from JSON-LD or details.
        runtime_minutes = self._runtime_minutes(response, ld_movie)

        # Extract content rating from details (PG-13 etc).
        content_rating = self._detail_value(response, "Rating")

        # Extract summary from JSON-LD description if possible.
        summary = self._safe_strip((ld_movie or {}).get("description"))

        # Fallback summary from description area.
        if not summary:
            summary = self._safe_strip(" ".join(response.css('div[class*="description"] *::text').getall() or []))

        # Extract metascore with targeted selectors first.
        metascore = self._extract_metascore(response)

        # Extract user score with targeted selectors.
        user_score = self._extract_userscore(response)

        # Extract critic review count if shown.
        critic_review_count = self._extract_count_by_keywords(response, ["Critic Reviews", "Critic reviews"])

        # Extract user rating count if shown.
        user_rating_count = self._extract_count_by_keywords(response, ["User Ratings", "User ratings"])

        # Extract distributions (positive/mixed/negative), best-effort.
        c_pos, c_mix, c_neg = self._extract_distribution(response, section_hint="critic")
        u_pos, u_mix, u_neg = self._extract_distribution(response, section_hint="user")

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
        yield from self._yield_production_companies(response, movie_id)

        # Yield awards best-effort (if pattern exists).
        yield from self._yield_awards(response, movie_id)

        # Build review page URLs.
        critic_base = f"https://www.metacritic.com/movie/{slug}/critic-reviews/"
        user_base = f"https://www.metacritic.com/movie/{slug}/user-reviews/"

        # Queue critic review pages.
        for p in range(self.max_review_pages):
            # Build URL for page p.
            url = critic_base if p == 0 else f"{critic_base}?page={p}"
            # Request critic reviews.
            yield scrapy.Request(url=url, callback=self.parse_critic_reviews, meta={"movie_id": movie_id}, priority=40)

        # Queue user review pages.
        for p in range(self.max_review_pages):
            # Build URL for page p.
            url = user_base if p == 0 else f"{user_base}?page={p}"
            # Request user reviews.
            yield scrapy.Request(url=url, callback=self.parse_user_reviews, meta={"movie_id": movie_id}, priority=40)

    # -------------------------
    # Critic reviews parsing
    # -------------------------

    def parse_critic_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Find review containers (best-effort).
        blocks = response.css("article, div")

        # Counter for limiting.
        count = 0

        # Loop blocks.
        for block in blocks:
            # Stop at limit.
            if count >= self.max_reviews_per_movie:
                break

            # Extract publication name.
            pub_name = self._safe_strip(block.xpath('.//a[contains(@href, "/publication/")]/text()').get())

            # Extract publication url.
            pub_href = block.xpath('.//a[contains(@href, "/publication/")]/@href').get()

            # Convert to absolute url.
            pub_url = response.urljoin(pub_href) if pub_href else None

            # Extract excerpt text.
            excerpt = self._safe_strip(" ".join(block.css("p::text").getall() or []))

            # Skip if no useful data.
            if not pub_name and not excerpt:
                continue

            # Create publication_id if name exists.
            publication_id = None

            # If we have a publication, handle it.
            if pub_name:
                # Normalize key.
                pub_key = pub_name.lower().strip()
                # Create stable ID.
                publication_id = self._stable_id(f"publication:{pub_key}")
                # Yield publication only once.
                if pub_key not in self.seen_publications:
                    self.seen_publications.add(pub_key)
                    # Yield publication row.
                    yield MetacriticPublicationItem(
                        publication_id=publication_id,
                        name=pub_name,
                        publication_url=pub_url,
                    )

            # Extract critic score (0-100) from the block html.
            score = self._first_int_from_html(block.get(), max_value=100)

            # Extract critic name (best-effort "By X").
            critic_name = self._extract_by_author_from_html(block.get())

            # Extract review date (best-effort).
            review_date = self._extract_date_from_html(block.get())

            # Extract full review link if present.
            full_href = block.xpath('.//a[contains(translate(., "READ", "read"), "read")]/@href').get()
            # Convert to absolute.
            full_url = response.urljoin(full_href) if full_href else None

            # Increase counter.
            count += 1

            # Yield critic review row.
            yield MetacriticCriticReviewItem(
                critic_review_id=self._stable_id(f"critic_review:{movie_id}:{response.url}:{count}"),
                movie_id=movie_id,
                publication_id=publication_id,
                critic_name=critic_name,
                score=score,
                review_date=review_date,
                excerpt=excerpt,
                full_review_url=full_url,
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # User reviews parsing
    # -------------------------

    def parse_user_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Find review containers (best-effort).
        blocks = response.css("article, div")

        # Counter for limiting.
        count = 0

        # Loop blocks.
        for block in blocks:
            # Stop at limit.
            if count >= self.max_reviews_per_movie:
                break

            # Extract username.
            username = self._safe_strip(block.xpath('.//a[contains(@href, "/user/")]/text()').get())

            # Extract profile url.
            user_href = block.xpath('.//a[contains(@href, "/user/")]/@href').get()

            # Convert to absolute.
            profile_url = response.urljoin(user_href) if user_href else None

            # Extract review text.
            review_text = self._safe_strip(" ".join(block.css("p::text").getall() or []))

            # Skip empty blocks.
            if not username and not review_text:
                continue

            # Create user id if username exists.
            metacritic_user_id = None

            # Yield user only once.
            if username:
                # Normalize key.
                user_key = username.lower().strip()
                # Create stable ID.
                metacritic_user_id = self._stable_id(f"user:{user_key}")
                # Yield user if new.
                if user_key not in self.seen_users:
                    self.seen_users.add(user_key)
                    # Yield user row.
                    yield MetacriticUserItem(
                        metacritic_user_id=metacritic_user_id,
                        username=username,
                        profile_url=profile_url,
                        scraped_at=datetime.now().isoformat(),
                    )

            # Extract score (0-10) from html.
            score = self._first_int_from_html(block.get(), max_value=10)

            # Extract review date (best-effort).
            review_date = self._extract_date_from_html(block.get())

            # Extract helpful/unhelpful counts (best-effort).
            helpful, unhelpful = self._extract_helpful_from_html(block.get())

            # Increase counter.
            count += 1

            # Yield user review row.
            yield MetacriticUserReviewItem(
                user_review_id=self._stable_id(f"user_review:{movie_id}:{metacritic_user_id}:{response.url}:{count}"),
                movie_id=movie_id,
                metacritic_user_id=metacritic_user_id,
                score=score,
                review_date=review_date,
                review_text=review_text,
                helpful_count=helpful,
                unhelpful_count=unhelpful,
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
        # Read directors from details.
        directors = self._split_list(self._detail_value(response, "Director"))

        # Read writers from details.
        writers = self._split_list(self._detail_value(response, "Writer"))

        # Also try to extract actor names from /person/ links on the page.
        actor_names = self._extract_person_links(response)

        # Yield directors.
        for name in directors:
            yield from self._yield_person_role(movie_id, name, "director", None, None)

        # Yield writers.
        for name in writers:
            yield from self._yield_person_role(movie_id, name, "writer", None, None)

        # Yield actors with billing order (limit 15).
        for idx, name in enumerate(actor_names[:15], start=1):
            yield from self._yield_person_role(movie_id, name, "actor", None, idx)

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
            movie_person_role_id=self._stable_id(f"mpr:{movie_id}:{person_id}:{role_type}"),
            movie_id=movie_id,
            person_id=person_id,
            role_type=role_type,
            character_name=character_name,
            billing_order=billing_order,
            scraped_at=datetime.now().isoformat(),
        )

    def _yield_production_companies(self, response, movie_id: int):
        # Try Production field.
        companies = self._split_list(self._detail_value(response, "Production"))
        # Fallback Production Company field.
        if not companies:
            companies = self._split_list(self._detail_value(response, "Production Company"))

        # Loop companies.
        for name in companies:
            # Normalize.
            key = name.strip().lower()
            # Skip empty.
            if not key:
                continue

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

    def _yield_awards(self, response, movie_id: int):
        # Read full page text.
        text = " ".join(response.css("body *::text").getall() or [])
        # Normalize whitespace.
        text = re.sub(r"\s+", " ", text).strip()

        # Best-effort pattern like: "Oscars: 2 wins, 5 nominations"
        matches = re.findall(
            r"([A-Za-z0-9 '&-]{3,60}):\s*(\d+)\s*wins?,\s*(\d+)\s*nominations?",
            text,
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
    # Extraction helpers
    # -------------------------

    def _extract_slug(self, url_or_path: str) -> Optional[str]:
        # Match /movie/<slug> in URLs and paths.
        m = re.search(r"/movie/([^/]+)/?$", url_or_path or "")
        # Return the slug if found.
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
        # This tries to locate a "detail row" containing the label.
        # It is best-effort because Metacritic HTML changes.
        xp = f'//*[self::li or self::div][.//text()[contains(., "{label}")]]'
        # Get first matching node html.
        node_html = response.xpath(xp).get()
        # Return None if missing.
        if not node_html:
            return None
        # Convert node html into text.
        txt = " ".join(Selector(text=node_html).css("::text").getall() or [])
        # Normalize whitespace.
        txt = re.sub(r"\s+", " ", txt).strip()
        # Remove label.
        txt = txt.replace(label, "").strip(" :|-")
        # Return cleaned value.
        return txt if txt else None

    def _runtime_minutes(self, response, ld_movie: Optional[Dict[str, Any]]) -> Optional[int]:
        # Try JSON-LD duration like PT116M.
        if ld_movie:
            dur = ld_movie.get("duration")
            if isinstance(dur, str):
                m = re.search(r"PT(\d+)M", dur)
                if m:
                    return int(m.group(1))
        # Fallback to detail value.
        raw = self._detail_value(response, "Runtime")
        if not raw:
            return None
        # Try "107 min".
        m = re.search(r"(\d+)\s*min", raw.lower())
        if m:
            return int(m.group(1))
        # Return None if no match.
        return None

    def _extract_metascore(self, response) -> Optional[int]:
        # Try targeted selector for metascore.
        # Metacritic often has a score number in a score container.
        candidates = response.css('[data-testid*="metascore"]::text').getall() or []
        # Add fallback: any score-like large number near "Metascore".
        if not candidates:
            text = " ".join(response.css("body *::text").getall() or [])
            text = re.sub(r"\s+", " ", text)
            m = re.search(r"Metascore.*?\b(\d{1,3})\b", text, flags=re.IGNORECASE)
            candidates = [m.group(1)] if m else []
        # Loop candidates.
        for c in candidates:
            # Keep digits only.
            c = re.sub(r"[^\d]", "", c or "")
            # Skip empty.
            if not c:
                continue
            # Parse int.
            val = int(c)
            # Validate range.
            if 0 <= val <= 100:
                return val
        # Return None if not found.
        return None

    def _extract_userscore(self, response) -> Optional[float]:
        # Try to find user score as float near "User Score".
        text = " ".join(response.css("body *::text").getall() or [])
        text = re.sub(r"\s+", " ", text)
        # Try float format like 6.8.
        m = re.search(r"User Score.*?\b(\d{1,2}\.\d)\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= 10.0:
                return val
        # Try integer format like 7.
        m = re.search(r"User Score.*?\b(\d{1,2})\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= 10.0:
                return val
        # Return None if not found.
        return None

    def _extract_count_by_keywords(self, response, keywords: List[str]) -> Optional[int]:
        # Get page text.
        text = " ".join(response.css("body *::text").getall() or [])
        # Normalize whitespace.
        text = re.sub(r"\s+", " ", text)
        # Try each keyword.
        for k in keywords:
            # Match like "Critic Reviews 45" or "User Ratings 1,234"
            m = re.search(rf"{re.escape(k)}.*?\b([\d,]+)\b", text, flags=re.IGNORECASE)
            if not m:
                continue
            raw = (m.group(1) or "").replace(",", "").strip()
            if raw.isdigit():
                return int(raw)
        # Return None if no match.
        return None

    def _extract_distribution(self, response, section_hint: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        # Read page text.
        text = " ".join(response.css("body *::text").getall() or [])
        # Normalize whitespace.
        text = re.sub(r"\s+", " ", text)
        # Hint helps us search around correct words.
        # We look for: Positive <n> Mixed <n> Negative <n>
        if section_hint.lower() == "critic":
            prefix = "Critic"
        else:
            prefix = "User"
        # Try pattern.
        m = re.search(
            rf"{prefix}.*?Positive.*?\b(\d+)\b.*?Mixed.*?\b(\d+)\b.*?Negative.*?\b(\d+)\b",
            text,
            flags=re.IGNORECASE,
        )
        # Return None values if not found.
        if not m:
            return None, None, None
        # Return parsed ints.
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    def _extract_person_links(self, response) -> List[str]:
        # Collect names from /person/ links.
        names = response.css('a[href*="/person/"]::text').getall() or []
        # Clean names.
        cleaned = [self._safe_strip(n) for n in names if self._safe_strip(n)]
        # Deduplicate while preserving order.
        out = []
        seen = set()
        for n in cleaned:
            key = n.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(n)
        return out

    def _first_int_from_html(self, html: str, max_value: int) -> Optional[int]:
        # Turn HTML fragment into text.
        txt = " ".join(Selector(text=html).css("::text").getall() or [])
        # Normalize whitespace.
        txt = re.sub(r"\s+", " ", txt)
        # Find ints.
        nums = re.findall(r"\b(\d{1,3})\b", txt)
        # Loop candidates.
        for n in nums:
            val = int(n)
            # Skip year-like values.
            if 1900 <= val <= 2099:
                continue
            # Check range.
            if 0 <= val <= max_value:
                return val
        # Return None if not found.
        return None

    def _extract_date_from_html(self, html: str) -> Optional[str]:
        # Convert HTML to text.
        txt = " ".join(Selector(text=html).css("::text").getall() or [])
        # Normalize whitespace.
        txt = re.sub(r"\s+", " ", txt).strip()
        # Match date like "Jan 17, 2026".
        m = re.search(r"\b([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b", txt)
        # Return date string if found.
        return m.group(1) if m else None

    def _extract_by_author_from_html(self, html: str) -> Optional[str]:
        # Convert HTML to text.
        txt = " ".join(Selector(text=html).css("::text").getall() or [])
        # Normalize whitespace.
        txt = re.sub(r"\s+", " ", txt).strip()
        # Match "By Name".
        m = re.search(r"\bBy\s+([A-Z][A-Za-z .,'-]{2,80})\b", txt)
        # Return name if found.
        return m.group(1).strip() if m else None

    def _extract_helpful_from_html(self, html: str) -> Tuple[Optional[int], Optional[int]]:
        # Convert HTML to text.
        txt = " ".join(Selector(text=html).css("::text").getall() or [])
        # Normalize whitespace.
        txt = re.sub(r"\s+", " ", txt).strip()
        # Default values.
        helpful = None
        unhelpful = None
        # Try "X helpful".
        m = re.search(r"\b(\d+)\b.*helpful", txt, flags=re.IGNORECASE)
        if m:
            helpful = int(m.group(1))
        # Try "X unhelpful".
        m = re.search(r"\b(\d+)\b.*unhelpful", txt, flags=re.IGNORECASE)
        if m:
            unhelpful = int(m.group(1))
        # Return tuple.
        return helpful, unhelpful
