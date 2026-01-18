"""
Metacritic Scraper.
This spider scrapes Metacritic data and tries to fill our ERD tables:
- metacritic_movie
- metacritic_score_summary
- metacritic_user + metacritic_user_review
- metacritic_publication + metacritic_critic_review
- person + movie_person_role (cast/crew)
- award_org + movie_award_summary
- production_company + movie_production_company

Author: Jeffrey | Course: Online Data Mining - Amsterdam UAS.
"""

# I use json to parse JSON-LD blocks (structured data) if they exist.
import json

# I use re for regex patterns like year extraction.
import re

# I use zlib to generate stable numeric IDs from strings (slug, username, etc.).
import zlib

# I use datetime to store scraped_at timestamps.
from datetime import datetime

# typing helps keep the code readable.
from typing import Any, Dict, List, Optional, Tuple

# Scrapy is our crawling framework.
import scrapy

# Selector is used when working with parts of HTML.
from scrapy import Selector

# These item classes do NOT exist yet in your repo, we will add them in items.py next.
# For now I import them as if they exist, because this spider is designed for the ERD.
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
    # Spider name for running: scrapy crawl metacritic_scraper
    name = "metacritic_scraper"

    # Allowed domain for safety.
    allowed_domains = ["metacritic.com"]

    # Custom headers so we look like a normal browser.
    custom_settings = {
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        },
    }

    def __init__(
        self,
        # Limit movies for testing, example: -a max_movies=10
        max_movies: int = 1000,
        # Limit browse pages, example: -a max_pages=2
        max_pages: int = 50,
        # Limit review pages, example: -a max_review_pages=2
        max_review_pages: int = 5,
        # Limit reviews per movie per type (critic and user), example: -a max_reviews_per_movie=20
        max_reviews_per_movie: int = 100,
        # Seed URLs for quick testing, comma-separated.
        seed_urls: str = "",
        # Enable Playwright on detail pages if blocked, example: -a use_playwright_on_detail=true
        use_playwright_on_detail: str = "false",
        *args,
        **kwargs,
    ):
        # Call parent constructor.
        super().__init__(*args, **kwargs)

        # Convert args to ints (Scrapy passes them as strings).
        self.max_movies = int(max_movies)

        # Convert browse page limit to int.
        self.max_pages = int(max_pages)

        # Convert review page limit to int.
        self.max_review_pages = int(max_review_pages)

        # Convert max reviews to int.
        self.max_reviews_per_movie = int(max_reviews_per_movie)

        # Parse seed_urls into a list.
        self.seed_urls = [u.strip() for u in (seed_urls or "").split(",") if u.strip()]

        # Parse playwright flag into bool.
        self.use_playwright_on_detail = str(use_playwright_on_detail).lower() == "true"

        # Track how many movies we scraped.
        self.movies_scraped = 0

        # Track which slugs we already visited.
        self.seen_slugs = set()

        # Track seen usernames so we do not re-yield users too often.
        self.seen_usernames = set()

        # Track seen publications so we do not re-yield them too often.
        self.seen_publications = set()

        # Track seen award org names.
        self.seen_award_orgs = set()

        # Track seen production companies.
        self.seen_production_companies = set()

        # Track seen persons.
        self.seen_persons = set()

    def start_requests(self):
        # If seed urls are provided, scrape those first.
        if self.seed_urls:
            # Loop seed urls but respect max_movies.
            for url in self.seed_urls[: self.max_movies]:
                # Extract slug so we can dedupe.
                slug = self._extract_slug_from_url(url)

                # If slug exists, mark it as seen.
                if slug:
                    self.seen_slugs.add(slug)

                # Request movie page.
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_movie,
                    meta=self._detail_meta(),
                    priority=50,
                )

            # Stop here to avoid browsing.
            return

        # If no seed urls, start from browse pages.
        base_url = "https://www.metacritic.com/browse/movie/?page={}"

        # Loop through browse pages.
        for page in range(0, self.max_pages):
            # Request browse page (usually needs Playwright).
            yield scrapy.Request(
                url=base_url.format(page),
                callback=self.parse_browse,
                meta={
                    "playwright": True,
                    "playwright_include_page": False,
                    "playwright_page_goto_kwargs": {
                        "wait_until": "domcontentloaded",
                        "timeout": 30000,
                    },
                },
                priority=10,
            )

    def parse_browse(self, response):
        # Stop if we reached our movie limit.
        if self.movies_scraped >= self.max_movies:
            return

        # Collect movie links that look like /movie/<slug>
        hrefs = response.css('a[href^="/movie/"]::attr(href)').getall() or []

        # Keep links list.
        movie_links = []

        # Loop all hrefs.
        for href in hrefs:
            # Skip empty href.
            if not href:
                continue

            # Only keep direct movie paths, not review pages.
            if re.match(r"^/movie/[^/]+/?$", href):
                movie_links.append(href)

        # Deduplicate movie links.
        unique_links = []

        # Local dedupe set.
        local_seen = set()

        # Dedupe while keeping order.
        for link in movie_links:
            if link in local_seen:
                continue
            local_seen.add(link)
            unique_links.append(link)

        # Follow each unique link.
        for link in unique_links:
            # Stop when max reached.
            if self.movies_scraped >= self.max_movies:
                break

            # Extract slug.
            slug = self._extract_slug_from_path(link)

            # Skip if no slug.
            if not slug:
                continue

            # Skip if already visited.
            if slug in self.seen_slugs:
                continue

            # Mark as seen.
            self.seen_slugs.add(slug)

            # Increase counter.
            self.movies_scraped += 1

            # Request movie page.
            yield scrapy.Request(
                url=response.urljoin(link),
                callback=self.parse_movie,
                meta=self._detail_meta(),
                priority=30,
            )

    def parse_movie(self, response):
        # Extract slug from url.
        slug = self._extract_slug_from_url(response.url)

        # Stop if slug missing.
        if not slug:
            return

        # Create a stable numeric movie_id from slug.
        movie_id = self._stable_id_from_text(f"movie:{slug}")

        # Extract JSON-LD for stable structured data if present.
        ld_movie = self._extract_movie_from_jsonld(response)

        # Extract title from JSON-LD if possible.
        title_on_site = self._safe_strip((ld_movie or {}).get("name"))

        # Fallback title from h1.
        if not title_on_site:
            title_on_site = self._safe_strip(response.css("h1::text").get())

        # Extract year (or release year).
        year = self._extract_year(response, ld_movie)

        # Extract release date from details if possible.
        release_date = self._extract_detail_value(response, "Release Date")

        # Extract runtime minutes from JSON-LD or details.
        runtime_minutes = self._extract_runtime_minutes(response, ld_movie)

        # Extract content rating from details.
        content_rating = self._extract_detail_value(response, "Rating")

        # Extract summary/description.
        summary = self._extract_summary(response, ld_movie)

        # Extract Metascore (0-100) and critic review count if possible.
        metascore = self._extract_number_near_label(response, "Metascore", max_value=100)

        # Extract critic review count.
        critic_review_count = self._extract_count_near_label(response, "Critic Reviews")

        # Extract user score (0-10) and user rating count if possible.
        user_score = self._extract_float_near_label(response, "User Score", max_value=10.0)

        # Extract user rating count.
        user_rating_count = self._extract_count_near_label(response, "User Ratings")

        # Extract score distribution (positive/mixed/negative) if present.
        critic_pos, critic_mix, critic_neg = self._extract_distribution_counts(response, "Critic")
        user_pos, user_mix, user_neg = self._extract_distribution_counts(response, "User")

        # Yield metacritic_movie record (this matches ERD metacritic_movie).
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

        # Yield metacritic_score_summary record (this matches ERD metacritic_score_summary).
        yield MetacriticScoreSummaryItem(
            movie_id=movie_id,
            critic_positive_count=critic_pos,
            critic_mixed_count=critic_mix,
            critic_negative_count=critic_neg,
            user_positive_count=user_pos,
            user_mixed_count=user_mix,
            user_negative_count=user_neg,
            scraped_at=datetime.now().isoformat(),
        )

        # Extract and yield cast and crew (person + movie_person_role).
        self._yield_people_and_roles(response, movie_id)

        # Extract and yield production companies (production_company + movie_production_company).
        self._yield_production_companies(response, movie_id)

        # Extract and yield awards (award_org + movie_award_summary).
        self._yield_awards(response, movie_id)

        # Build critic and user review base urls.
        critic_base = f"https://www.metacritic.com/movie/{slug}/critic-reviews/"
        user_base = f"https://www.metacritic.com/movie/{slug}/user-reviews/"

        # Loop critic review pages.
        for p in range(0, self.max_review_pages):
            # Build critic page url.
            url = critic_base if p == 0 else f"{critic_base}?page={p}"

            # Request critic review page.
            yield scrapy.Request(
                url=url,
                callback=self.parse_critic_reviews,
                meta={"movie_id": movie_id, "page": p},
                priority=40,
            )

        # Loop user review pages.
        for p in range(0, self.max_review_pages):
            # Build user page url.
            url = user_base if p == 0 else f"{user_base}?page={p}"

            # Request user review page.
            yield scrapy.Request(
                url=url,
                callback=self.parse_user_reviews,
                meta={"movie_id": movie_id, "page": p},
                priority=40,
            )

    def parse_critic_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Counter for review limit.
        count = 0

        # Get review blocks.
        blocks = self._iter_review_blocks(response)

        # Loop through blocks.
        for block in blocks:
            # Stop if limit reached.
            if count >= self.max_reviews_per_movie:
                break

            # Extract publication name (often exists on critic reviews).
            publication_name = self._safe_strip(
                block.xpath('.//a[contains(@href, "/publication/")]/text()').get()
            )

            # Extract publication url.
            publication_url = block.xpath('.//a[contains(@href, "/publication/")]/@href').get()

            # Convert publication url to full url.
            publication_url_full = response.urljoin(publication_url) if publication_url else None

            # Yield publication if we have a name and we did not yield it before.
            if publication_name and publication_name.lower() not in self.seen_publications:
                self.seen_publications.add(publication_name.lower())

                # Create publication_id.
                publication_id = self._stable_id_from_text(f"publication:{publication_name.lower()}")

                # Yield metacritic_publication item.
                yield MetacriticPublicationItem(
                    publication_id=publication_id,
                    name=publication_name,
                    publication_url=publication_url_full,
                )
            else:
                # Still compute publication_id for linking if name exists.
                publication_id = (
                    self._stable_id_from_text(f"publication:{publication_name.lower()}")
                    if publication_name
                    else None
                )

            # Extract critic name if available (sometimes it says "By X").
            critic_name = self._extract_by_author(block)

            # Extract score (0-100).
            score = self._extract_first_int_in_block(block, max_value=100)

            # Extract date.
            review_date = self._extract_review_date(block)

            # Extract excerpt (short text).
            excerpt = self._extract_review_text(block)

            # Extract link to full review if exists.
            full_review_url = block.xpath('.//a[contains(translate(., "READ", "read"), "read")]/@href').get()
            full_review_url = response.urljoin(full_review_url) if full_review_url else None

            # Yield critic review if we have something meaningful.
            if publication_id or critic_name or excerpt:
                count += 1

                # Yield metacritic_critic_review item.
                yield MetacriticCriticReviewItem(
                    critic_review_id=self._stable_id_from_text(
                        f"critic_review:{movie_id}:{response.url}:{count}"
                    ),
                    movie_id=movie_id,
                    publication_id=publication_id,
                    critic_name=critic_name,
                    score=score,
                    review_date=review_date,
                    excerpt=excerpt,
                    full_review_url=full_review_url,
                    scraped_at=datetime.now().isoformat(),
                )

    def parse_user_reviews(self, response):
        # Read movie_id from meta.
        movie_id = response.meta.get("movie_id")

        # Counter for limit.
        count = 0

        # Get review blocks.
        blocks = self._iter_review_blocks(response)

        # Loop blocks.
        for block in blocks:
            # Stop if limit reached.
            if count >= self.max_reviews_per_movie:
                break

            # Extract username from /user/ link.
            username = self._safe_strip(block.xpath('.//a[contains(@href, "/user/")]/text()').get())

            # Extract user profile url.
            profile_url = block.xpath('.//a[contains(@href, "/user/")]/@href').get()
            profile_url_full = response.urljoin(profile_url) if profile_url else None

            # If username exists, yield metacritic_user (deduped).
            if username and username.lower() not in self.seen_usernames:
                self.seen_usernames.add(username.lower())

                # Create metacritic_user_id.
                metacritic_user_id = self._stable_id_from_text(f"user:{username.lower()}")

                # Yield metacritic_user item.
                yield MetacriticUserItem(
                    metacritic_user_id=metacritic_user_id,
                    username=username,
                    profile_url=profile_url_full,
                    scraped_at=datetime.now().isoformat(),
                )
            else:
                # Compute user id anyway for linking.
                metacritic_user_id = (
                    self._stable_id_from_text(f"user:{username.lower()}")
                    if username
                    else None
                )

            # Extract user score (0-10) from block.
            score = self._extract_first_int_in_block(block, max_value=10)

            # Extract date.
            review_date = self._extract_review_date(block)

            # Extract review text.
            review_text = self._extract_review_text(block)

            # Extract helpful and unhelpful counts (if present).
            helpful_count, unhelpful_count = self._extract_helpful_counts(block)

            # Yield user review row.
            if metacritic_user_id or review_text:
                count += 1

                # Yield metacritic_user_review item.
                yield MetacriticUserReviewItem(
                    user_review_id=self._stable_id_from_text(
                        f"user_review:{movie_id}:{metacritic_user_id}:{response.url}:{count}"
                    ),
                    movie_id=movie_id,
                    metacritic_user_id=metacritic_user_id,
                    score=score,
                    review_date=review_date,
                    review_text=review_text,
                    helpful_count=helpful_count,
                    unhelpful_count=unhelpful_count,
                    scraped_at=datetime.now().isoformat(),
                )

    # -------------------------
    # Meta helpers
    # -------------------------

    def _detail_meta(self) -> Dict[str, Any]:
        # If we do not want Playwright on detail, return empty.
        if not self.use_playwright_on_detail:
            return {}

        # Otherwise return Playwright meta.
        return {
            "playwright": True,
            "playwright_include_page": False,
            "playwright_page_goto_kwargs": {
                "wait_until": "domcontentloaded",
                "timeout": 30000,
            },
        }

    # -------------------------
    # Yield helpers for ERD entities
    # -------------------------

    def _yield_people_and_roles(self, response, movie_id: int):
        # Directors from details.
        directors_text = self._extract_detail_value(response, "Director")

        # Writers from details.
        writers_text = self._extract_detail_value(response, "Writer")

        # Cast from /person/ links (best-effort).
        cast_names = self._extract_person_names_from_links(response)

        # If directors exist, split by commas.
        directors = [x.strip() for x in (directors_text or "").split(",") if x.strip()]

        # If writers exist, split by commas.
        writers = [x.strip() for x in (writers_text or "").split(",") if x.strip()]

        # Yield directors as persons and roles.
        for name in directors:
            self._yield_person_and_role(movie_id, name, "director", character_name=None, billing_order=None)

        # Yield writers as persons and roles.
        for name in writers:
            self._yield_person_and_role(movie_id, name, "writer", character_name=None, billing_order=None)

        # Yield cast as persons and roles.
        for idx, name in enumerate(cast_names[:15], start=1):
            self._yield_person_and_role(movie_id, name, "actor", character_name=None, billing_order=idx)

    def _yield_person_and_role(
        self,
        movie_id: int,
        person_name: str,
        role_type: str,
        character_name: Optional[str],
        billing_order: Optional[int],
    ):
        # Normalize the name for IDs.
        key_name = person_name.strip().lower()

        # Skip empty.
        if not key_name:
            return

        # Create person_id.
        person_id = self._stable_id_from_text(f"person:{key_name}")

        # If not seen, yield person.
        if key_name not in self.seen_persons:
            self.seen_persons.add(key_name)

            # Yield person item.
            yield_item = PersonItem(
                person_id=person_id,
                name=person_name.strip(),
                metacritic_person_url=None,
                scraped_at=datetime.now().isoformat(),
            )

            # Yield to Scrapy output.
            self.crawler.engine.slot.scheduler.enqueue_request  # dummy reference to keep linters quiet
            yield yield_item

        # Yield role link (movie_person_role).
        yield_role = MoviePersonRoleItem(
            movie_person_role_id=self._stable_id_from_text(f"mpr:{movie_id}:{person_id}:{role_type}"),
            movie_id=movie_id,
            person_id=person_id,
            role_type=role_type,
            character_name=character_name,
            billing_order=billing_order,
            scraped_at=datetime.now().isoformat(),
        )

        # Yield role to output.
        yield yield_role

    def _yield_production_companies(self, response, movie_id: int):
        # Try to read production company field.
        prod_text = self._extract_detail_value(response, "Production")

        # Fallback label.
        if not prod_text:
            prod_text = self._extract_detail_value(response, "Production Company")

        # If still missing, stop.
        if not prod_text:
            return

        # Split companies by comma.
        companies = [x.strip() for x in prod_text.split(",") if x.strip()]

        # Loop companies.
        for name in companies:
            # Normalize.
            key_name = name.lower()

            # Create id.
            production_company_id = self._stable_id_from_text(f"prodco:{key_name}")

            # Yield production company if new.
            if key_name not in self.seen_production_companies:
                self.seen_production_companies.add(key_name)

                # Yield production_company item.
                yield ProductionCompanyItem(
                    production_company_id=production_company_id,
                    name=name,
                    scraped_at=datetime.now().isoformat(),
                )

            # Yield link table row.
            yield MovieProductionCompanyItem(
                movie_id=movie_id,
                production_company_id=production_company_id,
                scraped_at=datetime.now().isoformat(),
            )

    def _yield_awards(self, response, movie_id: int):
        # Best-effort: look for awards keywords in page text.
        # Metacritic sometimes shows awards section, but markup can vary a lot.

        # Get all text from page.
        text = " ".join(response.css("body *::text").getall() or [])

        # Normalize spaces.
        text = re.sub(r"\s+", " ", text)

        # Find patterns like "Oscars: X wins, Y nominations" (best-effort).
        # This is not perfect, but it gives you something to defend and improve.
        award_matches = re.findall(
            r"([A-Za-z0-9 '&-]{3,60}):\s*(\d+)\s*wins?,\s*(\d+)\s*nominations?",
            text,
            flags=re.IGNORECASE,
        )

        # Loop matches.
        for (org_name, wins, noms) in award_matches:
            # Clean org name.
            org_name_clean = org_name.strip()

            # Normalize key.
            key_org = org_name_clean.lower()

            # Skip empty.
            if not key_org:
                continue

            # Create award_org_id.
            award_org_id = self._stable_id_from_text(f"award:{key_org}")

            # Yield award org if new.
            if key_org not in self.seen_award_orgs:
                self.seen_award_orgs.add(key_org)

                # Yield award_org item.
                yield AwardOrgItem(
                    award_org_id=award_org_id,
                    name=org_name_clean,
                    award_org_url=None,
                    scraped_at=datetime.now().isoformat(),
                )

            # Yield movie_award_summary.
            yield MovieAwardSummaryItem(
                movie_award_summary_id=self._stable_id_from_text(f"ma:{movie_id}:{award_org_id}"),
                movie_id=movie_id,
                award_org_id=award_org_id,
                wins=int(wins),
                nominations=int(noms),
                scraped_at=datetime.now().isoformat(),
            )

    # -------------------------
    # Extraction helpers
    # -------------------------

    def _extract_slug_from_path(self, path: str) -> Optional[str]:
        # Match /movie/<slug>/
        m = re.match(r"^/movie/([^/]+)/?$", path or "")
        return m.group(1) if m else None

    def _extract_slug_from_url(self, url: str) -> Optional[str]:
        # Match /movie/<slug> at end.
        m = re.search(r"/movie/([^/]+)/?$", url or "")
        return m.group(1) if m else None

    def _stable_id_from_text(self, text: str) -> int:
        # Use adler32 for stable checksum.
        checksum = zlib.adler32(text.encode("utf-8"))
        # Add offset to keep positive and large.
        return int(10_000_000_000 + checksum)

    def _extract_movie_from_jsonld(self, response) -> Optional[Dict[str, Any]]:
        # Get JSON-LD scripts.
        scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall() or []

        # Loop scripts.
        for raw in scripts:
            raw = (raw or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            # JSON can be list or dict.
            candidates = data if isinstance(data, list) else [data]

            # Loop candidates.
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue

                # Graph case.
                if "@graph" in obj and isinstance(obj["@graph"], list):
                    for g in obj["@graph"]:
                        if isinstance(g, dict) and g.get("@type") in ("Movie", "Film"):
                            return g

                # Direct movie object.
                if obj.get("@type") in ("Movie", "Film"):
                    return obj

        return None

    def _extract_year(self, response, ld_movie: Optional[Dict[str, Any]]) -> Optional[int]:
        # Try JSON-LD datePublished first.
        if ld_movie:
            dp = ld_movie.get("datePublished")
            if isinstance(dp, str):
                m = re.search(r"(\d{4})", dp)
                if m:
                    return int(m.group(1))

        # Fallback: search header.
        header_text = " ".join(response.css("header *::text").getall() or [])
        m = re.search(r"\b(19\d{2}|20\d{2})\b", header_text)
        return int(m.group(1)) if m else None

    def _extract_runtime_minutes(self, response, ld_movie: Optional[Dict[str, Any]]) -> Optional[int]:
        # JSON-LD duration like PT116M.
        if ld_movie:
            dur = ld_movie.get("duration")
            if isinstance(dur, str):
                m = re.search(r"PT(\d+)M", dur)
                if m:
                    return int(m.group(1))

        # Fallback: try Runtime detail.
        val = self._extract_detail_value(response, "Runtime")
        if not val:
            return None

        # Try "107 minutes".
        m = re.search(r"(\d+)\s*min", val.lower())
        if m:
            return int(m.group(1))

        # Try "1h 47m".
        m = re.search(r"(\d+)\s*h\s*(\d+)\s*m", val.lower())
        if m:
            return int(m.group(1)) * 60 + int(m.group(2))

        return None

    def _extract_summary(self, response, ld_movie: Optional[Dict[str, Any]]) -> Optional[str]:
        # Use JSON-LD description if exists.
        if ld_movie:
            desc = ld_movie.get("description")
            if isinstance(desc, str) and desc.strip():
                return desc.strip()

        # Fallback: description container (best-effort).
        parts = response.css('div[class*="c-productDetails_description"] span::text').getall() or []
        parts = [self._safe_strip(x) for x in parts if self._safe_strip(x)]
        return " ".join(parts).strip() if parts else None

    def _extract_detail_value(self, response, label: str) -> Optional[str]:
        # Find node that contains label text.
        xp = (
            f'//*[self::li or self::div][.//text()[contains(translate(., "{label.upper()}", "{label.lower()}"), "{label.lower()}")]]'
        )
        nodes = response.xpath(xp)

        # Stop if not found.
        if not nodes:
            return None

        # Collect text tokens.
        txt = [self._safe_strip(t) for t in nodes[0].xpath(".//text()").getall() if self._safe_strip(t)]

        # Remove label itself.
        txt = [t for t in txt if t.lower() != label.lower()]

        # Stop if empty.
        if not txt:
            return None

        # Join tokens and normalize whitespace.
        joined = " ".join(txt).strip()
        joined = re.sub(r"\s+", " ", joined)

        return joined[:500] if joined else None

    def _extract_number_near_label(self, response, label: str, max_value: int) -> Optional[int]:
        # Get page text.
        text = " ".join(response.css("body *::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Try to find label then a number.
        m = re.search(rf"{re.escape(label)}.*?\b(\d{{1,3}})\b", text, flags=re.IGNORECASE)
        if not m:
            return None

        # Parse int.
        val = int(m.group(1))

        # Validate range.
        if 0 <= val <= max_value:
            return val

        return None

    def _extract_float_near_label(self, response, label: str, max_value: float) -> Optional[float]:
        # Get page text.
        text = " ".join(response.css("body *::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Try float like 6.7.
        m = re.search(rf"{re.escape(label)}.*?\b(\d{{1,2}}\.\d)\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= max_value:
                return val

        # Try integer like 7.
        m = re.search(rf"{re.escape(label)}.*?\b(\d{{1,2}})\b", text, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0.0 <= val <= max_value:
                return val

        return None

    def _extract_count_near_label(self, response, label: str) -> Optional[int]:
        # Get page text.
        text = " ".join(response.css("body *::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Look for something like "Critic Reviews 45" or "User Ratings 1,234".
        m = re.search(rf"{re.escape(label)}.*?\b([\d,]+)\b", text, flags=re.IGNORECASE)
        if not m:
            return None

        # Remove commas and parse int.
        raw = m.group(1).replace(",", "").strip()

        # Validate digits.
        if raw.isdigit():
            return int(raw)

        return None

    def _extract_distribution_counts(self, response, prefix: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        # Get page text.
        text = " ".join(response.css("body *::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Look for patterns like "Critic Positive 20 Mixed 10 Negative 5" (best-effort).
        m = re.search(
            rf"{prefix}.*?Positive.*?\b(\d+)\b.*?Mixed.*?\b(\d+)\b.*?Negative.*?\b(\d+)\b",
            text,
            flags=re.IGNORECASE,
        )

        # If not found, return None triplet.
        if not m:
            return None, None, None

        # Return parsed ints.
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    def _extract_person_names_from_links(self, response) -> List[str]:
        # Find /person/ links on the page.
        names = response.css('a[href*="/person/"]::text').getall() or []

        # Clean.
        cleaned = [self._safe_strip(n) for n in names if self._safe_strip(n)]

        # Deduplicate.
        out = []
        seen = set()

        for n in cleaned:
            k = n.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(n)

        return out

    def _extract_by_author(self, block: Selector) -> Optional[str]:
        # Get all text in block.
        text = " ".join(block.css("::text").getall() or [])
        text = re.sub(r"\s+", " ", text).strip()

        # Try "By Name".
        m = re.search(r"\bBy\s+([A-Z][A-Za-z .,'-]{2,80})\b", text)
        return m.group(1).strip() if m else None

    def _extract_first_int_in_block(self, block: Selector, max_value: int) -> Optional[int]:
        # Get block text.
        text = " ".join(block.css("::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Find integers.
        candidates = re.findall(r"\b(\d{1,3})\b", text)

        # Loop candidates.
        for c in candidates[:20]:
            val = int(c)

            # Skip years.
            if 1900 <= val <= 2099:
                continue

            # Validate range.
            if 0 <= val <= max_value:
                return val

        return None

    def _extract_review_text(self, block: Selector) -> Optional[str]:
        # Prefer paragraph text.
        parts = block.css("p::text").getall() or []

        # Clean parts.
        parts = [self._safe_strip(p) for p in parts if self._safe_strip(p)]

        # Join if exists.
        if parts:
            return " ".join(parts).strip()[:5000]

        # Fallback to all text in block.
        raw = " ".join([self._safe_strip(t) for t in block.css("::text").getall() if self._safe_strip(t)])
        raw = re.sub(r"\s+", " ", raw).strip()

        return raw[:5000] if raw else None

    def _extract_review_date(self, block: Selector) -> Optional[str]:
        # Get text.
        text = " ".join(block.css("::text").getall() or [])
        text = re.sub(r"\s+", " ", text).strip()

        # Find date like "Jan 17, 2026".
        m = re.search(r"\b([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b", text)

        return m.group(1) if m else None

    def _extract_helpful_counts(self, block: Selector) -> Tuple[Optional[int], Optional[int]]:
        # Get text.
        text = " ".join(block.css("::text").getall() or [])
        text = re.sub(r"\s+", " ", text)

        # Helpful pattern.
        helpful = None
        unhelpful = None

        # Try to find "X found this helpful" type patterns (best-effort).
        m = re.search(r"\b(\d+)\b.*helpful", text, flags=re.IGNORECASE)
        if m:
            helpful = int(m.group(1))

        # Try to find unhelpful pattern.
        m = re.search(r"\b(\d+)\b.*unhelpful", text, flags=re.IGNORECASE)
        if m:
            unhelpful = int(m.group(1))

        return helpful, unhelpful

    def _iter_review_blocks(self, response) -> List[Selector]:
        # Try multiple selectors for review blocks.
        selectors = [
            'div[class*="c-siteReview"]',
            'div[class*="review"]',
            "article",
        ]

        # Return first selector that yields enough blocks.
        for css_sel in selectors:
            blocks = response.css(css_sel)
            if blocks and len(blocks) >= 3:
                return blocks

        return []

    def _safe_strip(self, s: Any) -> Optional[str]:
        # Handle None.
        if s is None:
            return None

        # Convert to str if needed.
        if not isinstance(s, str):
            s = str(s)

        # Strip whitespace.
        s = s.strip()

        # Return only if not empty.
        return s if s else None
