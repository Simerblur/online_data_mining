# Movie Scraper

Online Data Mining Project for MDDB 2025-2026 at Amsterdam University of Applied Sciences

## Team Members

| Name | Responsibility |
|------|----------------|
| Juliusz | IMDb scraper: infinite scroll pagination, movie/cast/director/review extraction, normalized 8-table schema, CSV/SQLite pipelines, Scrapy settings optimization (proxy config, Playwright integration, concurrency tuning, timeout handling,)ERD co-creation |  |
| Jeffrey | Metacritic scraper: critic/user reviews, metascores, search-based movie matching, slug conversion, video editing of presentation, ERD co-creation | 
| Lin | Box Office Mojo scraper: financial data extraction, budget/gross revenue collection, IMDB ID format discovery, ERD co-creation |
| Dennis | Rotten Tomatoes scraper: user reviews, expert reviews scraping 

## Key Scraping Strategies

### Anti-Bot Protection (All Scrapers)
- **Bright Data Web Unlocker Proxy**: Routes all requests through rotating residential IPs with US geo-targeting
- **Playwright Browser Automation**: Renders JavaScript-heavy pages in headless Chromium
- **Proxy + Playwright Integration**: Browser launches with proxy credentials for seamless anti-bot bypass

### IMDb Infinite Scroll (Juliusz)
- IMDb's search no longer uses `?start=` pagination - it uses JavaScript infinite scroll
- Solution: Playwright clicks the "50 more" button repeatedly to load all results
- Single page loads 1000+ movies before extracting links, avoiding redirect loops

### URL Format Discovery (Lin)
- Box Office Mojo uses IMDB's `tt` IDs but requires zero-padded 7-digit format
- Movie ID `111161` must become `tt0111161` (not `tt111161`)
- Format: `f"tt{movie_id:07d}"` ensures correct padding

### Metacritic Slug Matching (Jeffrey)
- Reads movies from IMDB database, converts titles to URL slugs
- "The Dark Knight" → `the-dark-knight` → `metacritic.com/movie/the-dark-knight/`
- Falls back to search if direct URL fails

### Performance Optimizations
- **Reviews limited to 4 per movie** (down from 100) - reduces requests by 96%
- **Cast limited to 10 actors** (down from 15) - focuses on main cast
- **Deduplication sets** prevent duplicate actors/directors in junction tables
- **Non-Playwright for detail pages**: Only search pages need JS rendering

### Pipeline & Settings Architecture (Juliusz)
- **Dual export**: Every item saves to both CSV files and normalized SQLite database
- **Stable ID generation**: Uses `zlib.adler32` hash for consistent foreign keys across runs
- **Proxy configuration**: Bright Data Web Unlocker on port 33335 with US geo-targeting
- **Playwright tuning**: 240s navigation timeout, resource blocking (images/fonts/stylesheets)
- **AutoThrottle**: Dynamic delay adjustment (0.25s-5s) based on server response times
- **Retry logic**: Auto-retry on 500/502/503/504/408/429 errors with 2 attempts

------------------
# Business Case & Morge
## To learn more about our business case, we made an entire seperate file consisting out of our business case as Disney, a real scientific research about movie succes and much more! Go check out the assignment_disney_information_business file. 
Here you can read some basic info about our case:

## Business Case

We position ourselves as a movie publishing company that needs to make strategic decisions about which movies to produce. To make informed decisions we need data about what makes movies successful.

## Business Question
**Which factors are associated with box office success of movies, and how can Disney use this information in a structured way to improve portfolio and release decisions?**

## Research Question

What is the relationship between critic reviews, audience reviews, and box office performance?

## Data Sources

| Data | Source | Spider |
|------|--------|--------|
| Movie info, cast, directors | IMDb | `movie_scraper` |
| User reviews | IMDb, Metascores, Rotten Tomatoes | `movie_scraper` |
| Metascores, | Metacritic | `metacritic_scraper` |
| critic reviews, | Metacritic, Rotten tomatoes| `metacritic_scraper, rotten tomatoes scraper` |
| Box office financials | Box Office Mojo | `boxoffice_scraper` |

## Database Schema

### Core Tables (IMDb)
- `movie` - Basic movie info (id, title, year, score, genres)
- `imdb_reviews` - User reviews with scores and text
- `imdb_genres` / `imdb_movie_genres` - Normalized genres
- `imdb_directors` / `imdb_movie_directors` - Directors with IMDB person IDs
- `imdb_actors` / `imdb_movie_cast` - Cast with character names

### Metacritic Tables
- `metacritic_data` - Metascores, user scores, review counts
- `metacritic_critic_reviews` / `metacritic_user_reviews` - Individual reviews
- `metacritic_publications` / `metacritic_users` - Review sources

### Box Office Table
- `box_office_data` - Budget, domestic/international/worldwide gross

### Rotten Tomatoes Table
- `rottentomatoes_critic_reviews` / `rottentomatoes_user_reviews` - User reviews & Critic reviews

## Installation

```bash
python -m venv venv
source venv/bin/activate
pip install scrapy scrapy-playwright playwright
playwright install chromium
```

## Usage

```bash
cd imdb_scraper

# Step 1: Scrape movies from IMDb (required first)
scrapy crawl movie_scraper -a max_movies=1000

# Step 2: Scrape Metacritic data (reads from IMDb database)
scrapy crawl metacritic_scraper

# Step 3: Scrape Box Office data (reads from IMDb database)
scrapy crawl boxoffice_scraper
```

## Output

All data saved to `imdb_scraper/output/`:
- `movies.db` - SQLite database with all tables
- `*.csv` - Individual CSV files per table

## Configuration

Proxy credentials in `settings.py`:
```python
BRIGHTDATA_USER = "brd-customer-hl_xxxxx-zone-group4"
BRIGHTDATA_PASS = "your_password"
BRIGHTDATA_PORT = "33335"  # Web Unlocker
```
