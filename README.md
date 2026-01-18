# Movie Scraper

Online Data Mining Project for MDDB 2025-2026 at Amsterdam University of Applied Sciences

## Team Members

| Name | Responsibility |
|------|----------------|
| Juliusz | IMDb scraper: normalized 8-table database schema, movie_scraper.py spider, items.py definitions, pipelines.py with SQLite storage, review/cast/director extraction |
| Jeffrey | TBD |
| Lin | TBD |

## Business Case

We position ourselves as a movie publishing company that needs to make strategic decisions about which movies to produce. To make informed decisions we need data about what makes movies successful.

## Research Question

What is the relationship between critic reviews, audience reviews, and box office performance?

This question helps the business decide how much to invest in critic marketing versus building audience buzz.

## Data Requirements

To answer our research question we need:

| Data | Source | Purpose |
|------|--------|---------|
| Movie info | IMDb | Basic movie attributes like title, genre, director, runtime |
| Metascores | Metacritic | Aggregated critic opinion |
| User scores | IMDb or Metacritic | Aggregated audience opinion |
| Review text | IMDb | Individual user reviews for sentiment analysis |
| Box office | BoxOfficeMojo | Financial performance to measure success |

## Database Schema

Eight normalized tables store all scraped data.

### Movie Table

| Column | Type | Description |
|--------|------|-------------|
| movie_id | INTEGER | Primary key (IMDb numeric ID) |
| title | VARCHAR 255 | Movie title |
| year | INTEGER | Release year |
| user_score | DECIMAL 3,1 | IMDb rating |
| box_office | BIGINT | Total gross revenue in USD |
| genres | VARCHAR 200 | Comma separated genres |
| scraped_at | DATETIME | Timestamp of scraping |

### IMDb Review Table

| Column | Type | Description |
|--------|------|-------------|
| review_id | INTEGER | Primary key |
| movie_id | INTEGER | Foreign key to movie table |
| author | VARCHAR 150 | Reviewer username |
| score | VARCHAR 150 | Score given by reviewer |
| text | TEXT | Full review text |
| is_critic | BOOLEAN | True for critics, False for users |
| review_date | DATE | When review was posted |
| scraped_at | DATETIME | Timestamp of scraping |

### Genre Table

| Column | Type | Description |
|--------|------|-------------|
| genre_id | INTEGER | Primary key |
| genre | VARCHAR 100 | Genre name (unique) |

### Movie Genre Table (Junction)

| Column | Type | Description |
|--------|------|-------------|
| movie_id | INTEGER | Foreign key to movie table |
| genre_id | INTEGER | Foreign key to genre table |

### Director Table

| Column | Type | Description |
|--------|------|-------------|
| director_id | INTEGER | Primary key |
| name | VARCHAR 255 | Director name |
| imdb_person_id | VARCHAR 20 | IMDb person ID like nm0001104 |

### Movie Director Table (Junction)

| Column | Type | Description |
|--------|------|-------------|
| movie_id | INTEGER | Foreign key to movie table |
| director_id | INTEGER | Foreign key to director table |
| director_order | INTEGER | Order if multiple directors |

### Actor Table

| Column | Type | Description |
|--------|------|-------------|
| actor_id | INTEGER | Primary key |
| name | VARCHAR 255 | Actor name |
| imdb_person_id | VARCHAR 20 | IMDb person ID like nm0000151 |

### Movie Cast Table (Junction)

| Column | Type | Description |
|--------|------|-------------|
| movie_id | INTEGER | Foreign key to movie table |
| actor_id | INTEGER | Foreign key to actor table |
| character_name | VARCHAR 255 | Character played |
| cast_order | INTEGER | Billing order |

## Current Features

Phase 2 completed:

- Scrapes movie details from IMDb Top 250
- Extracts directors with IMDb person IDs
- Extracts top 15 cast members with character names
- Scrapes user reviews (10 per movie by default)
- Normalizes genres into separate table
- Saves data to both CSV and SQLite database
- Respects robots.txt and includes polite crawling delays

## Installation

```bash
python -m venv venv
source venv/bin/activate
pip install scrapy
```

On Windows use venv\Scripts\activate instead.

## Usage

```bash
cd imdb_scraper
scrapy crawl movie_scraper
```

Output files will be created in the output directory:

- movies.csv (movie data only)
- movies.db (all 8 tables)

## Configuration

| Setting | Value |
|---------|-------|
| DOWNLOAD_DELAY | 2 seconds |
| ROBOTSTXT_OBEY | True |
| CONCURRENT_REQUESTS_PER_DOMAIN | 1 |
| max_movies | 5 (change in spider) |
| max_reviews_per_movie | 10 (change in spider) |

To scrape more movies edit imdb_scraper/spiders/movie_scraper.py and change max_movies.

## Project Structure

```
imdb_scraper/
    imdb_scraper/
        items.py         # MovieItem and ReviewItem definitions
        pipelines.py     # CSV and SQLite storage with 8 tables
        settings.py      # Scrapy configuration
        spiders/
            movie_scraper.py  # Main spider
    output/
        movies.csv
        movies.db
    scrapy.cfg
```

## Project Phases

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 | Done | Basic IMDb scraper with CSV and SQLite output |
| Phase 2 | Done | Review scraping, normalized schema with cast/directors |
| Phase 3 | Todo | Integrate Bright Data proxy |
| Phase 4 | Todo | Add Selenium for JavaScript pages |
| Phase 5 | Todo | Add second data source like Metacritic or BoxOfficeMojo |
