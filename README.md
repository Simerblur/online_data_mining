# Movie Scraper

Online Data Mining Project for MDDB 2025-2026 at Amsterdam University of Applied Sciences

## Team Members

| Name | Responsibility |
|------|----------------|
| Juliusz | TBD |
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
| Review text | Metacritic | Individual critic and user opinions for sentiment analysis |
| Box office | BoxOfficeMojo | Financial performance to measure success |

## Database Schema

Two tables store all scraped data.

### Movie Table

| Column | Type | Description |
|--------|------|-------------|
| movie_id | INTEGER | Primary key |
| title | VARCHAR 255 | Movie title |
| year | INTEGER | Release year |
| runtime_minutes | INTEGER | Duration in minutes |
| rating | VARCHAR 10 | Age rating like PG or R |
| metascore | INTEGER | Metacritic score 0 to 100 |
| user_score | DECIMAL | User rating |
| budget | BIGINT | Production budget in USD |
| box_office | BIGINT | Total gross revenue in USD |
| plot | TEXT | Movie synopsis |
| genres | VARCHAR 200 | Comma separated genres |
| director | VARCHAR 150 | Director name |
| studio | VARCHAR 100 | Production company |
| imdb_id | VARCHAR 20 | IMDb identifier like tt0111161 |
| scraped_at | DATETIME | Timestamp of scraping |

### Review Table

| Column | Type | Description |
|--------|------|-------------|
| review_id | INTEGER | Primary key |
| movie_id | INTEGER | Foreign key to movie table |
| source | VARCHAR 50 | Website where review came from |
| author | VARCHAR 150 | Reviewer name |
| score | INTEGER | Score given by reviewer |
| text | TEXT | Full review text |
| review_date | DATE | When review was posted |
| is_critic | BOOLEAN | True for critics, False for users |
| scraped_at | DATETIME | Timestamp of scraping |

## Current Features

Phase 1 MVP completed:

- Scrapes movie details from IMDb Top 250
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
scrapy crawl imdb_top250
```

Output files will be created in the output directory:

- movies.csv
- movies.db

## Configuration

| Setting | Value |
|---------|-------|
| DOWNLOAD_DELAY | 2 seconds |
| ROBOTSTXT_OBEY | True |
| CONCURRENT_REQUESTS_PER_DOMAIN | 1 |

To scrape all 250 movies edit imdb_scraper/spiders/imdb_top250.py and change max_movies from 5 to 250.

## Project Structure

```
imdb_scraper/
    imdb_scraper/
        items.py
        pipelines.py
        settings.py
        spiders/
            imdb_top250.py
    output/
        movies.csv
        movies.db
    scrapy.cfg
```

## Project Phases

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 | Done | Basic IMDb scraper with CSV and SQLite output |
| Phase 2 | Todo | Add review scraping |
| Phase 3 | Todo | Integrate Bright Data proxy |
| Phase 4 | Todo | Add Selenium for JavaScript pages |
| Phase 5 | Todo | Add second data source like Metacritic or BoxOfficeMojo |
