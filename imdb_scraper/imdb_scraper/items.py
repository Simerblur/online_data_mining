# Author: Juliusz (IMDb items), Jeffrey (Metacritic items), Lin (Box Office items)
# Online Data Mining - Amsterdam UAS

import scrapy

class MovieItem(scrapy.Item):
    # Data model for an IMDb Movie.
    # Maps to the 'movie' table in the database.
    movie_id = scrapy.Field()      # IMDb numeric ID (e.g., 12345 from tt0012345)
    title = scrapy.Field()         # Movie Title
    year = scrapy.Field()          # Release Year
    user_score = scrapy.Field()    # IMDb User Rating (0.0 - 10.0)
    box_office = scrapy.Field()    # Worldwide Gross Revenue (integer)
    genres = scrapy.Field()        # Comma-separated genres string
    genres_list = scrapy.Field()   # List of genres for normalization
    directors = scrapy.Field()     # List of dictionaries: {'name': str, 'imdb_person_id': str}
    cast = scrapy.Field()          # List of dictionaries: {'name': str, 'imdb_person_id': str, 'role': str}
    
    # New Metadata Fields
    release_date = scrapy.Field()          # Full release date string (e.g. "March 24, 1972")
    runtime_minutes = scrapy.Field()       # Duration in minutes (int)
    mpaa_rating = scrapy.Field()           # MPAA rating (e.g. "R", "PG-13")
    production_companies = scrapy.Field()  # List of company names (strings)
    composers = scrapy.Field()             # List of dictionaries: {'name': str, 'imdb_person_id': str}
    writers = scrapy.Field()               # List of dictionaries: {'name': str, 'imdb_person_id': str}
    
    scraped_at = scrapy.Field()    # Timestamp of scraping


class ReviewItem(scrapy.Item):
    # Data model for an IMDb User Review.
    # Maps to the 'imdb_review' table.
    movie_id = scrapy.Field()      # FK to MovieItem
    author = scrapy.Field()        # Reviewer Username
    score = scrapy.Field()         # Rating Score given by user
    text = scrapy.Field()          # Full text of the review
    is_critic = scrapy.Field()     # Boolean (always False for user reviews)
    review_date = scrapy.Field()   # Date string
    scraped_at = scrapy.Field()    # Timestamp


class MetacriticMovieItem(scrapy.Item):
    # Data model for Metacritic movie metadata.
    # Contains aggregate scores and counts.
    movie_id = scrapy.Field()              # FK: Aligns with IMDb Movie ID
    metacritic_url = scrapy.Field()        # URL to Metacritic page
    metacritic_slug = scrapy.Field()       # Slug (e.g., 'the-godfather')
    title_on_site = scrapy.Field()         # Title as it appears on Metacritic
    metascore = scrapy.Field()             # Critic Score (0-100)
    user_score = scrapy.Field()            # User Score (0-10)
    critic_review_count = scrapy.Field()   # Count of critic reviews
    user_rating_count = scrapy.Field()     # Count of user ratings
    scraped_at = scrapy.Field()            # Timestamp


class MetacriticCriticReviewItem(scrapy.Item):
    # Data model for a professional Critic Review on Metacritic.
    critic_review_id = scrapy.Field()      # Unique ID (if available, else hash)
    movie_id = scrapy.Field()              # FK to MovieItem
    publication_name = scrapy.Field()      # Publisher (e.g., "The New York Times")
    critic_name = scrapy.Field()           # Critic Name
    score = scrapy.Field()                 # Score given by critic (0-100)
    review_date = scrapy.Field()           # Date string
    excerpt = scrapy.Field()               # Short snippet of the review
    scraped_at = scrapy.Field()            # Timestamp


class MetacriticUserReviewItem(scrapy.Item):
    # Data model for a User Review on Metacritic.
    user_review_id = scrapy.Field()        # Unique ID
    movie_id = scrapy.Field()              # FK to MovieItem
    username = scrapy.Field()              # User's handle
    score = scrapy.Field()                 # User Score (0-10)
    review_date = scrapy.Field()           # Date string
    review_text = scrapy.Field()           # Full review text
    scraped_at = scrapy.Field()            # Timestamp


class BoxOfficeMojoItem(scrapy.Item):
    # Financial data from Box Office Mojo.
    # Provides detailed budget and revenue breakdown.
    movie_id = scrapy.Field()              # FK to MovieItem
    production_budget = scrapy.Field()     # Production Budget (USD)
    domestic_opening = scrapy.Field()      # Domestic Opening Weekend Revenue
    domestic_total = scrapy.Field()        # Total Domestic Revenue
    international_total = scrapy.Field()   # Total International Revenue
    worldwide_total = scrapy.Field()       # Total Worldwide Revenue
    domestic_distributor = scrapy.Field()  # Domestic Distributor (e.g. "Warner Bros.")
    scraped_at = scrapy.Field()            # Timestamp
