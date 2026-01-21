# Author: Juliusz (IMDb items), Jeffrey (Metacritic items), Lin (Box Office items)
# Online Data Mining - Amsterdam UAS

import scrapy


# =============================================================================
# IMDb Items
# =============================================================================

class MovieItem(scrapy.Item):
    """Movie data from IMDB."""
    movie_id = scrapy.Field()      # imdb numeric id
    title = scrapy.Field()         # movie title
    year = scrapy.Field()          # release year
    user_score = scrapy.Field()    # imdb rating
    box_office = scrapy.Field()    # gross revenue
    genres = scrapy.Field()        # comma separated genres
    genres_list = scrapy.Field()   # list for normalization
    directors = scrapy.Field()     # list of director dicts
    cast = scrapy.Field()          # list of actor dicts
    scraped_at = scrapy.Field()    # scrape timestamp


class ReviewItem(scrapy.Item):
    """User review from IMDB."""
    movie_id = scrapy.Field()      # foreign key to movie
    author = scrapy.Field()        # reviewer username
    score = scrapy.Field()         # rating given
    text = scrapy.Field()          # review content
    is_critic = scrapy.Field()     # critic or user
    review_date = scrapy.Field()   # date posted
    scraped_at = scrapy.Field()    # scrape timestamp


# =============================================================================
# Metacritic Items (simplified - only reviews)
# =============================================================================

class MetacriticMovieItem(scrapy.Item):
    """Basic Metacritic movie info with scores (FK to movie table)."""
    movie_id = scrapy.Field()              # FK to movie table
    metacritic_url = scrapy.Field()        # url to movie page
    metacritic_slug = scrapy.Field()       # slug in url
    title_on_site = scrapy.Field()         # title as shown on Metacritic
    metascore = scrapy.Field()             # 0-100 critic metascore
    user_score = scrapy.Field()            # 0-10 user score
    critic_review_count = scrapy.Field()   # number of critic reviews
    user_rating_count = scrapy.Field()     # number of user ratings
    scraped_at = scrapy.Field()            # timestamp


class MetacriticCriticReviewItem(scrapy.Item):
    """Critic review from Metacritic."""
    critic_review_id = scrapy.Field()      # PK
    movie_id = scrapy.Field()              # FK to movie
    publication_name = scrapy.Field()      # publication name (denormalized)
    critic_name = scrapy.Field()           # critic name (if available)
    score = scrapy.Field()                 # 0-100 score
    review_date = scrapy.Field()           # date
    excerpt = scrapy.Field()               # excerpt text
    scraped_at = scrapy.Field()            # timestamp


class MetacriticUserReviewItem(scrapy.Item):
    """User review from Metacritic."""
    user_review_id = scrapy.Field()        # PK
    movie_id = scrapy.Field()              # FK to movie
    username = scrapy.Field()              # username (denormalized)
    score = scrapy.Field()                 # 0-10 score
    review_date = scrapy.Field()           # date
    review_text = scrapy.Field()           # long text
    scraped_at = scrapy.Field()            # timestamp


# =============================================================================
# Box Office Mojo Items
# =============================================================================

class BoxOfficeMojoItem(scrapy.Item):
    """Box Office financial data (FK to movie table)."""
    movie_id = scrapy.Field()              # FK to movie (IMDB tt id as int)
    production_budget = scrapy.Field()     # production budget in dollars
    domestic_opening = scrapy.Field()      # domestic opening weekend
    domestic_total = scrapy.Field()        # domestic total gross
    international_total = scrapy.Field()   # international total gross
    worldwide_total = scrapy.Field()       # worldwide total gross
    scraped_at = scrapy.Field()            # timestamp
