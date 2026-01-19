# scrapy item definitions for imdb scraper

import scrapy


# movie data from imdb
class MovieItem(scrapy.Item):
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


# user review from imdb
class ReviewItem(scrapy.Item):
    movie_id = scrapy.Field()      # foreign key to movie
    author = scrapy.Field()        # reviewer username
    score = scrapy.Field()         # rating given
    text = scrapy.Field()          # review content
    is_critic = scrapy.Field()     # critic or user
    review_date = scrapy.Field()   # date posted
    scraped_at = scrapy.Field()    # scrape timestamp



# Metacritic (ERD tables)
# -------------------------

# metacritic_movie
class MetacriticMovieItem(scrapy.Item):
    movie_id = scrapy.Field()              # PK/FK to movie table
    metacritic_url = scrapy.Field()        # url to movie page
    metacritic_slug = scrapy.Field()       # slug in url
    title_on_site = scrapy.Field()         # title as shown on Metacritic
    release_date = scrapy.Field()          # release date
    runtime_minutes = scrapy.Field()       # runtime in minutes
    content_rating = scrapy.Field()        # PG-13, R, etc.
    summary = scrapy.Field()               # movie summary text
    metascore = scrapy.Field()             # 0-100 critic metascore
    critic_review_count = scrapy.Field()   # number of critic reviews
    user_score = scrapy.Field()            # 0-10 user score
    user_rating_count = scrapy.Field()     # number of user ratings
    scraped_at = scrapy.Field()            # timestamp


# metacritic_score_summary
class MetacriticScoreSummaryItem(scrapy.Item):
    movie_id = scrapy.Field()              # FK to movie table
    critic_positive_count = scrapy.Field() # count of positive critic reviews
    critic_mixed_count = scrapy.Field()    # count of mixed critic reviews
    critic_negative_count = scrapy.Field() # count of negative critic reviews
    user_positive_count = scrapy.Field()   # count of positive user reviews
    user_mixed_count = scrapy.Field()      # count of mixed user reviews
    user_negative_count = scrapy.Field()   # count of negative user reviews
    scraped_at = scrapy.Field()            # timestamp


# metacritic_user
class MetacriticUserItem(scrapy.Item):
    metacritic_user_id = scrapy.Field()    # PK
    username = scrapy.Field()              # username
    profile_url = scrapy.Field()           # user profile url
    scraped_at = scrapy.Field()            # timestamp


# metacritic_user_review
class MetacriticUserReviewItem(scrapy.Item):
    user_review_id = scrapy.Field()        # PK
    movie_id = scrapy.Field()              # FK to movie
    metacritic_user_id = scrapy.Field()    # FK to metacritic_user
    score = scrapy.Field()                 # 0-10 score
    review_date = scrapy.Field()           # date
    review_text = scrapy.Field()           # long text
    helpful_count = scrapy.Field()         # how many found helpful
    unhelpful_count = scrapy.Field()       # how many found unhelpful
    scraped_at = scrapy.Field()            # timestamp


# metacritic_publication
class MetacriticPublicationItem(scrapy.Item):
    publication_id = scrapy.Field()        # PK
    name = scrapy.Field()                  # publication name
    publication_url = scrapy.Field()       # publication page url


# metacritic_critic_review
class MetacriticCriticReviewItem(scrapy.Item):
    critic_review_id = scrapy.Field()      # PK
    movie_id = scrapy.Field()              # FK to movie
    publication_id = scrapy.Field()        # FK to metacritic_publication
    critic_name = scrapy.Field()           # critic name (if available)
    score = scrapy.Field()                 # 0-100 score
    review_date = scrapy.Field()           # date
    excerpt = scrapy.Field()               # excerpt text
    full_review_url = scrapy.Field()       # link to full review if exists
    scraped_at = scrapy.Field()            # timestamp


# person of metacritic
class PersonItem(scrapy.Item):
    person_id = scrapy.Field()             # PK
    name = scrapy.Field()                  # person name
    metacritic_person_url = scrapy.Field() # optional url if you can get it
    scraped_at = scrapy.Field()            # timestamp


# movie_person_role of metacritic
class MoviePersonRoleItem(scrapy.Item):
    movie_person_role_id = scrapy.Field()  # PK
    movie_id = scrapy.Field()              # FK to movie
    person_id = scrapy.Field()             # FK to person
    role_type = scrapy.Field()             # director, actor, writer, etc.
    character_name = scrapy.Field()        # for actors (optional)
    billing_order = scrapy.Field()         # cast order (optional)
    scraped_at = scrapy.Field()            # timestamp


# Matches table: award_org
class AwardOrgItem(scrapy.Item):
    award_org_id = scrapy.Field()          # PK
    name = scrapy.Field()                  # award organization name
    award_org_url = scrapy.Field()         # optional url
    scraped_at = scrapy.Field()            # timestamp


# movie_award_summary of metacritic
class MovieAwardSummaryItem(scrapy.Item):
    movie_award_summary_id = scrapy.Field()# PK
    movie_id = scrapy.Field()              # FK to movie
    award_org_id = scrapy.Field()          # FK to award_org
    wins = scrapy.Field()                  # wins count
    nominations = scrapy.Field()           # nominations count
    scraped_at = scrapy.Field()            # timestamp


# production_company of metacritic
class ProductionCompanyItem(scrapy.Item):
    production_company_id = scrapy.Field() # PK
    name = scrapy.Field()                  # company name
    scraped_at = scrapy.Field()            # timestamp


# movie_production_company of metacritic
class MovieProductionCompanyItem(scrapy.Item):
    movie_id = scrapy.Field()              # PK/FK to movie
    production_company_id = scrapy.Field() # PK/FK to production_company
    scraped_at = scrapy.Field()            # timestamp
