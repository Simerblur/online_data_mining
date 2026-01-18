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
