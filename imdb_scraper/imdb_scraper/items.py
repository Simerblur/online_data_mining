# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieItem(scrapy.Item):
    """Item representing a movie from IMDb Top 250."""
    title = scrapy.Field()
    year = scrapy.Field()
    runtime_minutes = scrapy.Field()
    rating = scrapy.Field()
    metascore = scrapy.Field()
    user_score = scrapy.Field()
    budget = scrapy.Field()
    box_office = scrapy.Field()
    plot = scrapy.Field()
    genres = scrapy.Field()
    director = scrapy.Field()
    studio = scrapy.Field()
    imdb_id = scrapy.Field()
    scraped_at = scrapy.Field()
