# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import csv
import sqlite3
from pathlib import Path

from itemadapter import ItemAdapter


class CsvPipeline:
    """Pipeline to save scraped movies to a CSV file."""

    def __init__(self):
        self.file = None
        self.writer = None
        self.fieldnames = [
            'title', 'year', 'runtime_minutes', 'rating', 'metascore',
            'user_score', 'budget', 'box_office', 'plot', 'genres',
            'director', 'studio', 'imdb_id', 'scraped_at'
        ]

    def open_spider(self, spider):
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        self.file = open(output_dir / 'movies.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def close_spider(self, spider):
        if self.file:
            self.file.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.writer.writerow(adapter.asdict())
        return item


class SqlitePipeline:
    """Pipeline to save scraped movies to a SQLite database."""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def open_spider(self, spider):
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        self.connection = sqlite3.connect(output_dir / 'movies.db')
        self.cursor = self.connection.cursor()
        self._create_table()

    def _create_table(self):
        """Create the movie table with the specified schema."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie (
                movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title VARCHAR(255) NOT NULL,
                year INTEGER,
                runtime_minutes INTEGER,
                rating VARCHAR(10),
                metascore INTEGER,
                user_score DECIMAL,
                budget BIGINT,
                box_office BIGINT,
                plot TEXT,
                genres VARCHAR(200),
                director VARCHAR(150),
                studio VARCHAR(100),
                imdb_id VARCHAR(20),
                scraped_at DATETIME
            )
        ''')
        self.connection.commit()

    def close_spider(self, spider):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.cursor.execute('''
            INSERT INTO movie (
                title, year, runtime_minutes, rating, metascore,
                user_score, budget, box_office, plot, genres,
                director, studio, imdb_id, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            adapter.get('title'),
            adapter.get('year'),
            adapter.get('runtime_minutes'),
            adapter.get('rating'),
            adapter.get('metascore'),
            adapter.get('user_score'),
            adapter.get('budget'),
            adapter.get('box_office'),
            adapter.get('plot'),
            adapter.get('genres'),
            adapter.get('director'),
            adapter.get('studio'),
            adapter.get('imdb_id'),
            adapter.get('scraped_at'),
        ))
        self.connection.commit()
        return item
