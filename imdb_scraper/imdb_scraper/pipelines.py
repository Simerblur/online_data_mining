# pipelines for storing scraped data

import csv
import sqlite3
from pathlib import Path

from itemadapter import ItemAdapter

from imdb_scraper.items import MovieItem, ReviewItem


# saves movies to csv file
class CsvPipeline:

    def __init__(self):
        self.file = None
        self.writer = None
        self.fieldnames = [
            'movie_id', 'title', 'year', 'user_score', 'box_office',
            'genres', 'scraped_at'
        ]

    def open_spider(self, spider):
        # create output directory
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        self.file = open(output_dir / 'movies.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames)
        self.writer.writeheader()

    def close_spider(self, spider):
        if self.file:
            self.file.close()

    def process_item(self, item, spider):
        # only write movie items
        if isinstance(item, MovieItem):
            adapter = ItemAdapter(item)
            row = {field: adapter.get(field) for field in self.fieldnames}
            self.writer.writerow(row)
        return item


# saves data to sqlite with 8 normalized tables
class SqlitePipeline:

    def __init__(self):
        self.connection = None
        self.cursor = None

    def open_spider(self, spider):
        # create output directory and connect
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        self.connection = sqlite3.connect(output_dir / 'movies.db')
        self.cursor = self.connection.cursor()
        # enable foreign key enforcement
        self.cursor.execute('PRAGMA foreign_keys = ON')
        self._create_tables()

    def _create_tables(self):
        # table 1: movie
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie (
                movie_id INTEGER PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                year INTEGER,
                user_score DECIMAL(3,1),
                box_office BIGINT,
                genres VARCHAR(200),
                scraped_at DATETIME NOT NULL
            )
        ''')

        # table 2: imdb_review
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS imdb_review (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER REFERENCES movie(movie_id),
                author VARCHAR(150),
                score VARCHAR(150),
                text TEXT,
                is_critic BOOLEAN,
                review_date DATE,
                scraped_at DATETIME NOT NULL
            )
        ''')

        # table 3: genre
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS genre (
                genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
                genre VARCHAR(100) UNIQUE NOT NULL
            )
        ''')

        # table 4: movie_genre junction
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_genre (
                movie_id INTEGER REFERENCES movie(movie_id),
                genre_id INTEGER REFERENCES genre(genre_id),
                PRIMARY KEY (movie_id, genre_id)
            )
        ''')

        # table 5: director
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS director (
                director_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                imdb_person_id VARCHAR(20) UNIQUE
            )
        ''')

        # table 6: movie_director junction
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_director (
                movie_id INTEGER REFERENCES movie(movie_id),
                director_id INTEGER REFERENCES director(director_id),
                director_order INTEGER DEFAULT 1,
                PRIMARY KEY (movie_id, director_id)
            )
        ''')

        # table 7: actor
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS actor (
                actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                imdb_person_id VARCHAR(20) UNIQUE
            )
        ''')

        # table 8: movie_cast junction
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_cast (
                movie_id INTEGER REFERENCES movie(movie_id),
                actor_id INTEGER REFERENCES actor(actor_id),
                character_name VARCHAR(255),
                cast_order INTEGER,
                PRIMARY KEY (movie_id, actor_id)
            )
        ''')

        self.connection.commit()

    def close_spider(self, spider):
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def process_item(self, item, spider):
        # route to correct handler
        adapter = ItemAdapter(item)

        if isinstance(item, MovieItem):
            self._process_movie(adapter)
        elif isinstance(item, ReviewItem):
            self._process_review(adapter)

        return item

    def _process_movie(self, adapter):
        movie_id = adapter.get('movie_id')

        # insert movie
        self.cursor.execute('''
            INSERT OR REPLACE INTO movie (
                movie_id, title, year, user_score, box_office, genres, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            movie_id,
            adapter.get('title'),
            adapter.get('year'),
            adapter.get('user_score'),
            adapter.get('box_office'),
            adapter.get('genres'),
            adapter.get('scraped_at'),
        ))

        # insert genres
        genres_list = adapter.get('genres_list') or []
        for genre_name in genres_list:
            genre_id = self._get_or_create_genre(genre_name)
            self._link_movie_genre(movie_id, genre_id)

        # insert directors
        directors = adapter.get('directors') or []
        for order, director_data in enumerate(directors, start=1):
            director_id = self._get_or_create_director(
                director_data.get('name'),
                director_data.get('imdb_person_id')
            )
            self._link_movie_director(movie_id, director_id, order)

        # insert cast
        cast = adapter.get('cast') or []
        for cast_member in cast:
            actor_id = self._get_or_create_actor(
                cast_member.get('name'),
                cast_member.get('imdb_person_id')
            )
            self._link_movie_cast(
                movie_id,
                actor_id,
                cast_member.get('character_name'),
                cast_member.get('cast_order')
            )

        self.connection.commit()

    def _process_review(self, adapter):
        # insert review
        self.cursor.execute('''
            INSERT INTO imdb_review (
                movie_id, author, score, text, is_critic, review_date, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            adapter.get('movie_id'),
            adapter.get('author'),
            adapter.get('score'),
            adapter.get('text'),
            adapter.get('is_critic'),
            adapter.get('review_date'),
            adapter.get('scraped_at'),
        ))
        self.connection.commit()

    def _get_or_create_genre(self, genre_name):
        # check if exists
        self.cursor.execute(
            'SELECT genre_id FROM genre WHERE genre = ?',
            (genre_name,)
        )
        result = self.cursor.fetchone()
        if result:
            return result[0]

        # create new
        self.cursor.execute(
            'INSERT INTO genre (genre) VALUES (?)',
            (genre_name,)
        )
        return self.cursor.lastrowid

    def _get_or_create_director(self, name, imdb_person_id):
        # check if exists
        if imdb_person_id:
            self.cursor.execute(
                'SELECT director_id FROM director WHERE imdb_person_id = ?',
                (imdb_person_id,)
            )
            result = self.cursor.fetchone()
            if result:
                return result[0]

        # create new
        self.cursor.execute(
            'INSERT OR IGNORE INTO director (name, imdb_person_id) VALUES (?, ?)',
            (name, imdb_person_id)
        )
        if self.cursor.lastrowid:
            return self.cursor.lastrowid

        # fetch if duplicate
        self.cursor.execute(
            'SELECT director_id FROM director WHERE imdb_person_id = ?',
            (imdb_person_id,)
        )
        return self.cursor.fetchone()[0]

    def _get_or_create_actor(self, name, imdb_person_id):
        # check if exists
        if imdb_person_id:
            self.cursor.execute(
                'SELECT actor_id FROM actor WHERE imdb_person_id = ?',
                (imdb_person_id,)
            )
            result = self.cursor.fetchone()
            if result:
                return result[0]

        # create new
        self.cursor.execute(
            'INSERT OR IGNORE INTO actor (name, imdb_person_id) VALUES (?, ?)',
            (name, imdb_person_id)
        )
        if self.cursor.lastrowid:
            return self.cursor.lastrowid

        # fetch if duplicate
        self.cursor.execute(
            'SELECT actor_id FROM actor WHERE imdb_person_id = ?',
            (imdb_person_id,)
        )
        return self.cursor.fetchone()[0]

    def _link_movie_genre(self, movie_id, genre_id):
        # link movie to genre
        self.cursor.execute(
            'INSERT OR IGNORE INTO movie_genre (movie_id, genre_id) VALUES (?, ?)',
            (movie_id, genre_id)
        )

    def _link_movie_director(self, movie_id, director_id, order):
        # link movie to director
        self.cursor.execute(
            '''INSERT OR IGNORE INTO movie_director
               (movie_id, director_id, director_order) VALUES (?, ?, ?)''',
            (movie_id, director_id, order)
        )

    def _link_movie_cast(self, movie_id, actor_id, character_name, cast_order):
        # link movie to actor
        self.cursor.execute(
            '''INSERT OR IGNORE INTO movie_cast
               (movie_id, actor_id, character_name, cast_order) VALUES (?, ?, ?, ?)''',
            (movie_id, actor_id, character_name, cast_order)
        )
