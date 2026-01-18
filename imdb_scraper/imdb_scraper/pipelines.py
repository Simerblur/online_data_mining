"""Pipelines for storing scraped movie data to CSV and SQLite."""

import csv
import sqlite3
from pathlib import Path

from itemadapter import ItemAdapter
from imdb_scraper.items import MovieItem, ReviewItem


class CsvPipeline:
    """Save movies to CSV file."""

    def __init__(self):
        self.file = None
        self.writer = None

    def open_spider(self, spider):
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        file_path = output_dir / 'movies.csv'

        self.file = open(file_path, 'a', newline='', encoding='utf-8')
        fields = ['movie_id', 'title', 'year', 'user_score', 'box_office', 'genres', 'scraped_at']
        self.writer = csv.DictWriter(self.file, fieldnames=fields)

        if not file_path.exists() or file_path.stat().st_size == 0:
            self.writer.writeheader()

    def close_spider(self, spider):
        if self.file:
            self.file.close()

    def process_item(self, item, spider):
        if isinstance(item, MovieItem):
            adapter = ItemAdapter(item)
            self.writer.writerow({
                'movie_id': adapter.get('movie_id'),
                'title': adapter.get('title'),
                'year': adapter.get('year'),
                'user_score': adapter.get('user_score'),
                'box_office': adapter.get('box_office'),
                'genres': adapter.get('genres'),
                'scraped_at': adapter.get('scraped_at'),
            })
        return item


class SqlitePipeline:
    """Save data to SQLite with normalized tables."""

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        self.conn = sqlite3.connect(output_dir / 'movies.db')
        self.cur = self.conn.cursor()
        self.cur.execute('PRAGMA foreign_keys = ON')
        self._create_tables()

    def _create_tables(self):
        self.cur.executescript('''
            CREATE TABLE IF NOT EXISTS movie (
                movie_id INTEGER PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                year INTEGER,
                user_score DECIMAL(3,1),
                box_office BIGINT,
                genres VARCHAR(200),
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS imdb_review (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                movie_id INTEGER REFERENCES movie(movie_id),
                author VARCHAR(150),
                score VARCHAR(150),
                text TEXT,
                is_critic BOOLEAN,
                review_date DATE,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS genre (
                genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
                genre VARCHAR(100) UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movie_genre (
                movie_id INTEGER REFERENCES movie(movie_id),
                genre_id INTEGER REFERENCES genre(genre_id),
                PRIMARY KEY (movie_id, genre_id)
            );

            CREATE TABLE IF NOT EXISTS director (
                director_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                imdb_person_id VARCHAR(20) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS movie_director (
                movie_id INTEGER REFERENCES movie(movie_id),
                director_id INTEGER REFERENCES director(director_id),
                director_order INTEGER DEFAULT 1,
                PRIMARY KEY (movie_id, director_id)
            );

            CREATE TABLE IF NOT EXISTS actor (
                actor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                imdb_person_id VARCHAR(20) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS movie_cast (
                movie_id INTEGER REFERENCES movie(movie_id),
                actor_id INTEGER REFERENCES actor(actor_id),
                character_name VARCHAR(255),
                cast_order INTEGER,
                PRIMARY KEY (movie_id, actor_id)
            );
        ''')
        self.conn.commit()

    def close_spider(self, spider):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if isinstance(item, MovieItem):
            self._save_movie(adapter)
        elif isinstance(item, ReviewItem):
            self._save_review(adapter)

        return item

    def _save_movie(self, adapter):
        movie_id = adapter.get('movie_id')

        # Insert movie
        self.cur.execute('''
            INSERT OR REPLACE INTO movie (movie_id, title, year, user_score, box_office, genres, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (movie_id, adapter.get('title'), adapter.get('year'), adapter.get('user_score'),
              adapter.get('box_office'), adapter.get('genres'), adapter.get('scraped_at')))

        # Insert genres
        for genre in adapter.get('genres_list') or []:
            genre_id = self._get_or_create('genre', 'genre_id', 'genre', genre)
            self.cur.execute('INSERT OR IGNORE INTO movie_genre VALUES (?, ?)', (movie_id, genre_id))

        # Insert directors
        for order, d in enumerate(adapter.get('directors') or [], 1):
            director_id = self._get_or_create('director', 'director_id', 'imdb_person_id',
                                               d.get('imdb_person_id'), d.get('name'))
            self.cur.execute('INSERT OR IGNORE INTO movie_director VALUES (?, ?, ?)',
                           (movie_id, director_id, order))

        # Insert cast
        for c in adapter.get('cast') or []:
            actor_id = self._get_or_create('actor', 'actor_id', 'imdb_person_id',
                                           c.get('imdb_person_id'), c.get('name'))
            self.cur.execute('INSERT OR IGNORE INTO movie_cast VALUES (?, ?, ?, ?)',
                           (movie_id, actor_id, c.get('character_name'), c.get('cast_order')))

        self.conn.commit()

    def _save_review(self, adapter):
        self.cur.execute('''
            INSERT INTO imdb_review (movie_id, author, score, text, is_critic, review_date, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (adapter.get('movie_id'), adapter.get('author'), adapter.get('score'),
              adapter.get('text'), adapter.get('is_critic'), adapter.get('review_date'),
              adapter.get('scraped_at')))
        self.conn.commit()

    def _get_or_create(self, table, id_col, unique_col, unique_val, name=None):
        """Get existing record ID or create new one."""
        if unique_val:
            self.cur.execute(f'SELECT {id_col} FROM {table} WHERE {unique_col} = ?', (unique_val,))
            result = self.cur.fetchone()
            if result:
                return result[0]

        if table == 'genre':
            self.cur.execute(f'INSERT OR IGNORE INTO {table} ({unique_col}) VALUES (?)', (unique_val,))
        else:
            self.cur.execute(f'INSERT OR IGNORE INTO {table} (name, {unique_col}) VALUES (?, ?)',
                           (name, unique_val))

        if self.cur.lastrowid:
            return self.cur.lastrowid

        self.cur.execute(f'SELECT {id_col} FROM {table} WHERE {unique_col} = ?', (unique_val,))
        return self.cur.fetchone()[0]
