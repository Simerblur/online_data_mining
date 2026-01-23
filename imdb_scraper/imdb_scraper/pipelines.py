# Author: Juliusz (IMDb pipelines), Jeffrey (Metacritic pipelines), Lin (Box Office pipelines)
# Online Data Mining - Amsterdam UAS
# Pipelines for storing scraped movie data to CSV and SQLite (IMDb + Metacritic + Box Office).

import csv
import sqlite3
import zlib
from pathlib import Path
from itemadapter import ItemAdapter
from imdb_scraper.items import MovieItem, ReviewItem, BoxOfficeMojoItem, MetacriticMovieItem, MetacriticCriticReviewItem, MetacriticUserReviewItem, MetacriticUserReviewItem


class CsvPipeline:
    """
    Pipeline for saving scraped items to separate CSV files.
    
    Features:
    - Normalizes data into multiple Relational CSVs (movies, casts, directors, etc.)
    - Handles deduplication to avoid writing the same genre/person multiple times.
    - Manages file handles efficiently.
    """

    def __init__(self):
        self.files = {}
        self.writers = {}
        self.seen_relations = set() # Generic set for deduplicating all relations

        # Define CSV Schemas (Table Name -> List of Headers)
        self.schemas = {
            # IMDb tables
            "movie": ["movie_id", "title", "year", "user_score", "box_office", "genres", "release_date", "runtime_minutes", "mpaa_rating", "scraped_at"],
            "imdb_reviews": ["movie_id", "author", "score", "text", "is_critic", "review_date", "scraped_at"],
            "imdb_genres": ["genre_id", "genre"],
            "imdb_movie_genres": ["movie_id", "genre_id"],
            "imdb_directors": ["director_id", "name", "imdb_person_id"],
            "imdb_movie_directors": ["movie_id", "director_id", "director_order"],
            "imdb_actors": ["actor_id", "name", "imdb_person_id"],
            "imdb_movie_cast": ["movie_id", "actor_id", "character_name", "cast_order"],
            "production_companies": ["company_id", "name"],
            "movie_companies": ["movie_id", "company_id"],
            "imdb_writers": ["writer_id", "name", "imdb_person_id"],
            "imdb_movie_writers": ["movie_id", "writer_id"],
            "imdb_composers": ["composer_id", "name", "imdb_person_id"],
            "imdb_movie_composers": ["movie_id", "composer_id"],

            # Metacritic & Box Office
            "metacritic_data": ["movie_id", "metacritic_slug", "metascore", "metacritic_user_score", "critic_review_count", "user_rating_count", "scraped_at"],
            "metacritic_critic_reviews": ["critic_review_id", "movie_id", "publication_name", "critic_name", "score", "review_date", "excerpt", "scraped_at"],
            "metacritic_user_reviews": ["user_review_id", "movie_id", "username", "score", "review_date", "review_text", "scraped_at"],
            "box_office_data": ["movie_id", "production_budget", "domestic_opening", "domestic_total", "international_total", "worldwide_total", "domestic_distributor", "scraped_at"]
        }

    def open_spider(self, spider):
        project_root = Path(__file__).resolve().parent.parent
        output_dir = project_root / "output"
        output_dir.mkdir(exist_ok=True)

        for name, fields in self.schemas.items():
            path = output_dir / f"{name}.csv"
            write_header = not path.exists() or path.stat().st_size == 0
            
            f = open(path, "a", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fields)
            if write_header:
                writer.writeheader()
            
            self.files[name] = f
            self.writers[name] = writer

    def close_spider(self, spider):
        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if isinstance(item, MovieItem):
            self._process_movie(adapter)
        elif isinstance(item, ReviewItem):
            self.writers["imdb_reviews"].writerow(adapter.asdict())
        elif isinstance(item, MetacriticMovieItem):
            self.writers["metacritic_data"].writerow(adapter.asdict())
        elif isinstance(item, MetacriticCriticReviewItem):
            self.writers["metacritic_critic_reviews"].writerow(adapter.asdict())
        elif isinstance(item, MetacriticUserReviewItem):
            self.writers["metacritic_user_reviews"].writerow(adapter.asdict())
        elif isinstance(item, BoxOfficeMojoItem):
            self.writers["box_office_data"].writerow(adapter.asdict())

        return item

    def _process_movie(self, adapter):
        movie_id = adapter.get("movie_id")
        
        # 1. Main Movie Data
        row = {k: adapter.get(k) for k in self.schemas["movie"]}
        self.writers["movie"].writerow(row)
        self.files["movie"].flush()
        
        # 2. Genres
        for genre in adapter.get("genres_list") or []:
            genre_id = self._get_stable_id(f"genre:{genre.lower()}")
            self._write_deduped("imdb_genres", genre_id, {"genre_id": genre_id, "genre": genre})
            self._write_deduped("imdb_movie_genres", (movie_id, genre_id), {"movie_id": movie_id, "genre_id": genre_id})

        # 3. People (Directors, Actors, etc.)
        self._process_people(movie_id, adapter.get("directors"), "imdb_directors", "imdb_movie_directors", "director")
        self._process_people(movie_id, adapter.get("cast"), "imdb_actors", "imdb_movie_cast", "actor")
        
    def _process_people(self, movie_id, people_list, person_table, join_table, role_prefix):
        """Generic helper to process lists of people (cast, crew)."""
        for order, p in enumerate(people_list or [], 1):
            name = p.get("name")
            pid = p.get("imdb_person_id") or f"gen:{self._get_stable_id(name)}"
            numeric_id = self._get_stable_id(pid)

            # Person Table
            self._write_deduped(person_table, pid, {
                f"{role_prefix}_id": numeric_id,
                "name": name,
                "imdb_person_id": pid
            })

            # Junction Table
            join_data = {
                "movie_id": movie_id,
                f"{role_prefix}_id": numeric_id,
            }
            if role_prefix == "director":
                join_data["director_order"] = order
            elif role_prefix == "actor":
                join_data["character_name"] = p.get("character_name")
                join_data["cast_order"] = p.get("cast_order")

            self._write_deduped(join_table, (movie_id, numeric_id), join_data)

    def _write_deduped(self, table, unique_key, data):
        """Writes to CSV only if the key hasn't been seen in this run."""
        full_key = (table, unique_key)
        if full_key not in self.seen_relations:
            self.seen_relations.add(full_key)
            self.writers[table].writerow(data)

    def _get_stable_id(self, key):
        return zlib.adler32(str(key).encode('utf-8')) & 0xffffffff


class SqlitePipeline:
    """
    Pipeline for saving data to a SQLite database.
    Normalizes data and enforces foreign key constraints.
    """

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        project_root = Path(__file__).resolve().parent.parent
        output_dir = project_root / "output"
        output_dir.mkdir(exist_ok=True)

        self.conn = sqlite3.connect(output_dir / "movies.db")
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA foreign_keys = ON")
        self.cur.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def close_spider(self, spider):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def process_item(self, item, spider):
        try:
            adapter = ItemAdapter(item)
            if isinstance(item, MovieItem):
                self._save_movie(adapter)
            elif isinstance(item, ReviewItem):
                self._insert("imdb_review", adapter.asdict())
            elif isinstance(item, MetacriticMovieItem):
                self._insert("metacritic_data", adapter.asdict(), replace=True)
            elif isinstance(item, MetacriticCriticReviewItem):
                self._insert("metacritic_critic_review", adapter.asdict(), replace=True)
            elif isinstance(item, MetacriticUserReviewItem):
                self._insert("metacritic_user_review", adapter.asdict(), replace=True)
            elif isinstance(item, BoxOfficeMojoItem):
                self._insert("box_office_data", adapter.asdict(), replace=True)
        except Exception as e:
            spider.logger.error(f"DB Error: {e}")
        return item

    def _save_movie(self, adapter):
        movie_id = adapter.get("movie_id")
        
        # 1. Save Movie
        movie_data = {k: adapter.get(k) for k in ["movie_id", "title", "year", "user_score", "box_office", "genres", "scraped_at"]}
        self._insert("movie", movie_data, replace=True)

        # 2. Save Genres
        for genre in adapter.get("genres_list") or []:
            genre_id = self._get_or_create("genre", "genre_id", "genre", genre)
            self._insert("movie_genre", {"movie_id": movie_id, "genre_id": genre_id}, ignore=True)

        # 3. Save People
        self._process_people(movie_id, adapter.get("directors"), "director", "movie_director")
        self._process_people(movie_id, adapter.get("cast"), "actor", "movie_cast")
        
        self.conn.commit()

    def _process_people(self, movie_id, people_list, table_name, join_table):
        for order, p in enumerate(people_list or [], 1):
            name = p.get("name")
            pid = p.get("imdb_person_id")
            
            # Get person ID (create if needed)
            person_id = self._get_or_create_person(table_name, f"{table_name}_id", pid, name)
            
            # Insert into junction table
            join_data = {"movie_id": movie_id, f"{table_name}_id": person_id}
            if table_name == "director":
                join_data["director_order"] = order
            elif table_name == "actor":
                join_data["character_name"] = p.get("character_name")
                join_data["cast_order"] = p.get("cast_order")
                
            self._insert(join_table, join_data, ignore=True)

    def _insert(self, table, data, replace=False, ignore=False):
        """Generic insert helper."""
        cols = list(data.keys())
        params = list(data.values())
        verb = "INSERT OR REPLACE" if replace else ("INSERT OR IGNORE" if ignore else "INSERT")
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"{verb} INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        self.cur.execute(sql, params)

    def _get_or_create(self, table, id_col, unique_col, value):
        self.cur.execute(f"SELECT {id_col} FROM {table} WHERE {unique_col} = ?", (value,))
        res = self.cur.fetchone()
        if res: return res[0]
        self._insert(table, {unique_col: value}, ignore=True)
        self.conn.commit()
        return self._get_or_create(table, id_col, unique_col, value)

    def _get_or_create_person(self, table, id_col, pid, name):
        """Specialized get_or_create for people who might not have an IMDb ID."""
        if pid:
            self.cur.execute(f"SELECT {id_col} FROM {table} WHERE imdb_person_id = ?", (pid,))
            res = self.cur.fetchone()
            if res: return res[0]

        data = {"name": name, "imdb_person_id": pid}
        self._insert(table, data, ignore=True)
        self.conn.commit()
        
        # Determine how to fetch back the ID
        if pid:
            query = f"SELECT {id_col} FROM {table} WHERE imdb_person_id = ?"
            arg = pid
        else:
            # Fallback for people without IDs (danger of duplicates if name matches, but acceptable here)
            query = f"SELECT {id_col} FROM {table} WHERE name = ? AND imdb_person_id IS NULL ORDER BY {id_col} DESC LIMIT 1"
            arg = name
            
        self.cur.execute(query, (arg,))
        res = self.cur.fetchone()
        return res[0] if res else self.cur.lastrowid

    def _create_schema(self):
        """Defines the DB schema (IF NOT EXISTS)."""
        self.cur.executescript("""
            CREATE TABLE IF NOT EXISTS movie (movie_id INTEGER PRIMARY KEY, title TEXT, year INTEGER, user_score REAL, box_office INTEGER, genres TEXT, scraped_at TEXT);
            CREATE TABLE IF NOT EXISTS imdb_review (review_id INTEGER PRIMARY KEY, movie_id INTEGER REFERENCES movie, author TEXT, score TEXT, text TEXT, is_critic BOOLEAN, review_date TEXT, scraped_at TEXT);
            CREATE TABLE IF NOT EXISTS genre (genre_id INTEGER PRIMARY KEY, genre TEXT UNIQUE);
            CREATE TABLE IF NOT EXISTS movie_genre (movie_id INTEGER REFERENCES movie, genre_id INTEGER REFERENCES genre, PRIMARY KEY (movie_id, genre_id));
            
            -- People Tables
            CREATE TABLE IF NOT EXISTS director (director_id INTEGER PRIMARY KEY, name TEXT, imdb_person_id TEXT UNIQUE);
            CREATE TABLE IF NOT EXISTS movie_director (movie_id INTEGER REFERENCES movie, director_id INTEGER REFERENCES director, director_order INTEGER, PRIMARY KEY (movie_id, director_id));
            CREATE TABLE IF NOT EXISTS actor (actor_id INTEGER PRIMARY KEY, name TEXT, imdb_person_id TEXT UNIQUE);
            CREATE TABLE IF NOT EXISTS movie_cast (movie_id INTEGER REFERENCES movie, actor_id INTEGER REFERENCES actor, character_name TEXT, cast_order INTEGER, PRIMARY KEY (movie_id, actor_id));

            -- Metacritic
            CREATE TABLE IF NOT EXISTS metacritic_data (movie_id INTEGER PRIMARY KEY REFERENCES movie, metacritic_slug TEXT, metascore INTEGER, metacritic_user_score REAL, critic_review_count INTEGER, user_rating_count INTEGER, scraped_at TEXT);
            CREATE TABLE IF NOT EXISTS metacritic_critic_review (critic_review_id INTEGER PRIMARY KEY, movie_id INTEGER REFERENCES movie, publication_name TEXT, critic_name TEXT, score INTEGER, review_date TEXT, excerpt TEXT, scraped_at TEXT);
            CREATE TABLE IF NOT EXISTS metacritic_user_review (user_review_id INTEGER PRIMARY KEY, movie_id INTEGER REFERENCES movie, username TEXT, score INTEGER, review_date TEXT, review_text TEXT, scraped_at TEXT);
            
            -- Box Office
            CREATE TABLE IF NOT EXISTS box_office_data (movie_id INTEGER PRIMARY KEY REFERENCES movie, production_budget INTEGER, domestic_opening INTEGER, domestic_total INTEGER, international_total INTEGER, worldwide_total INTEGER, domestic_distributor TEXT, scraped_at TEXT);
        """)
        self.conn.commit()


#ROTTEN TOMATOES PIPELINE
import hashlib
import psycopg2
from datetime import datetime
from scrapy.exceptions import DropItem


class RottenTomatoesPipeline:
    """Pipeline to store Rotten Tomatoes data in PostgreSQL"""

    def _init_(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        """Get database config from settings"""
        db_config = {
            'host': crawler.settings.get('DB_HOST', 'localhost'),
            'port': crawler.settings.get('DB_PORT', 5432),
            'database': crawler.settings.get('DB_NAME', 'movies'),
            'user': crawler.settings.get('DB_USER', 'postgres'),
            'password': crawler.settings.get('DB_PASSWORD', ''),
        }
        return cls(db_config)

    def open_spider(self, spider):
        """Open database connection when spider starts"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            spider.logger.info("Database connection established")
            self.create_tables()
        except psycopg2.Error as e:
            spider.logger.error(f"Database connection failed: {e}")
            raise

    def close_spider(self, spider):
        """Close database connection when spider closes"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            spider.logger.info("Database connection closed")

    def create_tables(self):
        """Create tables if they don't exist"""
        
        # Movie table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie (
                movie_id INTEGER PRIMARY KEY,
                title VARCHAR(255),
                year SMALLINT,
                release_date DATE,
                runtime_minutes SMALLINT,
                mpaa_rating VARCHAR(20),
                genres VARCHAR(255),
                summary TEXT,
                production_company VARCHAR(255),
                awards TEXT,
                music_composer VARCHAR(255),
                user_score DECIMAL(3,1),
                scraped_at TIMESTAMP
            )
        """)

        # Rotten Tomatoes critic reviews
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS rottentomatoes_critic_reviews (
                critic_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                publication_name VARCHAR(255),
                critic_name VARCHAR(255),
                score DECIMAL(4,1),
                review_date DATE,
                review_text TEXT,
                scraped_at TIMESTAMP
            )
        """)

        # Rotten Tomatoes user reviews
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS rottentomatoes_userreviews (
                user_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                user_name VARCHAR(255),
                score DECIMAL(3,1),
                review_text TEXT,
                review_date DATE,
                scraped_at TIMESTAMP
            )
        """)

        self.conn.commit()

    def process_item(self, item, spider):
        """Route item to appropriate handler based on type"""
        item_type = item.get('type')

        if item_type == 'movie':
            return self.process_movie(item, spider)
        elif item_type == 'critic_review':
            return self.process_critic_review(item, spider)
        elif item_type == 'user_review':
            return self.process_user_review(item, spider)
        else:
            raise DropItem(f"Unknown item type: {item_type}")

    def process_movie(self, item, spider):
        """Process and insert movie data"""
        data = item.get('data', {})
        rt_data = item.get('rt_data', {})

        # Generate numeric movie_id from slug
        movie_id = self.generate_id(data.get('movie_id'))

        try:
            # Insert or update movie
            self.cursor.execute("""
                INSERT INTO movie (
                    movie_id, title, year, release_date, runtime_minutes,
                    mpaa_rating, genres, summary, production_company,
                    awards, music_composer, user_score, scraped_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (movie_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    year = EXCLUDED.year,
                    runtime_minutes = EXCLUDED.runtime_minutes,
                    mpaa_rating = EXCLUDED.mpaa_rating,
                    genres = EXCLUDED.genres,
                    summary = EXCLUDED.summary,
                    user_score = EXCLUDED.user_score,
                    scraped_at = EXCLUDED.scraped_at
            """, (
                movie_id,
                data.get('title'),
                data.get('year'),
                data.get('release_date'),
                data.get('runtime_minutes'),
                data.get('mpaa_rating'),
                data.get('genres'),
                data.get('summary'),
                data.get('production_company'),
                data.get('awards'),
                data.get('music_composer'),
                data.get('user_score'),
                data.get('scraped_at')
            ))

            self.conn.commit()
            spider.logger.info(f"Inserted/Updated movie: {data.get('title')} (ID: {movie_id})")

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Error inserting movie: {e}")
            raise DropItem(f"Database error: {e}")

        return item

    def process_critic_review(self, item, spider):
        """Process and insert critic review"""
        data = item.get('data', {})

        # Generate IDs
        movie_id = self.generate_id(data.get('movie_id'))
        
        # Generate review_id from content if not provided
        if data.get('critic_review_id'):
            review_id = self.generate_id(str(data['critic_review_id']))
        else:
            # Create unique ID from movie, critic, publication, and review text
            unique_string = f"{data.get('movie_id')}{data.get('critic_name')}{data.get('publication_name')}_{data.get('review_text', '')[:100]}"
            review_id = self.generate_id(unique_string)

        try:
            self.cursor.execute("""
                INSERT INTO rottentomatoes_critic_reviews (
                    critic_review_id, movie_id, publication_name, critic_name,
                    score, review_date, review_text, scraped_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (critic_review_id) DO NOTHING
            """, (
                review_id,
                movie_id,
                data.get('publication_name'),
                data.get('critic_name'),
                data.get('score'),
                self.parse_date(data.get('review_date')),
                data.get('review_text'),
                data.get('scraped_at')
            ))

            self.conn.commit()
            spider.logger.debug(f"Inserted critic review (ID: {review_id})")

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Error inserting critic review: {e}")
            # Don't raise DropItem for reviews - continue scraping

        return item

    def process_user_review(self, item, spider):
        """Process and insert user review"""
        data = item.get('data', {})

        # Generate IDs
        movie_id = self.generate_id(data.get('movie_id'))
        
        # Generate review_id from content
        if data.get('user_review_id'):
            review_id = self.generate_id(str(data['user_review_id']))
        else:
            # Create unique ID from movie, user, and review text
            unique_string = f"{data.get('movie_id')}{data.get('user_name')}{data.get('review_text', '')[:100]}_{data.get('review_date')}"
            review_id = self.generate_id(unique_string)

        try:
            self.cursor.execute("""
                INSERT INTO rottentomatoes_userreviews (
                    user_review_id, movie_id, user_name, score,
                    review_text, review_date, scraped_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_review_id) DO NOTHING
            """, (
                review_id,
                movie_id,
                data.get('user_name'),
                data.get('score'),
                data.get('review_text'),
                self.parse_date(data.get('review_date')),
                data.get('scraped_at')
            ))

            self.conn.commit()
            spider.logger.debug(f"Inserted user review (ID: {review_id})")

        except psycopg2.Error as e:
            self.conn.rollback()
            spider.logger.error(f"Error inserting user review: {e}")
            # Don't raise DropItem for reviews - continue scraping

        return item

    @staticmethod
    def generate_id(text):
        """Generate a numeric ID from text using hash"""
        if not text:
            return None
        # Use hash and convert to positive integer within PostgreSQL INT range
        hash_obj = hashlib.md5(str(text).encode())
        return int(hash_obj.hexdigest()[:8], 16) % 2147483647

    @staticmethod
    def parse_date(date_string):
        """Parse date string to DATE format"""
        if not date_string:
            return None
        
        # Common RT date formats
        formats = [
            '%Y-%m-%d',
            '%b %d, %Y',  # Jan 15, 2024
            '%B %d, %Y',  # January 15, 2024
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt).date()
            except (ValueError, AttributeError):
                continue
        
        return None


class DuplicateFilterPipeline:
    """Filter duplicate items based on IDs"""

    def _init_(self):
        self.seen_movies = set()
        self.seen_critic_reviews = set()
        self.seen_user_reviews = set()

    def process_item(self, item, spider):
        item_type = item.get('type')
        
        if item_type == 'movie':
            movie_id = item.get('data', {}).get('movie_id')
            if movie_id in self.seen_movies:
                raise DropItem(f"Duplicate movie: {movie_id}")
            self.seen_movies.add(movie_id)
            
        elif item_type == 'critic_review':
            # Create unique key from review data
            data = item.get('data', {})
            key = f"{data.get('movie_id')}{data.get('critic_name')}{data.get('publication_name')}"
            if key in self.seen_critic_reviews:
                raise DropItem(f"Duplicate critic review: {key}")
            self.seen_critic_reviews.add(key)
            
        elif item_type == 'user_review':
            # Create unique key from review data
            data = item.get('data', {})
            key = f"{data.get('movie_id')}{data.get('user_name')}{data.get('review_text', '')[:50]}"
            if key in self.seen_user_reviews:
                raise DropItem(f"Duplicate user review: {key}")
            self.seen_user_reviews.add(key)
        
        return item