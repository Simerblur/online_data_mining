# Author: Juliusz (IMDb pipelines), Jeffrey (Metacritic pipelines), Lin (Box Office pipelines)
# Online Data Mining - Amsterdam UAS
# Pipelines for storing scraped movie data to CSV and SQLite (IMDb + Metacritic + Box Office).

import csv
import sqlite3
from pathlib import Path

from itemadapter import ItemAdapter

# IMDb items
from imdb_scraper.items import MovieItem, ReviewItem, BoxOfficeMojoItem

# Metacritic items (simplified - only reviews)
from imdb_scraper.items import (
    MetacriticMovieItem,
    MetacriticCriticReviewItem,
    MetacriticUserReviewItem,
)


class CsvPipeline:
    # Save items to separate CSV files

    def __init__(self):
        # File handles and writers
        self.files = {}
        self.writers = {}

        # Deduplication sets 
        self.seen_genres = set()
        self.seen_directors = set()
        self.seen_actors = set()
        # Junction table deduplication (movie_id, person_id pairs)
        self.seen_movie_directors = set()
        self.seen_movie_actors = set()
        self.seen_movie_genres = set()

        # Define schemas
        self.schemas = {
            # IMDb tables
            "movie": ["movie_id", "title", "year", "user_score", "box_office", "genres", 
                      "release_date", "runtime_minutes", "mpaa_rating", "scraped_at"], # Added new columns
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

            # Metacritic tables (simplified - only reviews)
            "metacritic_data": ["movie_id", "metacritic_slug", "metascore", "metacritic_user_score",
                              "critic_review_count", "user_rating_count", "scraped_at"],
            "metacritic_critic_reviews": ["critic_review_id", "movie_id", "publication_name", "critic_name",
                                        "score", "review_date", "excerpt", "scraped_at"],
            "metacritic_user_reviews": ["user_review_id", "movie_id", "username", "score",
                                      "review_date", "review_text", "scraped_at"],
            # Box Office Mojo table
            "box_office_data": ["movie_id", "production_budget", "domestic_opening", "domestic_total",
                               "international_total", "worldwide_total", "domestic_distributor", "scraped_at"]
        }

    def open_spider(self, spider):
        # Always output to PROJECT_ROOT/output
        # This assumes pipelines.py is in <root>/imdb_scraper/pipelines.py
        project_root = Path(__file__).resolve().parent.parent
        output_dir = project_root / "output"
        output_dir.mkdir(exist_ok=True)

        for name, fields in self.schemas.items():
            path = output_dir / f"{name}.csv"
            # Check if file exists and has content BEFORE opening
            write_header = not path.exists() or path.stat().st_size == 0
            # Open in append mode
            f = open(path, "a", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fields)
            # Write header if file was new/empty
            if write_header:
                writer.writeheader()
            self.files[name] = f
            self.writers[name] = writer

    def close_spider(self, spider):
        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # IMDb Items
        if isinstance(item, MovieItem):
            movie_id = adapter.get("movie_id")
            
            # 1. movie
            self.writers["movie"].writerow({
                "movie_id": movie_id,
                "title": adapter.get("title"),
                "year": adapter.get("year"),
                "user_score": adapter.get("user_score"),
                "box_office": adapter.get("box_office"),
                "genres": adapter.get("genres"),
                "scraped_at": adapter.get("scraped_at"),
                "release_date": adapter.get("release_date"),
                "runtime_minutes": adapter.get("runtime_minutes"),
                "mpaa_rating": adapter.get("mpaa_rating"),
            })
            self.files["movie"].flush()
            
            # 2. genres & imdb_movie_genres
            for genre in adapter.get("genres_list") or []:
                genre_id = self._get_stable_id(f"genre:{genre.lower()}")
                if genre_id not in self.seen_genres:
                    self.seen_genres.add(genre_id)
                    self.writers["imdb_genres"].writerow({
                        "genre_id": genre_id,
                        "genre": genre
                    })
                # Dedupe movie-genre junction
                movie_genre_key = (movie_id, genre_id)
                if movie_genre_key not in self.seen_movie_genres:
                    self.seen_movie_genres.add(movie_genre_key)
                    self.writers["imdb_movie_genres"].writerow({
                        "movie_id": movie_id,
                        "genre_id": genre_id
                    })
                
            # 3. directors & imdb_movie_directors
            for order, d in enumerate(adapter.get("directors") or [], 1):
                name = d.get("name")
                pid = d.get("imdb_person_id")
                # Create ID if missing (unlikely for imdb)
                if not pid:
                     pid = f"gen:{self._get_stable_id(name)}"

                director_numeric_id = self._get_stable_id(pid)

                # Check duplication by ID for directors table
                if pid not in self.seen_directors:
                    self.seen_directors.add(pid)
                    self.writers["imdb_directors"].writerow({
                        "director_id": director_numeric_id,
                        "name": name,
                        "imdb_person_id": pid
                    })

                # Dedupe movie-director junction
                movie_director_key = (movie_id, director_numeric_id)
                if movie_director_key not in self.seen_movie_directors:
                    self.seen_movie_directors.add(movie_director_key)
                    self.writers["imdb_movie_directors"].writerow({
                        "movie_id": movie_id,
                        "director_id": director_numeric_id,
                        "director_order": order
                    })
                
            # 4. cast & imdb_movie_cast
            for c in adapter.get("cast") or []:
                name = c.get("name")
                pid = c.get("imdb_person_id")
                character = c.get("character_name")
                order = c.get("cast_order")

                if not pid:
                    pid = f"gen:{self._get_stable_id(name)}"

                actor_numeric_id = self._get_stable_id(pid)

                # Dedupe actors table
                if pid not in self.seen_actors:
                    self.seen_actors.add(pid)
                    self.writers["imdb_actors"].writerow({
                        "actor_id": actor_numeric_id,
                        "name": name,
                        "imdb_person_id": pid
                    })

                # Dedupe movie-actor junction
                movie_actor_key = (movie_id, actor_numeric_id)
                if movie_actor_key not in self.seen_movie_actors:
                    self.seen_movie_actors.add(movie_actor_key)
                    self.writers["imdb_movie_cast"].writerow({
                        "movie_id": movie_id,
                        "actor_id": actor_numeric_id,
                        "character_name": character,
                        "cast_order": order
                    })

        elif isinstance(item, ReviewItem):
            self.writers["imdb_reviews"].writerow(adapter.asdict())

        # Metacritic Items (simplified)
        elif isinstance(item, MetacriticMovieItem):
            self.writers["metacritic_data"].writerow({
                "movie_id": adapter.get("movie_id"),
                "metacritic_slug": adapter.get("metacritic_slug"),
                "metascore": adapter.get("metascore"),
                "metacritic_user_score": adapter.get("user_score"),
                "critic_review_count": adapter.get("critic_review_count"),
                "user_rating_count": adapter.get("user_rating_count"),
                "scraped_at": adapter.get("scraped_at"),
            })

        elif isinstance(item, MetacriticCriticReviewItem):
            self.writers["metacritic_critic_reviews"].writerow(adapter.asdict())

        elif isinstance(item, MetacriticUserReviewItem):
            self.writers["metacritic_user_reviews"].writerow(adapter.asdict())

        # Box Office Mojo Items
        elif isinstance(item, BoxOfficeMojoItem):
            self.writers["box_office_data"].writerow(adapter.asdict())

        return item

    def _get_stable_id(self, key):
        # Generate stable numeric ID from string key (similar to hash).
        import zlib
        return zlib.adler32(str(key).encode('utf-8')) & 0xffffffff


class SqlitePipeline:
    # Save data to SQLite with normalized tables (IMDb + Metacritic ERD tables).

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        # Always output to PROJECT_ROOT/output
        project_root = Path(__file__).resolve().parent.parent
        output_dir = project_root / "output"
        output_dir.mkdir(exist_ok=True)

        self.conn = sqlite3.connect(output_dir / "movies.db")
        self.cur = self.conn.cursor()

        # Enable foreign keys and WAL mode for concurrency
        self.cur.execute("PRAGMA foreign_keys = ON")
        self.cur.execute("PRAGMA journal_mode = WAL")

        # Create tables
        self._create_tables()

    def close_spider(self, spider):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def _create_tables(self):
        self.cur.executescript("""
            -- IMDb TABLES

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

            -- METACRITIC TABLES (simplified - only reviews)

            CREATE TABLE IF NOT EXISTS metacritic_data (
                movie_id INTEGER PRIMARY KEY REFERENCES movie(movie_id),
                metacritic_slug VARCHAR(255),
                metascore INTEGER,
                metacritic_user_score DECIMAL(3,1),
                critic_review_count INTEGER,
                user_rating_count INTEGER,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metacritic_critic_review (
                critic_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                publication_name VARCHAR(255),
                critic_name VARCHAR(255),
                score INTEGER,
                review_date VARCHAR(50),
                excerpt TEXT,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metacritic_user_review (
                user_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                username VARCHAR(255),
                score INTEGER,
                review_date VARCHAR(50),
                review_text TEXT,
                scraped_at DATETIME NOT NULL
            );

            -- BOX OFFICE MOJO TABLE

            CREATE TABLE IF NOT EXISTS box_office_data (
                movie_id INTEGER PRIMARY KEY REFERENCES movie(movie_id),
                production_budget BIGINT,
                domestic_opening BIGINT,
                domestic_total BIGINT,
                international_total BIGINT,
                worldwide_total BIGINT,
                domestic_distributor VARCHAR(255),
                scraped_at DATETIME NOT NULL
            );
        """)

        self.conn.commit()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # IMDb
        if isinstance(item, MovieItem):
            try:
                self._save_imdb_movie(adapter)
            except Exception as e:
                spider.logger.error(f"Error in SqlitePipeline._save_imdb_movie: {e}")
        elif isinstance(item, ReviewItem):
            try:
                self._save_imdb_review(adapter)
            except Exception as e:
                spider.logger.error(f"Error in SqlitePipeline._save_imdb_review: {e}")

        # Metacritic (simplified)
        elif isinstance(item, MetacriticMovieItem):
            self._save_metacritic_movie(adapter)
        elif isinstance(item, MetacriticCriticReviewItem):
            self._save_metacritic_critic_review(adapter)
        elif isinstance(item, MetacriticUserReviewItem):
            self._save_metacritic_user_review(adapter)

        # Box Office Mojo
        elif isinstance(item, BoxOfficeMojoItem):
            self._save_box_office_data(adapter)

        return item

    def _save_imdb_movie(self, adapter):
        movie_id = adapter.get("movie_id")

        # Insert movie row
        self.cur.execute("""
            INSERT OR REPLACE INTO movie (movie_id, title, year, user_score, box_office, genres, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            movie_id,
            adapter.get("title"),
            adapter.get("year"),
            adapter.get("user_score"),
            adapter.get("box_office"),
            adapter.get("genres"),
            adapter.get("scraped_at"),
        ))

        # Insert genres into normalized tables
        for genre in adapter.get("genres_list") or []:
            genre_id = self._get_or_create_genre(genre)
            self.cur.execute("INSERT OR IGNORE INTO movie_genre VALUES (?, ?)", (movie_id, genre_id))

        # Insert directors
        for order, d in enumerate(adapter.get("directors") or [], 1):
            director_id = self._get_or_create_person_table("director", "director_id", "imdb_person_id", d.get("imdb_person_id"), d.get("name"))
            self.cur.execute("INSERT OR IGNORE INTO movie_director VALUES (?, ?, ?)", (movie_id, director_id, order))

        # Insert cast
        for c in adapter.get("cast") or []:
            actor_id = self._get_or_create_person_table("actor", "actor_id", "imdb_person_id", c.get("imdb_person_id"), c.get("name"))
            self.cur.execute("INSERT OR IGNORE INTO movie_cast VALUES (?, ?, ?, ?)", (movie_id, actor_id, c.get("character_name"), c.get("cast_order")))

        self.conn.commit()

    def _save_imdb_review(self, adapter):
        self.cur.execute("""
            INSERT INTO imdb_review (movie_id, author, score, text, is_critic, review_date, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("author"),
            adapter.get("score"),
            adapter.get("text"),
            adapter.get("is_critic"),
            adapter.get("review_date"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _get_or_create_genre(self, genre_name):
        if not genre_name:
            return None

        self.cur.execute("SELECT genre_id FROM genre WHERE genre = ?", (genre_name,))
        row = self.cur.fetchone()
        if row:
            return row[0]

        self.cur.execute("INSERT OR IGNORE INTO genre (genre) VALUES (?)", (genre_name,))
        self.conn.commit()

        self.cur.execute("SELECT genre_id FROM genre WHERE genre = ?", (genre_name,))
        return self.cur.fetchone()[0]

    def _get_or_create_person_table(self, table, id_col, unique_col, unique_val, name):
        if unique_val:
            self.cur.execute(f"SELECT {id_col} FROM {table} WHERE {unique_col} = ?", (unique_val,))
            row = self.cur.fetchone()
            if row:
                return row[0]

        self.cur.execute(f"INSERT OR IGNORE INTO {table} (name, {unique_col}) VALUES (?, ?)", (name, unique_val))
        self.conn.commit()

        if unique_val:
            self.cur.execute(f"SELECT {id_col} FROM {table} WHERE {unique_col} = ?", (unique_val,))
            row = self.cur.fetchone()
            if row:
                return row[0]

        # Fallback: return lastrowid (rare case)
        return self.cur.lastrowid

    def _save_metacritic_movie(self, adapter):
        # Save Metacritic data to metacritic_data table.
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_data (
                movie_id, metacritic_slug, metascore, metacritic_user_score,
                critic_review_count, user_rating_count, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("metacritic_slug"),
            adapter.get("metascore"),
            adapter.get("user_score"),
            adapter.get("critic_review_count"),
            adapter.get("user_rating_count"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_critic_review(self, adapter):
        # Save Metacritic critic review.
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_critic_review (
                critic_review_id, movie_id, publication_name, critic_name,
                score, review_date, excerpt, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("critic_review_id"),
            adapter.get("movie_id"),
            adapter.get("publication_name"),
            adapter.get("critic_name"),
            adapter.get("score"),
            adapter.get("review_date"),
            adapter.get("excerpt"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_user_review(self, adapter):
        # Save Metacritic user review.
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_user_review (
                user_review_id, movie_id, username, score,
                review_date, review_text, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("user_review_id"),
            adapter.get("movie_id"),
            adapter.get("username"),
            adapter.get("score"),
            adapter.get("review_date"),
            adapter.get("review_text"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_box_office_data(self, adapter):
        # Save Box Office Mojo financial data.
        self.cur.execute("""
            INSERT OR REPLACE INTO box_office_data (
                movie_id, production_budget, domestic_opening,
                domestic_total, international_total, worldwide_total, domestic_distributor, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("production_budget"),
            adapter.get("domestic_opening"),
            adapter.get("domestic_total"),
            adapter.get("international_total"),
            adapter.get("worldwide_total"),
            adapter.get("domestic_distributor"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()
