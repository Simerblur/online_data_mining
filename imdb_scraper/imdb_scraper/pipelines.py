"""Pipelines for storing scraped movie data to CSV and SQLite (IMDb + Metacritic)."""

import csv
import sqlite3
from pathlib import Path

from itemadapter import ItemAdapter

# IMDb items
from imdb_scraper.items import MovieItem, ReviewItem

# Metacritic (ERD) items
from imdb_scraper.items import (
    MetacriticMovieItem,
    MetacriticScoreSummaryItem,
    MetacriticUserItem,
    MetacriticUserReviewItem,
    MetacriticPublicationItem,
    MetacriticCriticReviewItem,
    PersonItem,
    MoviePersonRoleItem,
    AwardOrgItem,
    MovieAwardSummaryItem,
    ProductionCompanyItem,
    MovieProductionCompanyItem,
)


# ---------------------------------------------------------
# CSV PIPELINE (optional, still useful for quick debug)
# ---------------------------------------------------------

class CsvPipeline:
    """Save IMDB movies to CSV file (and Metacritic movies to a separate CSV)."""

    def __init__(self):
        self.file_imdb = None
        self.writer_imdb = None

        self.file_meta = None
        self.writer_meta = None

    def open_spider(self, spider):
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # IMDb movies CSV
        imdb_path = output_dir / "movies_imdb.csv"
        self.file_imdb = open(imdb_path, "a", newline="", encoding="utf-8")
        imdb_fields = ["movie_id", "title", "year", "user_score", "box_office", "genres", "scraped_at"]
        self.writer_imdb = csv.DictWriter(self.file_imdb, fieldnames=imdb_fields)

        if not imdb_path.exists() or imdb_path.stat().st_size == 0:
            self.writer_imdb.writeheader()

        # Metacritic data CSV (extends movie table)
        meta_path = output_dir / "metacritic_data.csv"
        self.file_meta = open(meta_path, "a", newline="", encoding="utf-8")
        meta_fields = [
            "movie_id",  # FK to movie table
            "metacritic_slug",
            "metascore",
            "metacritic_user_score",
            "critic_review_count",
            "user_rating_count",
            "scraped_at",
        ]
        self.writer_meta = csv.DictWriter(self.file_meta, fieldnames=meta_fields)

        if not meta_path.exists() or meta_path.stat().st_size == 0:
            self.writer_meta.writeheader()

    def close_spider(self, spider):
        if self.file_imdb:
            self.file_imdb.close()
        if self.file_meta:
            self.file_meta.close()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Write IMDb MovieItem
        if isinstance(item, MovieItem):
            self.writer_imdb.writerow({
                "movie_id": adapter.get("movie_id"),
                "title": adapter.get("title"),
                "year": adapter.get("year"),
                "user_score": adapter.get("user_score"),
                "box_office": adapter.get("box_office"),
                "genres": adapter.get("genres"),
                "scraped_at": adapter.get("scraped_at"),
            })

        # Write Metacritic data (linked to movie via movie_id FK)
        if isinstance(item, MetacriticMovieItem):
            self.writer_meta.writerow({
                "movie_id": adapter.get("movie_id"),
                "metacritic_slug": adapter.get("metacritic_slug"),
                "metascore": adapter.get("metascore"),
                "metacritic_user_score": adapter.get("user_score"),
                "critic_review_count": adapter.get("critic_review_count"),
                "user_rating_count": adapter.get("user_rating_count"),
                "scraped_at": adapter.get("scraped_at"),
            })

        return item


# ---------------------------------------------------------
# SQLITE PIPELINE (normalized DB)
# ---------------------------------------------------------

class SqlitePipeline:
    """Save data to SQLite with normalized tables (IMDb + Metacritic ERD tables)."""

    def __init__(self):
        self.conn = None
        self.cur = None

    def open_spider(self, spider):
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        self.conn = sqlite3.connect(output_dir / "movies.db")
        self.cur = self.conn.cursor()

        # Enable foreign keys
        self.cur.execute("PRAGMA foreign_keys = ON")

        # Create tables
        self._create_tables()

    def close_spider(self, spider):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    # -------------------------
    # Create all tables
    # -------------------------

    def _create_tables(self):
        self.cur.executescript("""
            -- =========================================================
            -- IMDb TABLES (existing structure)
            -- =========================================================

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

            -- =========================================================
            -- METACRITIC DATA (extends movie table, FK to movie.movie_id)
            -- =========================================================

            -- Metacritic-specific data for movies (1:1 with movie table)
            CREATE TABLE IF NOT EXISTS metacritic_data (
                movie_id INTEGER PRIMARY KEY REFERENCES movie(movie_id),
                metacritic_url TEXT,
                metacritic_slug VARCHAR(255),
                title_on_metacritic VARCHAR(255),
                metascore INTEGER,
                metacritic_user_score DECIMAL(3,1),
                critic_review_count INTEGER,
                user_rating_count INTEGER,
                content_rating VARCHAR(50),
                runtime_minutes INTEGER,
                summary TEXT,
                scraped_at DATETIME NOT NULL
            );

            -- Score breakdown (positive/mixed/negative counts)
            CREATE TABLE IF NOT EXISTS metacritic_score_summary (
                movie_id INTEGER PRIMARY KEY REFERENCES movie(movie_id),
                critic_positive_count INTEGER,
                critic_mixed_count INTEGER,
                critic_negative_count INTEGER,
                user_positive_count INTEGER,
                user_mixed_count INTEGER,
                user_negative_count INTEGER,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metacritic_user (
                metacritic_user_id INTEGER PRIMARY KEY,
                username VARCHAR(255),
                profile_url TEXT,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metacritic_user_review (
                user_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                metacritic_user_id INTEGER REFERENCES metacritic_user(metacritic_user_id),
                score INTEGER,
                review_date VARCHAR(50),
                review_text TEXT,
                helpful_count INTEGER,
                unhelpful_count INTEGER,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS metacritic_publication (
                publication_id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                publication_url TEXT
            );

            CREATE TABLE IF NOT EXISTS metacritic_critic_review (
                critic_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                publication_id INTEGER REFERENCES metacritic_publication(publication_id),
                critic_name VARCHAR(255),
                score INTEGER,
                review_date VARCHAR(50),
                excerpt TEXT,
                full_review_url TEXT,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS person (
                person_id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                metacritic_person_url TEXT,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movie_person_role (
                movie_person_role_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                person_id INTEGER REFERENCES person(person_id),
                role_type VARCHAR(50),
                character_name VARCHAR(255),
                billing_order INTEGER,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS production_company (
                production_company_id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movie_production_company (
                movie_id INTEGER REFERENCES movie(movie_id),
                production_company_id INTEGER REFERENCES production_company(production_company_id),
                scraped_at DATETIME NOT NULL,
                PRIMARY KEY (movie_id, production_company_id)
            );

            CREATE TABLE IF NOT EXISTS award_org (
                award_org_id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                award_org_url TEXT,
                scraped_at DATETIME NOT NULL
            );

            CREATE TABLE IF NOT EXISTS movie_award_summary (
                movie_award_summary_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                award_org_id INTEGER REFERENCES award_org(award_org_id),
                wins INTEGER,
                nominations INTEGER,
                scraped_at DATETIME NOT NULL
            );
        """)

        self.conn.commit()

    # -------------------------
    # Process item router
    # -------------------------

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # IMDb
        if isinstance(item, MovieItem):
            self._save_imdb_movie(adapter)
        elif isinstance(item, ReviewItem):
            self._save_imdb_review(adapter)

        # Metacritic
        elif isinstance(item, MetacriticMovieItem):
            self._save_metacritic_movie(adapter)
        elif isinstance(item, MetacriticScoreSummaryItem):
            self._save_metacritic_score_summary(adapter)
        elif isinstance(item, MetacriticUserItem):
            self._save_metacritic_user(adapter)
        elif isinstance(item, MetacriticUserReviewItem):
            self._save_metacritic_user_review(adapter)
        elif isinstance(item, MetacriticPublicationItem):
            self._save_metacritic_publication(adapter)
        elif isinstance(item, MetacriticCriticReviewItem):
            self._save_metacritic_critic_review(adapter)
        elif isinstance(item, PersonItem):
            self._save_person(adapter)
        elif isinstance(item, MoviePersonRoleItem):
            self._save_movie_person_role(adapter)
        elif isinstance(item, ProductionCompanyItem):
            self._save_production_company(adapter)
        elif isinstance(item, MovieProductionCompanyItem):
            self._save_movie_production_company(adapter)
        elif isinstance(item, AwardOrgItem):
            self._save_award_org(adapter)
        elif isinstance(item, MovieAwardSummaryItem):
            self._save_movie_award_summary(adapter)

        return item

    # -------------------------
    # IMDb save methods (existing)
    # -------------------------

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

    # -------------------------
    # Metacritic save methods
    # -------------------------

    def _save_metacritic_movie(self, adapter):
        """Save Metacritic data to metacritic_data table (FK to movie table)."""
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_data (
                movie_id, metacritic_url, metacritic_slug, title_on_metacritic,
                metascore, metacritic_user_score, critic_review_count, user_rating_count,
                content_rating, runtime_minutes, summary, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("metacritic_url"),
            adapter.get("metacritic_slug"),
            adapter.get("title_on_site"),
            adapter.get("metascore"),
            adapter.get("user_score"),
            adapter.get("critic_review_count"),
            adapter.get("user_rating_count"),
            adapter.get("content_rating"),
            adapter.get("runtime_minutes"),
            adapter.get("summary"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_score_summary(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_score_summary (
                movie_id, critic_positive_count, critic_mixed_count, critic_negative_count,
                user_positive_count, user_mixed_count, user_negative_count, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("critic_positive_count"),
            adapter.get("critic_mixed_count"),
            adapter.get("critic_negative_count"),
            adapter.get("user_positive_count"),
            adapter.get("user_mixed_count"),
            adapter.get("user_negative_count"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_user(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_user (
                metacritic_user_id, username, profile_url, scraped_at
            )
            VALUES (?, ?, ?, ?)
        """, (
            adapter.get("metacritic_user_id"),
            adapter.get("username"),
            adapter.get("profile_url"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_user_review(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_user_review (
                user_review_id, movie_id, metacritic_user_id, score, review_date,
                review_text, helpful_count, unhelpful_count, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("user_review_id"),
            adapter.get("movie_id"),
            adapter.get("metacritic_user_id"),
            adapter.get("score"),
            adapter.get("review_date"),
            adapter.get("review_text"),
            adapter.get("helpful_count"),
            adapter.get("unhelpful_count"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_metacritic_publication(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_publication (
                publication_id, name, publication_url
            )
            VALUES (?, ?, ?)
        """, (
            adapter.get("publication_id"),
            adapter.get("name"),
            adapter.get("publication_url"),
        ))
        self.conn.commit()

    def _save_metacritic_critic_review(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO metacritic_critic_review (
                critic_review_id, movie_id, publication_id, critic_name, score,
                review_date, excerpt, full_review_url, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("critic_review_id"),
            adapter.get("movie_id"),
            adapter.get("publication_id"),
            adapter.get("critic_name"),
            adapter.get("score"),
            adapter.get("review_date"),
            adapter.get("excerpt"),
            adapter.get("full_review_url"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_person(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO person (
                person_id, name, metacritic_person_url, scraped_at
            )
            VALUES (?, ?, ?, ?)
        """, (
            adapter.get("person_id"),
            adapter.get("name"),
            adapter.get("metacritic_person_url"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_movie_person_role(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO movie_person_role (
                movie_person_role_id, movie_id, person_id, role_type,
                character_name, billing_order, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_person_role_id"),
            adapter.get("movie_id"),
            adapter.get("person_id"),
            adapter.get("role_type"),
            adapter.get("character_name"),
            adapter.get("billing_order"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_production_company(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO production_company (
                production_company_id, name, scraped_at
            )
            VALUES (?, ?, ?)
        """, (
            adapter.get("production_company_id"),
            adapter.get("name"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_movie_production_company(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO movie_production_company (
                movie_id, production_company_id, scraped_at
            )
            VALUES (?, ?, ?)
        """, (
            adapter.get("movie_id"),
            adapter.get("production_company_id"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_award_org(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO award_org (
                award_org_id, name, award_org_url, scraped_at
            )
            VALUES (?, ?, ?, ?)
        """, (
            adapter.get("award_org_id"),
            adapter.get("name"),
            adapter.get("award_org_url"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()

    def _save_movie_award_summary(self, adapter):
        self.cur.execute("""
            INSERT OR REPLACE INTO movie_award_summary (
                movie_award_summary_id, movie_id, award_org_id,
                wins, nominations, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            adapter.get("movie_award_summary_id"),
            adapter.get("movie_id"),
            adapter.get("award_org_id"),
            adapter.get("wins"),
            adapter.get("nominations"),
            adapter.get("scraped_at"),
        ))
        self.conn.commit()
