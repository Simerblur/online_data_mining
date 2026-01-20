#!/usr/bin/env python3
"""
Migration script to refactor database schema.

Changes:
- Renames metacritic_movie -> metacritic_data
- Updates column names for clarity
- All Metacritic tables now reference movie.movie_id (not metacritic_movie)

Run from imdb_scraper directory:
    python migrate_db.py
"""

import sqlite3
from pathlib import Path


def migrate_database(db_path: str = "output/movies.db"):
    """Migrate the database to new schema."""
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"Database not found: {db_path}")
        return

    print(f"Migrating database: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Disable foreign keys during migration
    cur.execute("PRAGMA foreign_keys = OFF")

    # --- 1. Migrate metacritic_movie -> metacritic_data ---
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metacritic_movie'")
    if cur.fetchone():
        print("Migrating metacritic_movie -> metacritic_data...")

        cur.execute("""
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
            )
        """)

        cur.execute("""
            INSERT OR REPLACE INTO metacritic_data (
                movie_id, metacritic_url, metacritic_slug, title_on_metacritic,
                metascore, metacritic_user_score, critic_review_count, user_rating_count,
                content_rating, runtime_minutes, summary, scraped_at
            )
            SELECT
                movie_id, metacritic_url, metacritic_slug, title_on_site,
                metascore, user_score, critic_review_count, user_rating_count,
                content_rating, runtime_minutes, summary, scraped_at
            FROM metacritic_movie
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE metacritic_movie")
        print("  Dropped old metacritic_movie table")

    # --- 2. Fix metacritic_user_review FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='metacritic_user_review'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing metacritic_user_review FK...")

        cur.execute("""
            CREATE TABLE metacritic_user_review_new (
                user_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                metacritic_user_id INTEGER REFERENCES metacritic_user(metacritic_user_id),
                score INTEGER,
                review_date VARCHAR(50),
                review_text TEXT,
                helpful_count INTEGER,
                unhelpful_count INTEGER,
                scraped_at DATETIME NOT NULL
            )
        """)

        cur.execute("""
            INSERT INTO metacritic_user_review_new
            SELECT * FROM metacritic_user_review
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE metacritic_user_review")
        cur.execute("ALTER TABLE metacritic_user_review_new RENAME TO metacritic_user_review")
        print("  Recreated table with correct FK")

    # --- 3. Fix metacritic_critic_review FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='metacritic_critic_review'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing metacritic_critic_review FK...")

        cur.execute("""
            CREATE TABLE metacritic_critic_review_new (
                critic_review_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                publication_id INTEGER REFERENCES metacritic_publication(publication_id),
                critic_name VARCHAR(255),
                score INTEGER,
                review_date VARCHAR(50),
                excerpt TEXT,
                full_review_url TEXT,
                scraped_at DATETIME NOT NULL
            )
        """)

        cur.execute("""
            INSERT INTO metacritic_critic_review_new
            SELECT * FROM metacritic_critic_review
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE metacritic_critic_review")
        cur.execute("ALTER TABLE metacritic_critic_review_new RENAME TO metacritic_critic_review")
        print("  Recreated table with correct FK")

    # --- 4. Fix metacritic_score_summary FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='metacritic_score_summary'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing metacritic_score_summary FK...")

        cur.execute("""
            CREATE TABLE metacritic_score_summary_new (
                movie_id INTEGER PRIMARY KEY REFERENCES movie(movie_id),
                critic_positive_count INTEGER,
                critic_mixed_count INTEGER,
                critic_negative_count INTEGER,
                user_positive_count INTEGER,
                user_mixed_count INTEGER,
                user_negative_count INTEGER,
                scraped_at DATETIME NOT NULL
            )
        """)

        cur.execute("""
            INSERT INTO metacritic_score_summary_new
            SELECT * FROM metacritic_score_summary
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE metacritic_score_summary")
        cur.execute("ALTER TABLE metacritic_score_summary_new RENAME TO metacritic_score_summary")
        print("  Recreated table with correct FK")

    # --- 5. Fix movie_person_role FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='movie_person_role'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing movie_person_role FK...")

        cur.execute("""
            CREATE TABLE movie_person_role_new (
                movie_person_role_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                person_id INTEGER REFERENCES person(person_id),
                role_type VARCHAR(50),
                character_name VARCHAR(255),
                billing_order INTEGER,
                scraped_at DATETIME NOT NULL
            )
        """)

        cur.execute("""
            INSERT INTO movie_person_role_new
            SELECT * FROM movie_person_role
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE movie_person_role")
        cur.execute("ALTER TABLE movie_person_role_new RENAME TO movie_person_role")
        print("  Recreated table with correct FK")

    # --- 6. Fix movie_production_company FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='movie_production_company'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing movie_production_company FK...")

        cur.execute("""
            CREATE TABLE movie_production_company_new (
                movie_id INTEGER REFERENCES movie(movie_id),
                production_company_id INTEGER REFERENCES production_company(production_company_id),
                scraped_at DATETIME NOT NULL,
                PRIMARY KEY (movie_id, production_company_id)
            )
        """)

        cur.execute("""
            INSERT INTO movie_production_company_new
            SELECT * FROM movie_production_company
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE movie_production_company")
        cur.execute("ALTER TABLE movie_production_company_new RENAME TO movie_production_company")
        print("  Recreated table with correct FK")

    # --- 7. Fix movie_award_summary FK ---
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='movie_award_summary'")
    row = cur.fetchone()
    if row and 'metacritic_movie' in (row[0] or ''):
        print("Fixing movie_award_summary FK...")

        cur.execute("""
            CREATE TABLE movie_award_summary_new (
                movie_award_summary_id INTEGER PRIMARY KEY,
                movie_id INTEGER REFERENCES movie(movie_id),
                award_org_id INTEGER REFERENCES award_org(award_org_id),
                wins INTEGER,
                nominations INTEGER,
                scraped_at DATETIME NOT NULL
            )
        """)

        cur.execute("""
            INSERT INTO movie_award_summary_new
            SELECT * FROM movie_award_summary
        """)
        print(f"  Migrated {cur.rowcount} rows")

        cur.execute("DROP TABLE movie_award_summary")
        cur.execute("ALTER TABLE movie_award_summary_new RENAME TO movie_award_summary")
        print("  Recreated table with correct FK")

    # Re-enable foreign keys
    cur.execute("PRAGMA foreign_keys = ON")

    conn.commit()
    conn.close()

    print("\nMigration complete!")


def show_schema(db_path: str = "output/movies.db"):
    """Show current database schema."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("\n=== Current Database Schema ===\n")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cur.fetchall()

    for (table_name,) in tables:
        print(f"TABLE: {table_name}")
        cur.execute(f"PRAGMA table_info({table_name})")
        columns = cur.fetchall()
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        print()

    conn.close()


if __name__ == "__main__":
    import sys

    db_path = sys.argv[1] if len(sys.argv) > 1 else "output/movies.db"

    migrate_database(db_path)
    show_schema(db_path)
