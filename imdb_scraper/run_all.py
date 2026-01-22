# Author: Juliusz | Online Data Mining - Amsterdam UAS
# Run all scrapers in the correct order:
# 1. First: movie_scraper (populates movie table)
# 2. Then: metacritic_scraper + boxoffice_scraper (in parallel, both read from movie table)
#
# All scrapers write to the same database: output/movies.db

import subprocess
import sys
import argparse
from pathlib import Path


def run_scrapy(spider_name, max_movies, extra_args=None):
    """Run a scrapy spider and return the process."""
    cmd = [
        sys.executable, "-m", "scrapy", "crawl", spider_name,
        "-a", f"max_movies={max_movies}"
    ]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.Popen(cmd)


def main():
    parser = argparse.ArgumentParser(description="Run all movie scrapers")
    parser.add_argument("--max-movies", type=int, default=20000,
                        help="Maximum movies to scrape (default: 20000)")
    parser.add_argument("--skip-imdb", action="store_true",
                        help="Skip IMDB scraper (use existing movie database)")
    parser.add_argument("--fresh", action="store_true",
                        help="Delete existing database and start fresh")
    args = parser.parse_args()

    output_dir = Path(__file__).parent / "output"
    db_path = output_dir / "movies.db"

    # Optionally delete existing database
    if args.fresh and db_path.exists():
        print(f"Deleting existing database: {db_path}")
        db_path.unlink()

    print("=" * 60)
    print("MOVIE SCRAPER PIPELINE")
    print("=" * 60)
    print(f"Max movies: {args.max_movies}")
    print(f"Database: {db_path}")
    print()

    # STEP 1: Run movie_scraper first (metacritic and boxoffice depend on it)
    if not args.skip_imdb:
        print("-" * 60)
        print("STEP 1: Running IMDB movie_scraper...")
        print("-" * 60)
        proc = run_scrapy("movie_scraper", args.max_movies)
        try:
            proc.wait()
        except KeyboardInterrupt:
            print("\nInterrupted! Stopping movie_scraper...")
            proc.terminate()
            proc.wait()
            return

        if proc.returncode != 0:
            print(f"movie_scraper failed with return code {proc.returncode}")
            return
        print("movie_scraper completed!")
    else:
        print("Skipping IMDB scraper (--skip-imdb flag)")

    # Check if we have movies to process
    if not db_path.exists():
        print("ERROR: No movie database found. Run movie_scraper first.")
        return

    import sqlite3
    conn = sqlite3.connect(db_path)
    movie_count = conn.execute("SELECT COUNT(*) FROM movie").fetchone()[0]
    conn.close()
    print(f"\nMovies in database: {movie_count}")

    if movie_count == 0:
        print("ERROR: No movies in database. Nothing to scrape from Metacritic/BoxOffice.")
        return

    # STEP 2: Run metacritic_scraper and boxoffice_scraper in parallel
    print()
    print("-" * 60)
    print("STEP 2: Running Metacritic + Box Office scrapers in parallel...")
    print("-" * 60)

    metacritic_proc = run_scrapy("metacritic_scraper", args.max_movies)
    boxoffice_proc = run_scrapy("boxoffice_scraper", args.max_movies)

    try:
        # Wait for both to complete
        metacritic_proc.wait()
        boxoffice_proc.wait()
    except KeyboardInterrupt:
        print("\nInterrupted! Stopping scrapers...")
        metacritic_proc.terminate()
        boxoffice_proc.terminate()
        metacritic_proc.wait()
        boxoffice_proc.wait()
        return

    print()
    print("=" * 60)
    print("ALL SCRAPERS COMPLETED!")
    print("=" * 60)

    # Print final stats
    conn = sqlite3.connect(db_path)
    stats = {
        "Movies": conn.execute("SELECT COUNT(*) FROM movie").fetchone()[0],
        "IMDB Reviews": conn.execute("SELECT COUNT(*) FROM imdb_review").fetchone()[0],
        "Directors": conn.execute("SELECT COUNT(*) FROM director").fetchone()[0],
        "Actors": conn.execute("SELECT COUNT(*) FROM actor").fetchone()[0],
        "Metacritic Data": conn.execute("SELECT COUNT(*) FROM metacritic_data").fetchone()[0],
        "Metacritic Critic Reviews": conn.execute("SELECT COUNT(*) FROM metacritic_critic_review").fetchone()[0],
        "Metacritic User Reviews": conn.execute("SELECT COUNT(*) FROM metacritic_user_review").fetchone()[0],
        "Box Office Data": conn.execute("SELECT COUNT(*) FROM box_office_data").fetchone()[0],
    }
    conn.close()

    print("\nFinal database stats:")
    for table, count in stats.items():
        print(f"  {table}: {count}")
    print(f"\nDatabase saved to: {db_path}")


if __name__ == "__main__":
    main()
