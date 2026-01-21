import sqlite3, subprocess, sys
from pathlib import Path

def main():
    Path("imdb_scraper/output/movies.db").unlink(missing_ok=True)
    
    c = lambda n, m: [sys.executable, "-m", "scrapy", "crawl", n, "-a", f"max_movies={m}"]
    procs = [
        subprocess.Popen(c("movie_scraper", 0)),
        subprocess.Popen(c("metacritic_scraper", 20000)),
        subprocess.Popen(c("boxoffice_scraper", 20000))
    ]

    try:
        procs[0].wait()
    except KeyboardInterrupt:
        pass
    finally:
        [p.terminate() for p in procs]
        [p.wait() for p in procs]
        
        try:
            with sqlite3.connect("output/movies.db") as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("PRAGMA journal_mode = DELETE")
        except: pass

if __name__ == "__main__":
    main()
