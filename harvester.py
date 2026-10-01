"""
Devpost harvester: hackathon directory -> winner galleries -> winner project details.

Why the old version was slow, and what changed:
  1. One Turso HTTP round trip per row. Every project insert/update was its own
     network call. Writes are now batched (one round trip per gallery page or per
     chunk of projects).
  2. Galleries were crawled up to 30 pages each, then non-winners were deleted.
     Devpost lists winners first, so we stop at the first page that contains a
     non-winner. Usually 1 or 2 requests per hackathon instead of up to 30.
  3. Everything ran one request at a time with 1.5-4s sleeps. Requests now run in
     a small thread pool behind a shared rate limiter, so total speed is
     controlled by REQS_PER_SEC instead of by sleeps.
  4. A transient error on gallery page 1 used to mark the hackathon as
     "pending winners" forever. Errors now leave it queued for the next run, and
     pending hackathons are re-checked every RECHECK_DAYS.
  5. A run budget stops new work before the GitHub Actions timeout kills the job.
"""

import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import cloudscraper
import libsql_client
from bs4 import BeautifulSoup

try:
    import lxml  # noqa: F401
    PARSER = "lxml"
except ImportError:
    PARSER = "html.parser"


# ---------------------------------------------------------------- config ----

def env_int(name, default):
    return int(os.environ.get(name, default))


def env_float(name, default):
    return float(os.environ.get(name, default))


DIRECTORY_PAGES = env_int("DIRECTORY_PAGES", 2)       # newest pages, re-read every run
BACKFILL_PAGES = env_int("BACKFILL_PAGES", 50)        # older pages per run, resumes from saved cursor
HACKATHON_LIMIT = env_int("HACKATHON_LIMIT", 60)       # new galleries per run
RECHECK_LIMIT = env_int("RECHECK_LIMIT", 10)           # pending galleries re-checked per run
RECHECK_DAYS = env_float("RECHECK_DAYS", 3)
MAX_GALLERY_PAGES = env_int("MAX_GALLERY_PAGES", 30)
PROJECT_BATCH = env_int("PROJECT_BATCH", 300)          # winner detail pages per run
WORKERS = env_int("WORKERS", 6)
REQS_PER_SEC = env_float("REQS_PER_SEC", 3.0)          # shared across all workers
RUN_BUDGET_SEC = env_int("RUN_BUDGET_SEC", 15 * 60)    # keep below the job timeout
KEEP_NON_WINNERS = os.environ.get("KEEP_NON_WINNERS", "0") == "1"
DB_CHUNK = 50

START = time.monotonic()

TURSO_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")
if not TURSO_URL:
    raise ValueError("Missing TURSO_DATABASE_URL environment variable.")
if not TURSO_URL.startswith("file:") and not TURSO_TOKEN:
    raise ValueError("Missing TURSO_AUTH_TOKEN environment variable.")

# Force HTTPS to prevent WebSocket handshake failures
TURSO_URL = TURSO_URL.replace("libsql://", "https://").replace("wss://", "https://")
client = libsql_client.create_client_sync(url=TURSO_URL, auth_token=TURSO_TOKEN)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


def time_left():
    return RUN_BUDGET_SEC - (time.monotonic() - START)


def log(msg):
    print(f"[{time.monotonic() - START:6.1f}s] {msg}", flush=True)


# ------------------------------------------------------------------ http ----

class Blocked(Exception):
    """Devpost answered 403/429. Stop scheduling new requests this run."""


class RateLimiter:
    """Spaces requests evenly across all threads."""

    def __init__(self, per_sec):
        self.interval = 1.0 / per_sec
        self.lock = threading.Lock()
        self.next_at = time.monotonic()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_at)
            self.next_at = slot + self.interval * random.uniform(0.8, 1.2)
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)


limiter = RateLimiter(REQS_PER_SEC)
blocked = threading.Event()
_local = threading.local()


def get_scraper():
    # One session per thread: keeps connections alive and is thread safe.
    s = getattr(_local, "scraper", None)
    if s is None:
        s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "linux", "desktop": True})
        s.headers.update({"User-Agent": random.choice(USER_AGENTS)})
        _local.scraper = s
    return s


def fetch(url, retries=2):
    """Returns a response for 2xx/404. Raises Blocked on 403/429, raises on other failures."""
    last_err = None
    for attempt in range(retries + 1):
        if blocked.is_set():
            raise Blocked(url)
        limiter.wait()
        try:
            res = get_scraper().get(url, timeout=15)
        except Exception as e:  # timeouts, resets
            last_err = e
        else:
            if res.status_code in (403, 429):
                blocked.set()
                raise Blocked(f"{res.status_code} on {url}")
            if res.status_code < 500:
                return res
            last_err = RuntimeError(f"HTTP {res.status_code}")
        time.sleep(1.5 * (2 ** attempt))
    raise last_err


# -------------------------------------------------------------------- db ----

def get_state(key, default=None):
    rows = client.execute("SELECT value FROM harvester_state WHERE key = ?", [key]).rows
    return rows[0][0] if rows else default


def set_state(key, value):
    client.execute(
        "INSERT INTO harvester_state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [key, str(value)],
    )


def run_batch(stmts):
    for i in range(0, len(stmts), DB_CHUNK):
        client.batch(stmts[i:i + DB_CHUNK])


def setup_database():
    log("Verifying database schema")
    client.batch([
        """
        CREATE TABLE IF NOT EXISTS hackathons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE,
            title TEXT,
            url TEXT,
            themes TEXT,
            prize_amount TEXT,
            status TEXT,
            scraped_gallery INTEGER DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hackathon_id INTEGER,
            slug TEXT UNIQUE,
            title TEXT,
            url TEXT,
            is_winner INTEGER DEFAULT 0,
            prizes_won TEXT,
            tagline TEXT,
            story TEXT,
            technologies TEXT,
            scraped_details INTEGER DEFAULT 0,
            FOREIGN KEY(hackathon_id) REFERENCES hackathons(id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_hackathon_scraped ON hackathons(scraped_gallery)",
        "CREATE INDEX IF NOT EXISTS idx_project_scraped ON projects(scraped_details)",
        "CREATE INDEX IF NOT EXISTS idx_is_winner ON projects(is_winner)",
        "CREATE INDEX IF NOT EXISTS idx_project_pending ON projects(is_winner, scraped_details)",
        "CREATE TABLE IF NOT EXISTS harvester_state (key TEXT PRIMARY KEY, value TEXT)",
    ])
    # Additive column for re-checking "pending winners" hackathons. Safe on existing data.
    cols = {r[1] for r in client.execute("PRAGMA table_info(hackathons)").rows}
    if "last_checked" not in cols:
        client.execute("ALTER TABLE hackathons ADD COLUMN last_checked INTEGER")


# --------------------------------------------------------------- helpers ----

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\n\s*\n", "\n\n", text).strip()


def strip_tags(value):
    return re.sub(r"<[^>]+>", "", str(value or "")).strip()


def is_winner_card(card):
    return card.find(class_=lambda x: x and ("winner" in x.lower() or "entry-badge" in x.lower())) is not None


# --------------------------------------------------------------- phase 1 ----

def index_directory_page(page):
    """Returns number of hackathons on the page, 0 if empty, None on error."""
    url = f"https://devpost.com/api/hackathons?challenge_type[]=all&status[]=ended&page={page}"
    try:
        res = fetch(url)
        hackathons = res.json().get("hackathons", []) if res.status_code == 200 else []
    except Exception as e:
        log(f"  directory page {page} failed: {e}")
        return None
    if not hackathons:
        return 0

    stmts = []
    for h in hackathons:
        h_url = h.get("url") or ""
        if not h_url:
            continue
        slug = h_url.split("//")[-1].split(".")[0]
        themes = ",".join(t.get("name") or "" for t in h.get("themes") or [])
        stmts.append((
            "INSERT OR IGNORE INTO hackathons (slug, title, url, themes, prize_amount, status) "
            "VALUES (?, ?, ?, ?, ?, 'ended')",
            [slug, h.get("title"), h_url, themes, strip_tags(h.get("prize_amount", "$0"))],
        ))
    run_batch(stmts)
    return len(hackathons)


def step1_discover_hackathons():
    log("Phase 1: hackathon directory")
    before = client.execute("SELECT COUNT(*) FROM hackathons").rows[0][0]

    # Newest ended hackathons: always re-read so nothing new is missed.
    for page in range(1, DIRECTORY_PAGES + 1):
        if not index_directory_page(page):
            break

    # Backfill: continue into older pages from where the last run stopped.
    # New hackathons push older ones to higher page numbers, so a lagging cursor
    # only re-reads a few rows (ignored by INSERT OR IGNORE); it never skips any.
    cursor = get_state("directory_cursor", str(DIRECTORY_PAGES + 1))
    if cursor == "done":
        log("  backfill complete, only checking newest pages")
    else:
        page = max(int(cursor), DIRECTORY_PAGES + 1)
        first = page
        for _ in range(BACKFILL_PAGES):
            if blocked.is_set() or time_left() < 120:
                break
            n = index_directory_page(page)
            if n is None:          # error: keep cursor, retry this page next run
                break
            if n == 0:             # past the last page
                page = "done"
                break
            page += 1
        set_state("directory_cursor", page)
        if page == "done":
            log(f"  backfill reached the last directory page (started at {first})")
        else:
            log(f"  backfill pages {first}..{page - 1}, next run starts at page {page}")

    after = client.execute("SELECT COUNT(*) FROM hackathons").rows[0][0]
    log(f"  {after - before} new hackathons ({after} total)")


# --------------------------------------------------------------- phase 2 ----

def scan_gallery(h_id, h_url):
    """Runs in a worker thread. No DB access here; returns rows for the main thread."""
    base = f"{h_url.rstrip('/')}/project-gallery"
    rows, winners, pages = [], 0, 0

    for page in range(1, MAX_GALLERY_PAGES + 1):
        if time_left() < 30:
            return {"id": h_id, "state": "retry", "rows": [], "winners": 0, "pages": pages, "err": "(time budget)"}
        try:
            res = fetch(f"{base}?page={page}")
        except Blocked:
            return {"id": h_id, "state": "retry", "rows": [], "winners": 0, "pages": pages}
        except Exception as e:
            return {"id": h_id, "state": "retry", "rows": [], "winners": 0, "pages": pages, "err": str(e)}

        if res.status_code == 404:
            break
        pages += 1
        soup = BeautifulSoup(res.text, PARSER)
        cards = soup.select("div.gallery-item") or soup.select("a.link-to-software")
        if not cards:
            break

        saw_non_winner = False
        for card in cards:
            link = card if card.name == "a" else card.find("a", class_="link-to-software")
            if not link or not link.get("href"):
                continue
            win = is_winner_card(card)
            if not win:
                saw_non_winner = True
                if not KEEP_NON_WINNERS:
                    continue
            p_url = urljoin(base, link["href"]).split("?")[0]
            tagline_el = card.find(class_="tagline")
            rows.append([
                h_id,
                p_url.rstrip("/").split("/")[-1],
                link.get_text(strip=True),
                p_url,
                1 if win else 0,
                tagline_el.get_text(strip=True) if tagline_el else "",
            ])
            winners += win

        # Devpost sorts winners first. Once a non-winner shows up, there are no more winners.
        if saw_non_winner and not KEEP_NON_WINNERS:
            break

    state = "done" if winners else "pending"
    return {"id": h_id, "state": state, "rows": rows if winners else [], "winners": winners, "pages": pages}


def step2_scrape_galleries():
    log("Phase 2: winner galleries")
    now = int(time.time())
    due = now - int(RECHECK_DAYS * 86400)
    queue = client.execute(
        "SELECT id, url, title FROM hackathons WHERE scraped_gallery = 0 ORDER BY id DESC LIMIT ?",
        [HACKATHON_LIMIT],
    ).rows
    queue += client.execute(
        "SELECT id, url, title FROM hackathons WHERE scraped_gallery = 2 "
        "AND (last_checked IS NULL OR last_checked < ?) ORDER BY last_checked IS NOT NULL, last_checked LIMIT ?",
        [due, RECHECK_LIMIT],
    ).rows
    if not queue:
        log("  nothing to scan")
        return

    titles = {r[0]: r[2] for r in queue}
    done = pending = retry = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(scan_gallery, r[0], r[1]) for r in queue]
        for fut in as_completed(futures):
            r = fut.result()
            title = titles[r["id"]]
            if r["state"] == "retry":
                retry += 1
                log(f"  retry later: {title} {r.get('err', '(blocked)')}")
                continue

            stmts = [(
                "INSERT OR IGNORE INTO projects (hackathon_id, slug, title, url, is_winner, tagline) "
                "VALUES (?, ?, ?, ?, ?, ?)", row) for row in r["rows"]]
            # Upgrade rows that already exist as non-winners (e.g. from older runs).
            stmts += [("UPDATE projects SET is_winner = 1 WHERE slug = ? AND is_winner = 0", [row[1]])
                      for row in r["rows"] if row[4] == 1]
            flag = 1 if r["state"] == "done" else 2
            stmts.append(("UPDATE hackathons SET scraped_gallery = ?, last_checked = ? WHERE id = ?",
                          [flag, now, r["id"]]))
            run_batch(stmts)

            if flag == 1:
                done += 1
                log(f"  {title}: {r['winners']} winners ({r['pages']} page(s))")
            else:
                pending += 1
                log(f"  {title}: no winners yet, will re-check")

    log(f"  galleries: {done} complete, {pending} pending, {retry} retry")


# --------------------------------------------------------------- phase 3 ----

def scrape_project(p_id, p_url):
    try:
        res = fetch(p_url)
    except Blocked:
        return None
    except Exception as e:
        log(f"  error {p_url}: {e}")
        return None

    if res.status_code == 404:
        return ("UPDATE projects SET scraped_details = -1 WHERE id = ?", [p_id])

    soup = BeautifulSoup(res.text, PARSER)
    story_div = soup.find(id="app-details-left")
    story = clean_text(story_div.get_text(separator="\n", strip=True)) if story_div else ""
    tech = ",".join(li.get_text(strip=True) for li in soup.select("div#built-with li"))
    prizes = list(dict.fromkeys(  # dedupe, keep order
        p.get_text(strip=True) for p in soup.select(".software-prizes .winner, .software-prizes li")
    ))
    return (
        "UPDATE projects SET story = ?, technologies = ?, prizes_won = ?, is_winner = 1, scraped_details = 1 "
        "WHERE id = ?",
        [story, tech, "; ".join(prizes), p_id],
    )


def step3_scrape_project_details():
    log(f"Phase 3: winner details (limit {PROJECT_BATCH})")
    rows = client.execute(
        "SELECT id, url FROM projects WHERE scraped_details = 0 AND is_winner = 1 LIMIT ?",
        [PROJECT_BATCH],
    ).rows
    if not rows:
        log("  no pending winners")
        return

    pending, saved = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for r in rows:
            if blocked.is_set() or time_left() < 30:
                break
            futures[pool.submit(scrape_project, r[0], r[1])] = r[0]
        for fut in as_completed(futures):
            stmt = fut.result()
            if stmt:
                pending.append(stmt)
            if len(pending) >= DB_CHUNK:
                run_batch(pending)
                saved += len(pending)
                pending = []
                log(f"  saved {saved}/{len(futures)}")
            if time_left() < 20:
                # Out of time: stop waiting on queued work.
                for f in futures:
                    f.cancel()
    if pending:
        run_batch(pending)
        saved += len(pending)
    log(f"  details saved for {saved} project(s)")


# ------------------------------------------------------------------ main ----

if __name__ == "__main__":
    setup_database()
    step1_discover_hackathons()
    if not blocked.is_set() and time_left() > 60:
        step2_scrape_galleries()
    if not blocked.is_set() and time_left() > 30:
        step3_scrape_project_details()
    if blocked.is_set():
        log("Devpost returned 403/429; stopped early. Remaining work is queued for the next run.")
    client.close()
    log("Execution complete.")
