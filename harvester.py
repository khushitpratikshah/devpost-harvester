import os
import time
import random
import re
import requests
import cloudscraper
from bs4 import BeautifulSoup
import libsql_client

TURSO_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

if not TURSO_URL or not TURSO_TOKEN:
    raise ValueError("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN environment variables.")

# Force HTTPS to prevent WebSocket handshake failures
TURSO_URL = TURSO_URL.replace("libsql://", "https://").replace("wss://", "https://")
client = libsql_client.create_client_sync(url=TURSO_URL, auth_token=TURSO_TOKEN)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

def get_scraper():
    scraper = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'linux', 'desktop': True})
    scraper.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return scraper

def clean_text(text):
    if not text:
        return ""
    return re.sub(r'\n\s*\n', '\n\n', text).strip()

def setup_database():
    print("--- Verifying Database Schema ---")
    client.execute("""
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
    """)
    client.execute("""
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
    """)
    client.execute("CREATE INDEX IF NOT EXISTS idx_hackathon_scraped ON hackathons(scraped_gallery)")
    client.execute("CREATE INDEX IF NOT EXISTS idx_project_scraped ON projects(scraped_details)")
    client.execute("CREATE INDEX IF NOT EXISTS idx_is_winner ON projects(is_winner)")
    print("Database schema is ready.\n")

def step1_discover_hackathons(max_pages=2):
    print("--- Phase 1: Checking Hackathon Directory ---")
    session = requests.Session()
    session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    
    for page in range(1, max_pages + 1):
        url = f"https://devpost.com/api/hackathons?challenge_type[]=all&status[]=ended&page={page}"
        try:
            res = session.get(url, timeout=10)
            if res.status_code != 200:
                break
            data = res.json()
            hackathons = data.get("hackathons", [])
            if not hackathons:
                break
                
            for h in hackathons:
                slug = h.get("url", "").split("//")[-1].split(".")[0]
                title = h.get("title")
                h_url = h.get("url")
                themes = ",".join([t.get("name") for t in h.get("themes", [])])
                prize = str(h.get("prize_amount", "$0"))
                
                client.execute(
                    "INSERT OR IGNORE INTO hackathons (slug, title, url, themes, prize_amount, status) VALUES (?, ?, ?, ?, ?, 'ended')",
                    [slug, title, h_url, themes, prize]
                )
            print(f"Indexed hackathon directory page {page}")
            time.sleep(1)
        except Exception as e:
            print(f"Error on directory page {page}: {e}")
            break

def step2_scrape_galleries(limit=6, max_pages=30):
    print("\n--- Phase 2: Indexing Hackathon Galleries ---")
    scraper = get_scraper()
    rs = client.execute("SELECT id, url, title FROM hackathons WHERE scraped_gallery = 0 LIMIT ?", [limit])
    
    for row in rs.rows:
        h_id, h_url, h_title = row[0], row[1], row[2]
        print(f"Scanning gallery for: {h_title}")
        base_gallery = f"{h_url.rstrip('/')}/project-gallery"
        page = 1
        winners_found = 0
        
        while page <= max_pages:
            gallery_url = f"{base_gallery}?page={page}"
            try:
                res = scraper.get(gallery_url, timeout=12)
                if res.status_code in [404, 403, 429]:
                    break
                
                soup = BeautifulSoup(res.text, 'html.parser')
                cards = soup.select('div.gallery-item') or soup.select('a.link-to-software')
                
                if not cards:
                    break
                    
                for card in cards:
                    link_elem = card if card.name == 'a' else card.find('a', class_='link-to-software')
                    if not link_elem or 'href' not in link_elem.attrs:
                        continue
                    
                    p_url = link_elem['href']
                    p_slug = p_url.split('/')[-1]
                    
                    winner_tag = card.find(class_=lambda x: x and ('winner' in x.lower() or 'entry-badge' in x.lower()))
                    is_winner = 1 if winner_tag else 0
                    
                    if is_winner == 1:
                        winners_found += 1
                    
                    tagline_elem = card.find(class_='tagline')
                    tagline = tagline_elem.get_text(strip=True) if tagline_elem else ""
                    title = link_elem.get_text(strip=True)
                    
                    client.execute(
                        "INSERT OR IGNORE INTO projects (hackathon_id, slug, title, url, is_winner, tagline) VALUES (?, ?, ?, ?, ?, ?)",
                        [h_id, p_slug, title, p_url, is_winner, tagline]
                    )
                
                print(f"  -> Processed gallery page {page}")
                page += 1
                time.sleep(random.uniform(1.5, 3.0))
            except Exception as e:
                print(f"Error scanning gallery {gallery_url}: {e}")
                break
                
        # INTELLIGENT WINNER CHECK
        if winners_found > 0:
            print(f"  -> Success: Found {winners_found} winners. Marking as complete.")
            client.execute("UPDATE hackathons SET scraped_gallery = 1 WHERE id = ?", [h_id])
        else:
            print(f"  -> No winners found for '{h_title}'. Likely pending judging.")
            # Delete non-winning projects to avoid database bloat
            client.execute("DELETE FROM projects WHERE hackathon_id = ?", [h_id])
            # Set to 2 (Pending Winners) so we skip it in future automated runs
            client.execute("UPDATE hackathons SET scraped_gallery = 2 WHERE id = ?", [h_id])

def step3_scrape_project_details(batch_size=80):
    print(f"\n--- Phase 3: Deep Scraping Winners Only (Limit: {batch_size}) ---")
    scraper = get_scraper()
    
    rs = client.execute("SELECT id, url, title FROM projects WHERE scraped_details = 0 AND is_winner = 1 LIMIT ?", [batch_size])
    
    if not rs.rows:
        print("No pending winners to deep-scrape.")
        return

    for row in rs.rows:
        p_id, p_url, p_title = row[0], row[1], row[2]
        print(f"Fetching details for winner {p_id}: {p_url.split('/')[-1]}")
        
        try:
            res = scraper.get(p_url, timeout=12)
            if res.status_code in [403, 429]:
                print(f"  -> Status {res.status_code} hit. Backing off.")
                break
            
            if res.status_code == 404:
                client.execute("UPDATE projects SET scraped_details = -1 WHERE id = ?", [p_id])
                continue
                
            soup = BeautifulSoup(res.text, 'html.parser')
            
            story_div = soup.find(id='app-details-left')
            story = clean_text(story_div.get_text(separator='\n', strip=True)) if story_div else ""
            
            tech_tags = [li.get_text(strip=True) for li in soup.select('div#built-with li')]
            technologies = ",".join(tech_tags)
            
            prizes_list = [p.get_text(strip=True) for p in soup.select('.software-prizes .winner, .software-prizes li')]
            prizes_won = "; ".join(prizes_list)
            is_winner_flag = 1 if len(prizes_list) > 0 else 1
            
            client.execute("""
                UPDATE projects 
                SET story = ?, technologies = ?, prizes_won = ?, 
                    is_winner = ?, scraped_details = 1 
                WHERE id = ?
            """, [story, technologies, prizes_won, is_winner_flag, p_id])
            
            time.sleep(random.uniform(2.0, 4.0))
        except Exception as e:
            print(f"Error scraping project {p_url}: {e}")
            time.sleep(2)

if __name__ == "__main__":
    setup_database()
    step1_discover_hackathons(max_pages=2)
    step2_scrape_galleries(limit=6, max_pages=30)
    step3_scrape_project_details(batch_size=80)
    print("\nExecution complete. Database updated.")
