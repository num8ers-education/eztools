# Domain Email Scraper, Sitemap Generator & Directory Extractor

A production-ready web application that crawls any website domain to **extract emails**, **generate XML sitemaps**, and **scrape paginated directories**. Built with a **Python Flask** backend and a polished, responsive dark-mode frontend.

![Python](https://img.shields.io/badge/Python-3.8+-blue) ![Flask](https://img.shields.io/badge/Flask-3.x-green) ![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Features

### Email Extraction
- **URL Input** — Enter any website URL to start crawling
- **Max Pages Limit** — Control how many pages to crawl (1–10,000)
- **Subdomain Toggle** — Optionally include subdomains
- **Crawl Delay** — Configurable delay between requests
- **Start / Stop Controls** — Start and gracefully stop crawls
- **Live Status** — Real-time progress with pulsing indicator, progress bar, and mode badge
- **Results Table** — View extracted emails with their source pages
- **Summary Metrics** — Pages visited, queue size, emails found, URLs found, errors
- **CSV Download** — Export email results as CSV (`email`, `source_page`)

### Sitemap Generator *(NEW)*
- **Generate Sitemap** — Crawl any domain and build a standard XML sitemap
- **robots.txt Discovery** — Parses `/robots.txt` for `Sitemap:` directives
- **sitemap.xml Seeding** — Uses existing sitemaps as URL discovery sources
- **Valid XML Output** — Generates sitemaps with `<urlset>`, `<url>`, `<loc>`, `<lastmod>`, `<changefreq>`, `<priority>`
- **Download Sitemap XML** — Export the generated sitemap file
- **URL Deduplication** — All discovered URLs are normalized and deduplicated
- **Plain Text URL List** — Also generates a `.txt` file for debugging

### Combined Full Crawl
- **Full Crawl** — Extract emails AND generate sitemap in a single pass
- Produces both CSV (emails) and XML (sitemap) downloads
- Uses robots.txt/sitemap.xml discovery for better URL coverage

### Directory Extraction Mode *(NEW)*
- **Deep Directory Crawling** — Extracts emails from paginated faculty/staff directories
- **Pagination Detection** — Handles `?page=`, `/page/N`, numbered links, "Next" buttons, `rel="next"`
- **Profile Page Traversal** — Follows links to individual profile/bio pages
- **Public API Discovery** — Detects and queries public XHR/JSON endpoints from page source
- **Load More Support** — Discovers `data-*` attributes and inline config for AJAX loading
- **Smart Limits** — Configurable max listing pages, profile pages, and emails
- **3-Phase Workflow** — Analyze → Traverse listings → Visit profiles
- **Junk Email Filtering** — Automatically removes noreply, test, example addresses
- **Same-Domain Only** — All requests stay within the target domain

---

## Prerequisites

- **Python 3.8+** (or later)
- **pip** (Python package manager)

---

## Quick Start

### 1. Clone or navigate to the project

```bash
cd domain-email-scraper
```

### 2. (Optional) Create a virtual environment

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the application

```bash
python app.py
```

### 5. Open in browser

Navigate to:

```
http://127.0.0.1:5000
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/start-scrape` | Start email-only crawl. Body: `{ url, max_pages, crawl_delay, include_subdomains }` |
| `POST` | `/start-sitemap` | Start sitemap-only crawl. Same body as above. |
| `POST` | `/start-full` | Start combined crawl (emails + sitemap). Same body. |
| `POST` | `/start-directory` | Start directory extraction. Extra params: `max_directory_pages`, `max_profile_pages`, `max_emails` |
| `POST` | `/stop-scrape` | Send stop signal to the active crawl |
| `GET` | `/status` | Get current crawl status, metrics, and job type |
| `GET` | `/results` | Get email results and/or sitemap URLs |
| `GET` | `/download/<job_id>` | Download the CSV file (email results) |
| `GET` | `/download/sitemap/<job_id>` | Download the XML sitemap file |

---

## Project Structure

```
domain-email-scraper/
├── app.py                  # Flask app & API endpoints
├── crawler.py              # Base BFS domain crawler with email extraction
├── sitemap_crawler.py      # SitemapCrawler & FullCrawler (extends crawler.py)
├── directory_crawler.py    # DirectoryCrawler with pagination/profile/API detection
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── downloads/              # Generated CSV, XML, and TXT files
├── templates/
│   └── index.html          # Frontend HTML
└── static/
    ├── css/
    │   └── style.css       # Styling
    └── js/
        └── app.js          # Frontend logic
```

---

## How It Works

### Email Extraction Mode
1. User enters a URL and clicks **Extract Emails**
2. Flask backend spawns a background thread running a BFS crawler
3. The crawler visits pages within the target domain only
4. Emails are extracted from raw HTML using regex and from `mailto:` links
5. The frontend polls `/status` and `/results` every second for live updates
6. On completion (or stop), results are saved to a CSV file for download

### Sitemap Generation Mode
1. User enters a URL and clicks **Generate Sitemap**
2. The sitemap crawler first checks `/robots.txt` and `/sitemap.xml` for seed URLs
3. It then performs a BFS crawl collecting all internal HTML page URLs
4. URLs are normalized and deduplicated
5. On completion, a valid XML sitemap file is generated following the sitemap protocol
6. A plain text URL list is also generated for debugging

### Full Crawl Mode
1. User clicks **Full Crawl** — both email extraction and sitemap generation happen in a single pass
2. Produces both CSV (emails) and XML (sitemap) for download

### Directory Extraction Mode
1. User enters a faculty/staff directory URL and clicks **Directory Extract**
2. **Phase 1 — Analyze**: The system fetches the starting page and detects:
   - Pagination patterns (`?page=`, `/page/N`, numbered links, "Next" buttons)
   - Profile detail page links (URLs containing `/faculty/`, `/staff/`, `/people/`, etc.)
   - Public API/XHR endpoints (from `data-*` attrs, inline `<script>` configs)
3. **Phase 2 — Traverse Listings**: Visits all discovered listing pages, following pagination chains
4. **Phase 2b — Query APIs**: Replays any discovered public API endpoints with pagination
5. **Phase 3 — Visit Profiles**: Follows all discovered profile detail links
6. Emails are extracted at every stage from HTML, mailto links, and JSON responses
7. Results are saved to CSV with `email` and `source_page` columns

#### Supported Directory Patterns
- `?page=2`, `?offset=10`, `?p=3`, `?paged=2`, `?start=10`, `?pageNumber=2`
- `/page/2`, `/page/3` (path-based)
- `<a rel="next">` links
- "Next", "›", "»", "Older", "Load More" button text
- Numbered pagination inside `.pagination`, `.pager`, `.nav-links` containers
- `data-url`, `data-endpoint`, `data-ajax-url` attributes
- Inline JavaScript API URL patterns

#### Sample Directory CSV Output

```csv
email,source_page
john.doe@university.edu,https://university.edu/faculty/john-doe
jane.smith@university.edu,https://university.edu/directory?page=3
bob.jones@university.edu,[API] https://university.edu/api/directory?page=5
```

---

## Sample Sitemap Output

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com</loc>
    <lastmod>2026-04-06</lastmod>
    <changefreq>weekly</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>https://example.com/about</loc>
    <lastmod>2026-04-06</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
</urlset>
```

---

## Ethical Use

This tool is designed for **ethical and authorized use only**:
- Only crawls publicly accessible HTML pages
- Does not bypass login walls, CAPTCHAs, or protected content
- Respects configurable crawl delays
- Includes a proper User-Agent header
- Only extracts publicly visible email addresses

---

## License

MIT License — use freely and responsibly.
