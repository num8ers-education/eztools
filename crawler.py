"""
Domain Email Scraper — Crawler Module
BFS web crawler that extracts emails from a single domain.
Extended to support inheritance for sitemap generation.
"""

import csv
import logging
import os
import re
import time
import threading
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

JUNK_EMAILS = {
    "example@example.com", "test@test.com", "test@example.com",
    "user@example.com", "email@example.com", "info@example.com",
    "your@email.com", "name@domain.com", "someone@example.com",
}
JUNK_PREFIXES = ("noreply@", "no-reply@", "donotreply@", "do-not-reply@", "mailer-daemon@")

SKIP_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".rar", ".gz", ".tar", ".7z",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".wav",
    ".css", ".js", ".json", ".xml", ".woff", ".woff2", ".ttf", ".eot",
}

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36 "
    "DomainEmailScraper/1.0"
)

DOWNLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def normalize_url(url: str) -> str:
    """Ensure the URL has a scheme, strip fragments, and remove trailing slash."""
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    url, _ = urldefrag(url)
    if url.endswith("/"):
        url = url.rstrip("/")
    return url


def get_domain(url: str) -> str:
    """Return the domain (hostname) of the URL."""
    return urlparse(url).netloc.lower()


def is_same_domain(url: str, target_domain: str, include_subdomains: bool) -> bool:
    """Check whether *url* belongs to the target domain."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if include_subdomains:
        return host == target_domain or host.endswith("." + target_domain)
    return host == target_domain


def has_skippable_extension(url: str) -> bool:
    """Return True if the URL path ends with a non-HTML extension."""
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SKIP_EXTENSIONS)


def _is_junk_email(email: str) -> bool:
    """Return True if the email is a known junk/placeholder address."""
    if email in JUNK_EMAILS:
        return True
    if email.startswith(JUNK_PREFIXES):
        return True
    return False


def extract_emails_from_html(html: str) -> set:
    """Extract email addresses from raw HTML using regex + mailto links."""
    emails = set()
    for match in EMAIL_REGEX.findall(html):
        email = match.lower().strip(".")
        if email.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
            continue
        if _is_junk_email(email):
            continue
        emails.add(email)
    return emails


def extract_emails_from_json(text: str) -> set:
    """Extract email addresses from a JSON or plain-text API response."""
    emails = set()
    for match in EMAIL_REGEX.findall(text):
        email = match.lower().strip(".")
        if email.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
            continue
        if _is_junk_email(email):
            continue
        emails.add(email)
    return emails


def extract_links(html: str, base_url: str) -> set:
    """Extract absolute URLs from anchor tags using BeautifulSoup."""
    links = set()
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if href.startswith(("javascript:", "tel:", "#")):
                continue
            # Handle mailto — skip as link but emails already captured by regex
            if href.startswith("mailto:"):
                continue
            absolute = urljoin(base_url, href)
            absolute, _ = urldefrag(absolute)
            if absolute.endswith("/"):
                absolute = absolute.rstrip("/")
            links.add(absolute)
    except Exception:
        pass
    return links


# ---------------------------------------------------------------------------
# Main Crawler class
# ---------------------------------------------------------------------------

class DomainCrawler:
    """BFS crawler that extracts emails from a single domain."""

    def __init__(self, job: dict, stop_event: threading.Event):
        self.job = job
        self.stop_event = stop_event

        self.target_url: str = job["target_url"]
        self.max_pages: int = job.get("max_pages", 100)
        self.include_subdomains: bool = job.get("include_subdomains", False)
        self.crawl_delay: float = job.get("crawl_delay", 0.5)

        self.target_domain: str = get_domain(self.target_url)
        self.visited: set = set()
        self.queue: deque = deque()
        self.emails: dict = {}  # email -> source_page
        self.sitemap_urls: set = set()  # all discovered internal HTML URLs
        self.errors: list = []

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    # ---- public entry point ------------------------------------------------

    def run(self):
        """Execute the crawl. Called inside a thread."""
        self.job["status"] = "running"
        self._seed_queue()

        try:
            while self.queue and not self.stop_event.is_set():
                if len(self.visited) >= self.max_pages:
                    break

                url = self.queue.popleft()
                if url in self.visited:
                    continue
                if has_skippable_extension(url):
                    continue

                self._update_status(url)
                self._crawl_page(url)

                if self.crawl_delay > 0:
                    time.sleep(self.crawl_delay)

        except Exception as exc:
            logger.exception("Fatal crawl error")
            self.errors.append(f"Fatal: {exc}")

        # Finalise
        final_status = "stopped" if self.stop_event.is_set() else "completed"
        self.job["status"] = final_status
        self.job["current_page"] = ""
        self.job["pages_queued"] = 0
        self._build_results()
        self._write_outputs()

    # ---- overridable seed --------------------------------------------------

    def _seed_queue(self):
        """Seed the crawl queue. Override in subclasses for extra discovery."""
        self.queue.append(self.target_url)

    # ---- internal ----------------------------------------------------------

    def _update_status(self, current_url: str):
        self.job["current_page"] = current_url
        self.job["pages_visited"] = len(self.visited)
        self.job["pages_queued"] = len(self.queue)
        self.job["unique_emails_found"] = len(self.emails)
        self.job["unique_urls_found"] = len(self.sitemap_urls)

    def _crawl_page(self, url: str):
        """Fetch a single page, extract emails and links."""
        retries = 2
        html = None

        for attempt in range(retries + 1):
            try:
                resp = self.session.get(url, timeout=15, allow_redirects=True)
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" not in content_type and "application/xhtml" not in content_type:
                    self.visited.add(url)
                    return
                resp.raise_for_status()
                html = resp.text
                break
            except requests.RequestException as exc:
                if attempt == retries:
                    self.errors.append(f"{url} — {exc}")
                    self.visited.add(url)
                    return
                time.sleep(1)

        self.visited.add(url)

        if html is None:
            return

        # Track this URL as a valid sitemap entry
        self.sitemap_urls.add(url)

        # Extract emails
        found_emails = extract_emails_from_html(html)
        for email in found_emails:
            if email not in self.emails:
                self.emails[email] = url

        # Extract links and queue same-domain ones
        links = extract_links(html, url)
        for link in links:
            if link not in self.visited and is_same_domain(link, self.target_domain, self.include_subdomains):
                self.queue.append(link)

        # Update live counters
        self.job["pages_visited"] = len(self.visited)
        self.job["pages_queued"] = len(self.queue)
        self.job["unique_emails_found"] = len(self.emails)
        self.job["unique_urls_found"] = len(self.sitemap_urls)

    def _build_results(self):
        """Store the final results list on the job dict."""
        self.job["results"] = [
            {"email": email, "source_page": source}
            for email, source in sorted(self.emails.items())
        ]
        self.job["pages_visited"] = len(self.visited)
        self.job["unique_emails_found"] = len(self.emails)
        self.job["unique_urls_found"] = len(self.sitemap_urls)
        self.job["errors"] = self.errors

    def _write_outputs(self):
        """Write output files. Override in subclasses to add more."""
        self._write_csv()

    def _write_csv(self):
        """Write results to a CSV file."""
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        csv_path = os.path.join(DOWNLOADS_DIR, f"{self.job['job_id']}.csv")
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["email", "source_page"])
                for email, source in sorted(self.emails.items()):
                    writer.writerow([email, source])
            self.job["csv_path"] = csv_path
        except Exception as exc:
            self.errors.append(f"CSV write error: {exc}")
