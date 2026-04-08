"""
Domain Email Scraper — Sitemap Crawler Module
Extends DomainCrawler to generate XML sitemaps.
Also provides a FullCrawler that does emails + sitemap in one pass.
"""

import logging
import os
import re
import time
from datetime import date
from urllib.parse import urljoin, urlparse
from xml.sax.saxutils import escape as xml_escape

import requests
from bs4 import BeautifulSoup

from crawler import (
    DomainCrawler,
    DOWNLOADS_DIR,
    extract_links,
    get_domain,
    has_skippable_extension,
    is_same_domain,
    normalize_url,
    extract_emails_from_html,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex for extracting URLs from robots.txt Sitemap directives
# and from XML <loc> tags
# ---------------------------------------------------------------------------
ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
XML_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.IGNORECASE)


# ===========================================================================
# SitemapCrawler — crawl-only, no email extraction, produces XML sitemap
# ===========================================================================

class SitemapCrawler(DomainCrawler):
    """
    BFS crawler focused on URL discovery for sitemap generation.
    Inherits the full crawl loop from DomainCrawler, but:
      - seeds from robots.txt / sitemap.xml too
      - skips email extraction for speed
      - writes XML sitemap + text URL list
    """

    # ---- seed override: add robots.txt & sitemap.xml discovery --------------

    def _seed_queue(self):
        """Seed queue with target URL + URLs discovered from robots.txt and
        any existing sitemap.xml."""
        self.queue.append(self.target_url)
        self._discover_from_robots()
        self._discover_from_sitemap()

    # ---- crawl page override: skip email extraction --------------------------

    def _crawl_page(self, url: str):
        """Fetch page, collect URL for sitemap, extract links. No emails."""
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

        # Track URL as valid sitemap entry
        self.sitemap_urls.add(url)

        # Extract links and queue same-domain ones
        links = extract_links(html, url)
        for link in links:
            if link not in self.visited and is_same_domain(
                link, self.target_domain, self.include_subdomains
            ):
                self.queue.append(link)

        # Update live counters
        self.job["pages_visited"] = len(self.visited)
        self.job["pages_queued"] = len(self.queue)
        self.job["unique_urls_found"] = len(self.sitemap_urls)

    # ---- results override ---------------------------------------------------

    def _build_results(self):
        """Store sitemap URL list on the job dict."""
        self.job["sitemap_urls"] = sorted(self.sitemap_urls)
        self.job["pages_visited"] = len(self.visited)
        self.job["unique_urls_found"] = len(self.sitemap_urls)
        self.job["errors"] = self.errors

    # ---- outputs override ---------------------------------------------------

    def _write_outputs(self):
        """Write XML sitemap + plain text URL list. No CSV."""
        self._write_sitemap_xml()
        self._write_url_list()

    # ---- XML sitemap writer -------------------------------------------------

    def _write_sitemap_xml(self):
        """Generate a valid XML sitemap file."""
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)

        domain_safe = get_domain(self.target_url).replace(":", "_").replace(".", "_")
        timestamp = date.today().isoformat()
        filename = f"sitemap_{domain_safe}_{timestamp}.xml"
        xml_path = os.path.join(DOWNLOADS_DIR, filename)

        try:
            with open(xml_path, "w", encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')

                sorted_urls = sorted(self.sitemap_urls)
                for i, url in enumerate(sorted_urls):
                    loc = xml_escape(url)
                    # Root page gets highest priority
                    priority = "1.0" if url == self.target_url else "0.8"
                    f.write("  <url>\n")
                    f.write(f"    <loc>{loc}</loc>\n")
                    f.write(f"    <lastmod>{timestamp}</lastmod>\n")
                    f.write(f"    <changefreq>weekly</changefreq>\n")
                    f.write(f"    <priority>{priority}</priority>\n")
                    f.write("  </url>\n")

                f.write("</urlset>\n")

            self.job["sitemap_path"] = xml_path
            logger.info("Sitemap written: %s (%d URLs)", xml_path, len(self.sitemap_urls))
        except Exception as exc:
            self.errors.append(f"Sitemap XML write error: {exc}")

    # ---- plain text URL list ------------------------------------------------

    def _write_url_list(self):
        """Write a plain text file with one URL per line for debugging."""
        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        domain_safe = get_domain(self.target_url).replace(":", "_").replace(".", "_")
        timestamp = date.today().isoformat()
        filename = f"urls_{domain_safe}_{timestamp}.txt"
        txt_path = os.path.join(DOWNLOADS_DIR, filename)

        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                for url in sorted(self.sitemap_urls):
                    f.write(url + "\n")
            self.job["url_list_path"] = txt_path
        except Exception as exc:
            self.errors.append(f"URL list write error: {exc}")

    # ---- robots.txt discovery -----------------------------------------------

    def _discover_from_robots(self):
        """Fetch /robots.txt and extract Sitemap: directives."""
        parsed = urlparse(self.target_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        try:
            resp = self.session.get(robots_url, timeout=10)
            if resp.status_code == 200 and "text" in resp.headers.get("Content-Type", ""):
                text = resp.text
                # Extract Sitemap: directives
                for match in ROBOTS_SITEMAP_RE.findall(text):
                    sitemap_url = match.strip()
                    if is_same_domain(sitemap_url, self.target_domain, self.include_subdomains):
                        # This is a sitemap XML — try to parse it for URLs
                        self._extract_sitemap_locs(sitemap_url)
                    elif sitemap_url.endswith(".xml"):
                        # External sitemap — skip
                        logger.debug("Skipping external sitemap: %s", sitemap_url)
                logger.info("Parsed robots.txt at %s", robots_url)
        except requests.RequestException:
            logger.debug("Could not fetch robots.txt at %s", robots_url)

    # ---- sitemap.xml discovery ----------------------------------------------

    def _discover_from_sitemap(self):
        """Fetch /sitemap.xml and extract <loc> URLs."""
        parsed = urlparse(self.target_url)
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
        self._extract_sitemap_locs(sitemap_url)

    def _extract_sitemap_locs(self, sitemap_url: str):
        """Fetch a sitemap XML and add same-domain <loc> URLs to the queue."""
        try:
            resp = self.session.get(sitemap_url, timeout=10)
            if resp.status_code != 200:
                return
            content = resp.text

            locs = XML_LOC_RE.findall(content)
            added = 0
            for loc in locs:
                loc = loc.strip()
                if not loc:
                    continue
                # Normalize
                loc = normalize_url(loc)
                if not loc:
                    continue
                # Only same-domain
                if not is_same_domain(loc, self.target_domain, self.include_subdomains):
                    continue
                # Skip binary/non-HTML
                if has_skippable_extension(loc):
                    continue
                if loc not in self.visited:
                    self.queue.append(loc)
                    added += 1

            logger.info("Extracted %d URLs from sitemap %s", added, sitemap_url)
        except requests.RequestException:
            logger.debug("Could not fetch sitemap at %s", sitemap_url)


# ===========================================================================
# FullCrawler — emails + sitemap in a single pass
# ===========================================================================

class FullCrawler(DomainCrawler):
    """
    Combined crawler: extracts emails AND collects sitemap URLs.
    Seeds from robots.txt/sitemap.xml like SitemapCrawler, but also
    extracts emails like DomainCrawler. Writes CSV + XML + URL list.
    """

    # ---- seed: reuse SitemapCrawler discovery --------------------------------

    def _seed_queue(self):
        self.queue.append(self.target_url)
        # Use SitemapCrawler's discovery methods
        self._sitemap_helper = SitemapCrawler.__new__(SitemapCrawler)
        self._sitemap_helper.session = self.session
        self._sitemap_helper.target_url = self.target_url
        self._sitemap_helper.target_domain = self.target_domain
        self._sitemap_helper.include_subdomains = self.include_subdomains
        self._sitemap_helper.visited = self.visited
        self._sitemap_helper.queue = self.queue
        self._sitemap_helper.errors = self.errors
        self._sitemap_helper._discover_from_robots()
        self._sitemap_helper._discover_from_sitemap()

    # ---- _crawl_page: does both emails AND sitemap URL tracking ----
    # The parent DomainCrawler._crawl_page already does both since we
    # added sitemap_urls tracking there. So we inherit it as-is.

    # ---- results: combine both ----------------------------------------------

    def _build_results(self):
        """Store both email results and sitemap URLs."""
        # Email results
        self.job["results"] = [
            {"email": email, "source_page": source}
            for email, source in sorted(self.emails.items())
        ]
        # Sitemap URLs
        self.job["sitemap_urls"] = sorted(self.sitemap_urls)
        # Counters
        self.job["pages_visited"] = len(self.visited)
        self.job["unique_emails_found"] = len(self.emails)
        self.job["unique_urls_found"] = len(self.sitemap_urls)
        self.job["errors"] = self.errors

    # ---- outputs: CSV + XML + URL list --------------------------------------

    def _write_outputs(self):
        """Write CSV for emails and XML sitemap for URLs."""
        self._write_csv()
        # Reuse SitemapCrawler's XML/txt writers via the helper
        self._sitemap_helper.sitemap_urls = self.sitemap_urls
        self._sitemap_helper.job = self.job
        self._sitemap_helper._write_sitemap_xml()
        self._sitemap_helper._write_url_list()
