"""
Domain Email Scraper — Directory Crawler Module
Handles paginated faculty/staff directories, profile pages, and public API endpoints.
Extends DomainCrawler with pagination detection, profile link extraction, and API discovery.
"""

import json
import logging
import re
import time
import threading
from collections import deque
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup
import requests

from crawler import (
    DomainCrawler,
    extract_emails_from_html,
    extract_emails_from_json,
    extract_links,
    get_domain,
    has_skippable_extension,
    is_same_domain,
    normalize_url,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Query parameter names commonly used for pagination
PAGINATION_PARAMS = {
    "page", "p", "paged", "pg", "pagenumber", "pagenum",
    "offset", "start", "skip", "from", "cursor",
}

# Keywords indicating directory/profile content
DIRECTORY_KEYWORDS = {
    "faculty", "staff", "people", "profile", "directory", "member",
    "person", "employee", "team", "personnel", "contact", "about",
    "bio", "biography", "instructor", "professor", "teacher",
    "researcher", "advisor", "adviser",
}

# Text patterns for "Next" pagination buttons
NEXT_BUTTON_TEXTS = {
    "next", "next page", "next »", "next »", "next ›",
    "›", "»", "→", "older", "more", "load more",
    "show more", "view more",
}

# CSS class/id keywords for pagination containers
PAGINATION_CONTAINER_KEYWORDS = {
    "pagination", "pager", "paging", "page-nav", "page-numbers",
    "nav-links", "paginator", "pages", "page-link",
}

# CSS class/id keywords for directory/profile containers
PROFILE_CONTAINER_KEYWORDS = {
    "faculty", "staff", "people", "directory", "member", "team",
    "person", "profile", "card", "listing", "result", "entry",
    "employee", "contact",
}


# ===========================================================================
# PaginationDetector
# ===========================================================================

class PaginationDetector:
    """Analyzes HTML to detect pagination patterns and extract page URLs."""

    def __init__(self, base_url: str, target_domain: str, include_subdomains: bool):
        self.base_url = base_url
        self.target_domain = target_domain
        self.include_subdomains = include_subdomains

    def detect(self, html: str) -> dict:
        """
        Analyze HTML for pagination patterns.
        Returns: {
            "type": str,  # "query_param"|"path"|"next_link"|"numbered"|"none"
            "urls": list,  # discovered pagination URLs
            "next_url": str|None,  # immediate next page if found
            "param_name": str|None,  # the query param used (e.g. "page")
        }
        """
        soup = BeautifulSoup(html, "html.parser")
        result = {"type": "none", "urls": [], "next_url": None, "param_name": None}

        # Strategy 1: <a rel="next"> (most reliable)
        rel_next = self._find_rel_next(soup)
        if rel_next:
            result["next_url"] = rel_next
            result["type"] = "next_link"

        # Strategy 2: query-param pagination (?page=2, ?offset=10, etc.)
        param_urls, param_name = self._find_query_param_pagination(soup)
        if param_urls:
            result["urls"].extend(param_urls)
            result["param_name"] = param_name
            if result["type"] == "none":
                result["type"] = "query_param"

        # Strategy 3: path-based pagination (/page/2, /page/3)
        path_urls = self._find_path_pagination(soup)
        if path_urls:
            result["urls"].extend(path_urls)
            if result["type"] == "none":
                result["type"] = "path"

        # Strategy 4: numbered pagination links inside pagination containers
        numbered_urls = self._find_numbered_pagination(soup)
        if numbered_urls:
            result["urls"].extend(numbered_urls)
            if result["type"] == "none":
                result["type"] = "numbered"

        # Strategy 5: "Next" button text
        if not result["next_url"]:
            next_from_text = self._find_next_by_text(soup)
            if next_from_text:
                result["next_url"] = next_from_text
                if result["type"] == "none":
                    result["type"] = "next_link"

        # Deduplicate and filter
        seen = set()
        clean_urls = []
        for url in result["urls"]:
            n = normalize_url(url)
            if n and n not in seen and n != normalize_url(self.base_url):
                if is_same_domain(n, self.target_domain, self.include_subdomains):
                    seen.add(n)
                    clean_urls.append(n)
        result["urls"] = clean_urls

        if result["next_url"]:
            result["next_url"] = normalize_url(result["next_url"])

        return result

    def generate_page_urls(self, html: str, max_pages: int = 100) -> list:
        """
        After initial detection, generate a full set of pagination URLs.
        Uses detected param name to build ?page=1..N style URLs if a param was found.
        """
        detection = self.detect(html)
        urls = list(detection["urls"])

        # If we found a param-based pattern, try to generate more pages
        if detection["param_name"] and detection["type"] == "query_param":
            param = detection["param_name"]
            parsed = urlparse(self.base_url)
            qs = parse_qs(parsed.query)

            for page_num in range(2, max_pages + 1):
                qs_copy = dict(qs)
                qs_copy[param] = [str(page_num)]
                new_query = urlencode(qs_copy, doseq=True)
                new_url = urlunparse(parsed._replace(query=new_query))
                n = normalize_url(new_url)
                if n not in urls:
                    urls.append(n)

        return urls

    # ---- Internal detection methods ----------------------------------------

    def _find_rel_next(self, soup: BeautifulSoup):
        """Find <a rel='next'> or <link rel='next'>."""
        for tag in soup.find_all(["a", "link"], rel="next", href=True):
            href = tag["href"].strip()
            if href and not href.startswith(("javascript:", "#")):
                return urljoin(self.base_url, href)
        return None

    def _find_query_param_pagination(self, soup: BeautifulSoup):
        """Find links with pagination query params like ?page=2."""
        found_urls = []
        found_param = None
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue
            abs_url = urljoin(self.base_url, href)
            parsed = urlparse(abs_url)
            qs = parse_qs(parsed.query)
            for param in PAGINATION_PARAMS:
                if param in qs:
                    try:
                        val = qs[param][0]
                        int(val)  # must be numeric
                        found_urls.append(abs_url)
                        found_param = param
                    except (ValueError, IndexError):
                        pass
        return found_urls, found_param

    def _find_path_pagination(self, soup: BeautifulSoup):
        """Find /page/2 style path-based pagination."""
        page_re = re.compile(r"/page/(\d+)/?$", re.IGNORECASE)
        found = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue
            abs_url = urljoin(self.base_url, href)
            if page_re.search(urlparse(abs_url).path):
                found.append(abs_url)
        return found

    def _find_numbered_pagination(self, soup: BeautifulSoup):
        """Find numbered links inside pagination containers."""
        found = []
        # Look for containers matching pagination keywords
        for kw in PAGINATION_CONTAINER_KEYWORDS:
            containers = soup.find_all(
                attrs={"class": re.compile(kw, re.I)}
            )
            containers += soup.find_all(
                attrs={"id": re.compile(kw, re.I)}
            )
            for container in containers:
                for a in container.find_all("a", href=True):
                    href = a["href"].strip()
                    if href.startswith(("javascript:", "mailto:", "#")):
                        continue
                    text = a.get_text(strip=True)
                    # Numbered links: "2", "3", etc.
                    if text.isdigit():
                        found.append(urljoin(self.base_url, href))
                    # Or any link in pagination container
                    elif len(text) < 20:  # short text = likely pagination
                        found.append(urljoin(self.base_url, href))
        return found

    def _find_next_by_text(self, soup: BeautifulSoup):
        """Find anchor whose text matches common 'Next' patterns."""
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            if text in NEXT_BUTTON_TEXTS:
                href = a["href"].strip()
                if href and not href.startswith(("javascript:", "#")):
                    return urljoin(self.base_url, href)
        return None


# ===========================================================================
# ProfileDetector
# ===========================================================================

class ProfileDetector:
    """Identifies profile detail page links from a directory listing page."""

    def __init__(self, base_url: str, target_domain: str, include_subdomains: bool):
        self.base_url = base_url
        self.target_domain = target_domain
        self.include_subdomains = include_subdomains
        # Build regex from directory keywords for URL path matching
        self._keyword_re = re.compile(
            r"/(" + "|".join(DIRECTORY_KEYWORDS) + r")/",
            re.IGNORECASE,
        )

    def find_profile_links(self, html: str, page_url: str) -> list:
        """
        Extract links that appear to be profile detail pages.
        Returns list of absolute URLs.
        """
        soup = BeautifulSoup(html, "html.parser")
        profile_links = []

        # Strategy 1: Links inside containers with profile-related class names
        for kw in PROFILE_CONTAINER_KEYWORDS:
            containers = soup.find_all(
                attrs={"class": re.compile(kw, re.I)}
            )
            for container in containers:
                for a in container.find_all("a", href=True):
                    url = self._validate_link(a["href"], page_url)
                    if url:
                        profile_links.append(url)

        # Strategy 2: Links whose URL path contains directory keywords
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue
            abs_url = urljoin(page_url, href)
            parsed = urlparse(abs_url)
            if self._keyword_re.search(parsed.path):
                url = self._validate_link(href, page_url)
                if url:
                    profile_links.append(url)

        # Deduplicate
        seen = set()
        clean = []
        for url in profile_links:
            n = normalize_url(url)
            if n and n not in seen:
                seen.add(n)
                clean.append(n)
        return clean

    def _validate_link(self, href: str, page_url: str):
        """Validate and normalize a potential profile link."""
        href = href.strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")):
            return None
        abs_url = urljoin(page_url, href)
        n = normalize_url(abs_url)
        if not n:
            return None
        if not is_same_domain(n, self.target_domain, self.include_subdomains):
            return None
        if has_skippable_extension(n):
            return None
        return n


# ===========================================================================
# APIEndpointDetector
# ===========================================================================

class APIEndpointDetector:
    """Discovers public API/XHR endpoints from HTML source."""

    # Regex for common API URL patterns in inline scripts
    API_URL_RE = re.compile(
        r"""(?:["'])((?:https?://[^"'\s]+|/[\w\-./]+)(?:api|ajax|json|wp-json|rest|endpoint|search|directory|people|faculty|staff|load|fetch|query)[^"'\s]*?)(?:["'])""",
        re.IGNORECASE,
    )

    # Data attributes that may contain API endpoints
    DATA_ATTRS = [
        "data-url", "data-endpoint", "data-ajax-url", "data-api",
        "data-source", "data-href", "data-action", "data-load-url",
        "data-fetch-url", "data-request-url",
    ]

    def __init__(self, base_url: str, target_domain: str, include_subdomains: bool):
        self.base_url = base_url
        self.target_domain = target_domain
        self.include_subdomains = include_subdomains

    def discover(self, html: str) -> list:
        """
        Find public API endpoints in the page source.
        Returns list of {"url": str, "method": "GET"|"POST", "params": dict}.
        """
        soup = BeautifulSoup(html, "html.parser")
        endpoints = []

        # Strategy 1: data-* attributes on elements
        for attr in self.DATA_ATTRS:
            for el in soup.find_all(attrs={attr: True}):
                url = el[attr].strip()
                if url:
                    ep = self._validate_endpoint(url)
                    if ep:
                        endpoints.append(ep)

        # Strategy 2: Scan <script> tags for API URL patterns
        for script in soup.find_all("script"):
            if not script.string:
                continue
            text = script.string

            # Find URL-like strings containing API keywords
            for match in self.API_URL_RE.findall(text):
                ep = self._validate_endpoint(match)
                if ep:
                    endpoints.append(ep)

            # Try to find embedded JSON config objects
            self._extract_json_configs(text, endpoints)

        # Deduplicate by URL
        seen = set()
        clean = []
        for ep in endpoints:
            if ep["url"] not in seen:
                seen.add(ep["url"])
                clean.append(ep)
        return clean

    def _validate_endpoint(self, url: str):
        """Validate an API endpoint URL."""
        if not url or url.startswith(("javascript:", "mailto:", "#")):
            return None

        abs_url = urljoin(self.base_url, url)
        parsed = urlparse(abs_url)

        # Must be HTTP(S)
        if parsed.scheme not in ("http", "https"):
            return None

        # Must be same domain
        if not is_same_domain(abs_url, self.target_domain, self.include_subdomains):
            return None

        return {"url": abs_url, "method": "GET", "params": {}}

    def _extract_json_configs(self, script_text: str, endpoints: list):
        """Try to find JSON config objects with API URLs in script text."""
        # Look for patterns like: var config = {...}; or window.settings = {...};
        json_re = re.compile(r"[{]\s*['\"](?:api|ajax|endpoint|url|base)['\"]", re.I)
        if not json_re.search(script_text):
            return

        # Try to extract JSON-like objects
        brace_depth = 0
        start = None
        for i, ch in enumerate(script_text):
            if ch == "{":
                if brace_depth == 0:
                    start = i
                brace_depth += 1
            elif ch == "}":
                brace_depth -= 1
                if brace_depth == 0 and start is not None:
                    candidate = script_text[start:i + 1]
                    if len(candidate) < 5000:  # safety limit
                        try:
                            # Try parsing with relaxed JSON
                            obj = json.loads(candidate)
                            self._scan_json_for_urls(obj, endpoints)
                        except json.JSONDecodeError:
                            pass
                    start = None

    def _scan_json_for_urls(self, obj, endpoints: list, depth: int = 0):
        """Recursively scan a parsed JSON object for URL values."""
        if depth > 5:
            return
        if isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(val, str) and ("url" in key.lower() or "endpoint" in key.lower() or "api" in key.lower()):
                    ep = self._validate_endpoint(val)
                    if ep:
                        endpoints.append(ep)
                elif isinstance(val, (dict, list)):
                    self._scan_json_for_urls(val, endpoints, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:50]:  # limit
                self._scan_json_for_urls(item, endpoints, depth + 1)


# ===========================================================================
# DirectoryCrawler — main crawler for directory extraction mode
# ===========================================================================

class DirectoryCrawler(DomainCrawler):
    """
    Specialized crawler for paginated directory/listing pages.

    3-phase workflow:
      Phase 1: Analyze starting page — detect pagination, API endpoints, profiles
      Phase 2: Traverse all listing/directory pages — collect emails + profile links
      Phase 3: Visit profile detail pages — collect emails

    Reuses DomainCrawler's session, domain checks, email extraction, and CSV output.
    """

    def __init__(self, job: dict, stop_event: threading.Event):
        super().__init__(job, stop_event)

        # Directory-specific limits
        self.max_directory_pages: int = job.get("max_directory_pages", 100)
        self.max_profile_pages: int = job.get("max_profile_pages", 500)
        self.max_emails: int = job.get("max_emails", 5000)

        # Counters
        self.listing_pages: set = set()       # visited listing page URLs
        self.profile_pages: set = set()       # visited profile page URLs
        self.profile_queue: deque = deque()   # profile URLs to visit
        self.discovered_profiles: set = set() # all discovered profile URLs
        self.listing_queue: deque = deque()   # listing page URLs to visit
        self.discovered_listings: set = set() # all discovered listing URLs
        self.api_endpoints: list = []         # discovered API endpoints

        # Detection state
        self.pagination_type: str = "none"

        # Detectors
        self.pagination_detector = PaginationDetector(
            self.target_url, self.target_domain, self.include_subdomains
        )
        self.profile_detector = ProfileDetector(
            self.target_url, self.target_domain, self.include_subdomains
        )
        self.api_detector = APIEndpointDetector(
            self.target_url, self.target_domain, self.include_subdomains
        )

    # ---- Override run() for 3-phase workflow --------------------------------

    def run(self):
        """Execute the 3-phase directory extraction."""
        self.job["status"] = "running"

        try:
            # Phase 1: Analyze starting page
            logger.info("Phase 1: Analyzing starting page %s", self.target_url)
            self.job["current_page"] = self.target_url
            starting_html = self._fetch_page(self.target_url)

            if starting_html is None:
                self.job["status"] = "completed"
                self._build_results()
                self._write_outputs()
                return

            self.listing_pages.add(self.target_url)
            self.visited.add(self.target_url)

            # Extract emails from starting page
            self._extract_and_store_emails(starting_html, self.target_url)

            # Detect pagination
            detection = self.pagination_detector.detect(starting_html)
            self.pagination_type = detection["type"]
            self.job["pagination_type"] = self.pagination_type
            logger.info("Pagination detected: %s (found %d urls, next=%s)",
                        detection["type"], len(detection["urls"]), detection.get("next_url"))

            # Queue discovered pagination URLs
            for url in detection["urls"]:
                if url not in self.visited:
                    self.listing_queue.append(url)
                    self.discovered_listings.add(url)

            if detection["next_url"] and detection["next_url"] not in self.visited:
                self.listing_queue.appendleft(detection["next_url"])
                self.discovered_listings.add(detection["next_url"])

            # Detect profile links
            profiles = self.profile_detector.find_profile_links(starting_html, self.target_url)
            for url in profiles:
                if url not in self.visited and url not in self.discovered_profiles:
                    self.profile_queue.append(url)
                    self.discovered_profiles.add(url)

            # Detect API endpoints
            self.api_endpoints = self.api_detector.discover(starting_html)
            if self.api_endpoints:
                logger.info("Discovered %d API endpoints", len(self.api_endpoints))

            self._update_dir_status()

            # Phase 2: Traverse listing pages
            logger.info("Phase 2: Traversing listing pages")
            self._phase2_traverse_listings()

            # Phase 2b: Try API endpoints
            if self.api_endpoints and not self.stop_event.is_set():
                self._phase2b_query_apis()

            # Phase 3: Visit profile pages
            logger.info("Phase 3: Visiting profile pages (%d queued)", len(self.profile_queue))
            self._phase3_visit_profiles()

        except Exception as exc:
            logger.exception("Fatal directory crawl error")
            self.errors.append(f"Fatal: {exc}")

        # Finalize
        final_status = "stopped" if self.stop_event.is_set() else "completed"
        self.job["status"] = final_status
        self.job["current_page"] = ""
        self._build_results()
        self._write_outputs()

    # ---- Phase 2: Traverse listing/directory pages --------------------------

    def _phase2_traverse_listings(self):
        """Visit all listing (pagination) pages — extract emails and discover more profiles."""
        while self.listing_queue and not self.stop_event.is_set():
            if len(self.listing_pages) >= self.max_directory_pages:
                logger.info("Reached max directory pages (%d)", self.max_directory_pages)
                break
            if len(self.emails) >= self.max_emails:
                break

            url = self.listing_queue.popleft()
            if url in self.visited:
                continue
            if has_skippable_extension(url):
                continue

            self.job["current_page"] = url
            self._update_dir_status()

            html = self._fetch_page(url)
            if html is None:
                continue

            self.listing_pages.add(url)
            self.visited.add(url)

            # Extract emails
            self._extract_and_store_emails(html, url)

            # Discover more pagination URLs (follow "Next" chains)
            detection = self.pagination_detector.detect(html)
            if detection["next_url"] and detection["next_url"] not in self.visited:
                if detection["next_url"] not in self.discovered_listings:
                    self.listing_queue.append(detection["next_url"])
                    self.discovered_listings.add(detection["next_url"])

            for purl in detection["urls"]:
                if purl not in self.visited and purl not in self.discovered_listings:
                    self.listing_queue.append(purl)
                    self.discovered_listings.add(purl)

            # Discover profile links
            profiles = self.profile_detector.find_profile_links(html, url)
            for purl in profiles:
                if purl not in self.visited and purl not in self.discovered_profiles:
                    self.profile_queue.append(purl)
                    self.discovered_profiles.add(purl)

            self._update_dir_status()

            if self.crawl_delay > 0:
                time.sleep(self.crawl_delay)

    # ---- Phase 2b: Query discovered API endpoints ----------------------------

    def _phase2b_query_apis(self):
        """Try discovered API endpoints, paginating through them."""
        for ep in self.api_endpoints:
            if self.stop_event.is_set():
                break
            if len(self.emails) >= self.max_emails:
                break

            url = ep["url"]
            self.job["current_page"] = f"[API] {url}"
            self._update_dir_status()

            try:
                # Try paginating the API (page=1,2,3...)
                empty_streak = 0
                for page_num in range(1, self.max_directory_pages + 1):
                    if self.stop_event.is_set() or len(self.emails) >= self.max_emails:
                        break

                    # Build paginated URL
                    parsed = urlparse(url)
                    qs = parse_qs(parsed.query)
                    qs["page"] = [str(page_num)]
                    paginated_url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

                    if paginated_url in self.visited:
                        continue

                    try:
                        resp = self.session.get(paginated_url, timeout=15)
                        self.visited.add(paginated_url)

                        if resp.status_code != 200:
                            empty_streak += 1
                            if empty_streak >= 3:
                                break
                            continue

                        content = resp.text
                        content_type = resp.headers.get("Content-Type", "")

                        # Extract emails from response
                        if "json" in content_type or "javascript" in content_type:
                            found = extract_emails_from_json(content)
                            # Also look for profile URLs in JSON
                            self._extract_profile_urls_from_json(content)
                        else:
                            found = extract_emails_from_html(content)

                        if found:
                            empty_streak = 0
                            for email in found:
                                if email not in self.emails:
                                    self.emails[email] = f"[API] {paginated_url}"
                        else:
                            empty_streak += 1

                        # Stop if 3 consecutive empty pages
                        if empty_streak >= 3:
                            break

                        self._update_dir_status()

                        if self.crawl_delay > 0:
                            time.sleep(self.crawl_delay)

                    except requests.RequestException as exc:
                        self.errors.append(f"API {paginated_url} — {exc}")
                        empty_streak += 1
                        if empty_streak >= 3:
                            break

            except Exception as exc:
                self.errors.append(f"API endpoint error: {exc}")

    # ---- Phase 3: Visit profile detail pages --------------------------------

    def _phase3_visit_profiles(self):
        """Visit profile detail pages to extract emails."""
        while self.profile_queue and not self.stop_event.is_set():
            if len(self.profile_pages) >= self.max_profile_pages:
                logger.info("Reached max profile pages (%d)", self.max_profile_pages)
                break
            if len(self.emails) >= self.max_emails:
                break

            url = self.profile_queue.popleft()
            if url in self.visited:
                continue
            if has_skippable_extension(url):
                continue

            self.job["current_page"] = url
            self._update_dir_status()

            html = self._fetch_page(url)
            if html is None:
                continue

            self.profile_pages.add(url)
            self.visited.add(url)

            # Extract emails from profile page
            self._extract_and_store_emails(html, url)

            self._update_dir_status()

            if self.crawl_delay > 0:
                time.sleep(self.crawl_delay)

    # ---- Helpers ------------------------------------------------------------

    def _fetch_page(self, url: str):
        """Fetch a page with retries. Returns HTML string or None."""
        retries = 2
        for attempt in range(retries + 1):
            try:
                resp = self.session.get(url, timeout=15, allow_redirects=True)
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" not in content_type and "application/xhtml" not in content_type:
                    self.visited.add(url)
                    return None
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as exc:
                if attempt == retries:
                    self.errors.append(f"{url} — {exc}")
                    self.visited.add(url)
                    return None
                time.sleep(1)
        return None

    def _extract_and_store_emails(self, html: str, source_url: str):
        """Extract emails from HTML and store them."""
        found = extract_emails_from_html(html)
        for email in found:
            if email not in self.emails:
                self.emails[email] = source_url

    def _extract_profile_urls_from_json(self, text: str):
        """Try to find profile URLs in a JSON response."""
        try:
            data = json.loads(text)
            self._scan_json_for_profile_urls(data)
        except json.JSONDecodeError:
            pass

    def _scan_json_for_profile_urls(self, obj, depth: int = 0):
        """Recursively scan JSON for profile URL strings."""
        if depth > 4:
            return
        if isinstance(obj, dict):
            for key, val in obj.items():
                if isinstance(val, str) and ("url" in key.lower() or "link" in key.lower() or "href" in key.lower()):
                    n = normalize_url(val)
                    if n and is_same_domain(n, self.target_domain, self.include_subdomains):
                        parsed = urlparse(n)
                        if any(kw in parsed.path.lower() for kw in DIRECTORY_KEYWORDS):
                            if n not in self.visited and n not in self.discovered_profiles:
                                self.profile_queue.append(n)
                                self.discovered_profiles.add(n)
                elif isinstance(val, (dict, list)):
                    self._scan_json_for_profile_urls(val, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:100]:
                self._scan_json_for_profile_urls(item, depth + 1)

    def _update_dir_status(self):
        """Update job dict with directory-specific stats."""
        self.job["pages_visited"] = len(self.visited)
        self.job["pages_queued"] = len(self.listing_queue) + len(self.profile_queue)
        self.job["unique_emails_found"] = len(self.emails)
        self.job["directory_pages_found"] = len(self.discovered_listings) + 1  # +1 for start page
        self.job["profile_pages_found"] = len(self.discovered_profiles)
        self.job["listing_pages_visited"] = len(self.listing_pages)
        self.job["profile_pages_visited"] = len(self.profile_pages)
        self.job["pagination_type"] = self.pagination_type

    # ---- Results & output overrides -----------------------------------------

    def _build_results(self):
        """Store email results on the job dict."""
        self.job["results"] = [
            {"email": email, "source_page": source}
            for email, source in sorted(self.emails.items())
        ]
        self._update_dir_status()
        self.job["errors"] = self.errors

    def _write_outputs(self):
        """Write CSV with directory results."""
        self._write_csv()
