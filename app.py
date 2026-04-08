"""
Domain Email Scraper — Flask Application
Extended with Sitemap Generator and Full Crawl modes.
"""

import logging
import os
import uuid
import threading
from typing import Optional

from flask import Flask, jsonify, request, render_template, send_file

from crawler import DomainCrawler, normalize_url, DOWNLOADS_DIR
from sitemap_crawler import SitemapCrawler, FullCrawler
from directory_crawler import DirectoryCrawler

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = Flask(__name__)

# In-memory store  — { job_id: job_dict }
jobs: dict = {}
active_job_id: Optional[str] = None
active_thread: Optional[threading.Thread] = None
active_stop_event: Optional[threading.Event] = None
active_crawler: Optional[DomainCrawler] = None


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_job(
    target_url: str,
    max_pages: int,
    include_subdomains: bool,
    crawl_delay: float,
    job_type: str = "email",
    # Directory-specific
    max_directory_pages: int = 100,
    max_profile_pages: int = 500,
    max_emails: int = 5000,
) -> dict:
    """Create a blank job dictionary."""
    return {
        "job_id": str(uuid.uuid4()),
        "job_type": job_type,            # "email" | "sitemap" | "full" | "directory"
        "target_url": target_url,
        "max_pages": max_pages,
        "include_subdomains": include_subdomains,
        "crawl_delay": crawl_delay,
        "status": "pending",
        "pages_visited": 0,
        "pages_queued": 0,
        "current_page": "",
        # Email-related
        "unique_emails_found": 0,
        "results": [],
        "csv_path": None,
        # Sitemap-related
        "unique_urls_found": 0,
        "sitemap_urls": [],
        "sitemap_path": None,
        "url_list_path": None,
        # Directory-specific
        "max_directory_pages": max_directory_pages,
        "max_profile_pages": max_profile_pages,
        "max_emails": max_emails,
        "directory_pages_found": 0,
        "profile_pages_found": 0,
        "listing_pages_visited": 0,
        "profile_pages_visited": 0,
        "pagination_type": "none",
        # Errors
        "errors": [],
    }


def _parse_crawl_params(data: dict):
    """Extract and validate common crawl parameters from request JSON."""
    raw_url = data.get("url", "").strip()
    if not raw_url:
        return None, "URL is required."

    target_url = normalize_url(raw_url)
    if not target_url:
        return None, "Invalid URL."

    max_pages = min(int(data.get("max_pages", 100)), 10000)
    include_subdomains = bool(data.get("include_subdomains", False))
    crawl_delay = max(float(data.get("crawl_delay", 0.5)), 0.0)

    return {
        "target_url": target_url,
        "max_pages": max_pages,
        "include_subdomains": include_subdomains,
        "crawl_delay": crawl_delay,
    }, None


def _launch_crawl(job: dict, crawler_class):
    """Create crawler, launch in background thread, store globals."""
    global active_job_id, active_thread, active_stop_event, active_crawler

    job_id = job["job_id"]
    jobs[job_id] = job
    active_job_id = job_id

    stop_event = threading.Event()
    active_stop_event = stop_event
    crawler = crawler_class(job, stop_event)
    active_crawler = crawler
    thread = threading.Thread(target=crawler.run, daemon=True)
    active_thread = thread
    thread.start()

    logger.info("Launched %s job %s for %s", job["job_type"], job_id, job["target_url"])
    return job_id


# ---------------------------------------------------------------------------
# Page route
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.route("/start-scrape", methods=["POST"])
def start_scrape():
    """Start an email-only crawl."""
    if active_job_id and jobs.get(active_job_id, {}).get("status") == "running":
        return jsonify({"error": "A crawl is already running. Stop it first."}), 409

    data = request.get_json(force=True)
    params, err = _parse_crawl_params(data)
    if err:
        return jsonify({"error": err}), 400

    job = _make_job(**params, job_type="email")
    job_id = _launch_crawl(job, DomainCrawler)

    return jsonify({"job_id": job_id, "status": "started", "job_type": "email"}), 200


@app.route("/start-sitemap", methods=["POST"])
def start_sitemap():
    """Start a sitemap-only crawl."""
    if active_job_id and jobs.get(active_job_id, {}).get("status") == "running":
        return jsonify({"error": "A crawl is already running. Stop it first."}), 409

    data = request.get_json(force=True)
    params, err = _parse_crawl_params(data)
    if err:
        return jsonify({"error": err}), 400

    job = _make_job(**params, job_type="sitemap")
    job_id = _launch_crawl(job, SitemapCrawler)

    return jsonify({"job_id": job_id, "status": "started", "job_type": "sitemap"}), 200


@app.route("/start-full", methods=["POST"])
def start_full():
    """Start a combined crawl (emails + sitemap)."""
    if active_job_id and jobs.get(active_job_id, {}).get("status") == "running":
        return jsonify({"error": "A crawl is already running. Stop it first."}), 409

    data = request.get_json(force=True)
    params, err = _parse_crawl_params(data)
    if err:
        return jsonify({"error": err}), 400

    job = _make_job(**params, job_type="full")
    job_id = _launch_crawl(job, FullCrawler)

    return jsonify({"job_id": job_id, "status": "started", "job_type": "full"}), 200


@app.route("/start-directory", methods=["POST"])
def start_directory():
    """Start a directory extraction crawl."""
    if active_job_id and jobs.get(active_job_id, {}).get("status") == "running":
        return jsonify({"error": "A crawl is already running. Stop it first."}), 409

    data = request.get_json(force=True)
    params, err = _parse_crawl_params(data)
    if err:
        return jsonify({"error": err}), 400

    max_dir = min(int(data.get("max_directory_pages", 100)), 1000)
    max_prof = min(int(data.get("max_profile_pages", 500)), 5000)
    max_em = min(int(data.get("max_emails", 5000)), 50000)

    job = _make_job(**params, job_type="directory",
                    max_directory_pages=max_dir,
                    max_profile_pages=max_prof,
                    max_emails=max_em)
    job_id = _launch_crawl(job, DirectoryCrawler)

    return jsonify({"job_id": job_id, "status": "started", "job_type": "directory"}), 200


@app.route("/stop-scrape", methods=["POST"])
def stop_scrape():
    """Send stop signal to the active crawl (any type)."""
    if not active_job_id:
        return jsonify({"error": "No active crawl to stop."}), 404

    job = jobs.get(active_job_id)
    if not job or job["status"] != "running":
        return jsonify({"error": "No running crawl to stop."}), 404

    if active_stop_event:
        active_stop_event.set()

    return jsonify({"message": "Stop signal sent.", "job_id": active_job_id}), 200


@app.route("/status", methods=["GET"])
def get_status():
    if not active_job_id:
        return jsonify({"status": "idle"}), 200

    job = jobs.get(active_job_id)
    if not job:
        return jsonify({"status": "idle"}), 200

    return jsonify({
        "job_id": job["job_id"],
        "job_type": job["job_type"],
        "target_url": job["target_url"],
        "status": job["status"],
        "pages_visited": job["pages_visited"],
        "pages_queued": job["pages_queued"],
        "current_page": job["current_page"],
        "unique_emails_found": job["unique_emails_found"],
        "unique_urls_found": job["unique_urls_found"],
        "error_count": len(job.get("errors", [])),
        "has_sitemap": job.get("sitemap_path") is not None,
        "has_csv": job.get("csv_path") is not None,
        # Directory-specific
        "directory_pages_found": job.get("directory_pages_found", 0),
        "profile_pages_found": job.get("profile_pages_found", 0),
        "listing_pages_visited": job.get("listing_pages_visited", 0),
        "profile_pages_visited": job.get("profile_pages_visited", 0),
        "pagination_type": job.get("pagination_type", "none"),
    }), 200


@app.route("/results", methods=["GET"])
def get_results():
    if not active_job_id:
        return jsonify({"results": [], "sitemap_urls": [], "errors": []}), 200

    job = jobs.get(active_job_id)
    if not job:
        return jsonify({"results": [], "sitemap_urls": [], "errors": []}), 200

    job_type = job.get("job_type", "email")

    # Build live email results during crawl
    if job["status"] == "running" and active_crawler is not None:
        if job_type in ("email", "full", "directory"):
            results = [
                {"email": email, "source_page": source}
                for email, source in sorted(active_crawler.emails.items())
            ]
        else:
            results = []

        sitemap_urls = sorted(active_crawler.sitemap_urls) if job_type in ("sitemap", "full") else []
    else:
        results = job.get("results", [])
        sitemap_urls = job.get("sitemap_urls", [])

    return jsonify({
        "results": results,
        "sitemap_urls": sitemap_urls,
        "errors": job.get("errors", []),
    }), 200


@app.route("/download/<job_id>", methods=["GET"])
def download_csv(job_id):
    """Download the CSV file for email results."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404

    csv_path = job.get("csv_path")
    if not csv_path or not os.path.isfile(csv_path):
        return jsonify({"error": "CSV not yet available."}), 404

    return send_file(
        csv_path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"emails_{job_id[:8]}.csv",
    )


@app.route("/download/sitemap/<job_id>", methods=["GET"])
def download_sitemap(job_id):
    """Download the XML sitemap file."""
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404

    sitemap_path = job.get("sitemap_path")
    if not sitemap_path or not os.path.isfile(sitemap_path):
        return jsonify({"error": "Sitemap XML not yet available."}), 404

    return send_file(
        sitemap_path,
        mimetype="application/xml",
        as_attachment=True,
        download_name=f"sitemap_{job_id[:8]}.xml",
    )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    app.run(debug=True, host="127.0.0.1", port=5000)
