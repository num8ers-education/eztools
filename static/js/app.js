/* ============================================================
   Domain Email Scraper — Frontend Logic
   Extended with Sitemap, Full Crawl, & Directory Extraction
   ============================================================ */

// --------------- State --------------------------------------------------
let currentJobId = null;
let currentJobType = null;   // "email" | "sitemap" | "full" | "directory"
let pollingTimer = null;
const POLL_INTERVAL = 1000; // ms

// --------------- DOM refs -----------------------------------------------
const $urlInput        = document.getElementById("url-input");
const $maxPages        = document.getElementById("max-pages");
const $crawlDelay      = document.getElementById("crawl-delay");
const $includeSubdom   = document.getElementById("include-subdomains");
const $btnStart        = document.getElementById("btn-start");
const $btnSitemap      = document.getElementById("btn-sitemap");
const $btnFull         = document.getElementById("btn-full");
const $btnDirectory    = document.getElementById("btn-directory");
const $btnStop         = document.getElementById("btn-stop");
const $btnDownload     = document.getElementById("btn-download");
const $btnDownloadSm   = document.getElementById("btn-download-sitemap");

const $directoryOpts   = document.getElementById("directory-options");
const $maxDirPages     = document.getElementById("max-dir-pages");
const $maxProfPages    = document.getElementById("max-profile-pages");
const $maxEmails       = document.getElementById("max-emails");

const $statusSection   = document.getElementById("status-section");
const $statusDot       = document.getElementById("status-dot");
const $statusText      = document.getElementById("status-text");
const $statusBadge     = document.getElementById("status-badge");
const $statusUrl       = document.getElementById("status-current-url");
const $progressFill    = document.getElementById("progress-fill");
const $paginationInd   = document.getElementById("pagination-indicator");

const $metricsSection  = document.getElementById("metrics-section");
const $metricPages     = document.getElementById("metric-pages");
const $metricQueued    = document.getElementById("metric-queued");
const $metricEmails    = document.getElementById("metric-emails");
const $metricUrls      = document.getElementById("metric-urls");
const $metricErrors    = document.getElementById("metric-errors");
const $metricEmailCard = document.getElementById("metric-emails-card");
const $metricUrlsCard  = document.getElementById("metric-urls-card");

const $dirMetrics      = document.getElementById("dir-metrics-section");
const $dirListings     = document.getElementById("dir-metric-listings");
const $dirProfiles     = document.getElementById("dir-metric-profiles");
const $dirEmails       = document.getElementById("dir-metric-emails");
const $dirDiscovered   = document.getElementById("dir-metric-discovered");
const $dirQueued       = document.getElementById("dir-metric-queued");
const $dirErrors       = document.getElementById("dir-metric-errors");

const $resultsSection  = document.getElementById("results-section");
const $resultsBody     = document.getElementById("results-body");
const $emptyState      = document.getElementById("empty-state");

const $sitemapSection  = document.getElementById("sitemap-section");
const $sitemapBody     = document.getElementById("sitemap-body");
const $sitemapEmpty    = document.getElementById("sitemap-empty-state");

const $errorsSection   = document.getElementById("errors-section");
const $errorsList      = document.getElementById("errors-list");

// --------------- Build payload ------------------------------------------
function getPayload() {
    return {
        url: $urlInput.value.trim(),
        max_pages: parseInt($maxPages.value, 10) || 100,
        crawl_delay: parseFloat($crawlDelay.value) || 0.5,
        include_subdomains: $includeSubdom.checked,
    };
}

function getDirectoryPayload() {
    const base = getPayload();
    base.max_directory_pages = parseInt($maxDirPages.value, 10) || 100;
    base.max_profile_pages = parseInt($maxProfPages.value, 10) || 500;
    base.max_emails = parseInt($maxEmails.value, 10) || 5000;
    return base;
}

// --------------- Start functions ----------------------------------------
async function startScrape() {
    await launchCrawl("/start-scrape", "email", getPayload());
}

async function startSitemap() {
    await launchCrawl("/start-sitemap", "sitemap", getPayload());
}

async function startFull() {
    await launchCrawl("/start-full", "full", getPayload());
}

async function startDirectory() {
    await launchCrawl("/start-directory", "directory", getDirectoryPayload());
}

async function launchCrawl(endpoint, jobType, payload) {
    if (!payload) payload = getPayload();
    if (!payload.url) {
        shakeElement($urlInput);
        return;
    }

    try {
        const resp = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok) {
            alert(data.error || "Failed to start crawl.");
            return;
        }

        currentJobId = data.job_id;
        currentJobType = jobType;
        setUIState("running");
        startPolling();

    } catch (err) {
        alert("Network error: " + err.message);
    }
}

// --------------- Stop Crawl ---------------------------------------------
async function stopScrape() {
    try {
        await fetch("/stop-scrape", { method: "POST" });
    } catch (err) {
        console.error("Stop error:", err);
    }
}

// --------------- Polling ------------------------------------------------
function startPolling() {
    stopPolling();
    poll();  // immediate first call
    pollingTimer = setInterval(poll, POLL_INTERVAL);
}

function stopPolling() {
    if (pollingTimer) {
        clearInterval(pollingTimer);
        pollingTimer = null;
    }
}

async function poll() {
    try {
        // Fetch status
        const statusResp = await fetch("/status");
        const status = await statusResp.json();

        // Update job type from server
        if (status.job_type) currentJobType = status.job_type;

        updateStatus(status);

        // Fetch results
        const resultsResp = await fetch("/results");
        const resultsData = await resultsResp.json();

        updateResults(resultsData.results || []);
        updateSitemapResults(resultsData.sitemap_urls || []);
        updateErrors(resultsData.errors || []);

        // Stop polling if crawl is done
        if (status.status === "completed" || status.status === "stopped") {
            stopPolling();
            setUIState("done");
        }

    } catch (err) {
        console.error("Polling error:", err);
    }
}

// --------------- UI Updates ---------------------------------------------
function updateStatus(status) {
    $statusText.textContent = status.status || "Idle";

    // Dot color
    $statusDot.className = "status-indicator";
    if (status.status) $statusDot.classList.add(status.status);

    // Badge
    const badgeLabels = { email: "Emails", sitemap: "Sitemap", full: "Full Crawl", directory: "Directory" };
    $statusBadge.textContent = badgeLabels[status.job_type] || "";
    $statusBadge.className = "status-badge";
    if (status.job_type) $statusBadge.classList.add("status-badge--" + status.job_type);

    $statusUrl.textContent = status.current_page || "";

    // Pagination indicator (directory mode)
    if (status.job_type === "directory" && status.pagination_type && status.pagination_type !== "none") {
        $paginationInd.textContent = "Pagination: " + status.pagination_type.replace("_", " ");
        $paginationInd.classList.remove("hidden");
    } else {
        $paginationInd.classList.add("hidden");
    }

    // Standard metrics
    if (status.job_type !== "directory") {
        $metricPages.textContent  = status.pages_visited ?? 0;
        $metricQueued.textContent  = status.pages_queued ?? 0;
        $metricEmails.textContent  = status.unique_emails_found ?? 0;
        $metricUrls.textContent    = status.unique_urls_found ?? 0;
        $metricErrors.textContent  = status.error_count ?? 0;
    }

    // Directory metrics
    if (status.job_type === "directory") {
        $dirListings.textContent   = status.listing_pages_visited ?? 0;
        $dirProfiles.textContent   = status.profile_pages_visited ?? 0;
        $dirEmails.textContent     = status.unique_emails_found ?? 0;
        $dirDiscovered.textContent = status.profile_pages_found ?? 0;
        $dirQueued.textContent     = status.pages_queued ?? 0;
        $dirErrors.textContent     = status.error_count ?? 0;
    }

    // Progress bar (approximate)
    const maxPages = parseInt($maxPages.value, 10) || 100;
    const pct = Math.min(((status.pages_visited || 0) / maxPages) * 100, 100);
    $progressFill.style.width = pct + "%";
}

function updateResults(results) {
    if (!results.length) {
        $emptyState.classList.remove("hidden");
        return;
    }
    $emptyState.classList.add("hidden");

    // Only re-render if count changed
    if ($resultsBody.childElementCount === results.length) return;

    $resultsBody.innerHTML = "";
    results.forEach((item, idx) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${idx + 1}</td>
            <td>${escapeHtml(item.email)}</td>
            <td><a href="${escapeHtml(item.source_page)}" target="_blank" rel="noopener">${escapeHtml(item.source_page)}</a></td>
        `;
        $resultsBody.appendChild(tr);
    });
}

function updateSitemapResults(urls) {
    if (!urls.length) {
        $sitemapEmpty.classList.remove("hidden");
        return;
    }
    $sitemapEmpty.classList.add("hidden");

    // Only re-render if count changed
    if ($sitemapBody.childElementCount === urls.length) return;

    $sitemapBody.innerHTML = "";
    urls.forEach((url, idx) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${idx + 1}</td>
            <td><a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(url)}</a></td>
        `;
        $sitemapBody.appendChild(tr);
    });
}

function updateErrors(errors) {
    if (!errors.length) {
        $errorsSection.classList.add("hidden");
        return;
    }
    $errorsSection.classList.remove("hidden");
    $errorsList.innerHTML = "";
    errors.forEach((msg) => {
        const li = document.createElement("li");
        li.textContent = msg;
        $errorsList.appendChild(li);
    });
}

function setUIState(state) {
    const isDirectory = currentJobType === "directory";

    if (state === "running") {
        // Disable all start buttons, enable stop
        $btnStart.disabled = true;
        $btnSitemap.disabled = true;
        $btnFull.disabled = true;
        $btnDirectory.disabled = true;
        $btnStop.disabled = false;
        $btnDownload.disabled = true;
        $btnDownloadSm.disabled = true;

        // Show status always
        $statusSection.classList.remove("hidden");

        // Show correct metrics section
        if (isDirectory) {
            $metricsSection.classList.add("hidden");
            $dirMetrics.classList.remove("hidden");
        } else {
            $metricsSection.classList.remove("hidden");
            $dirMetrics.classList.add("hidden");
        }

        // Show/hide result sections based on job type
        const showEmails  = currentJobType === "email" || currentJobType === "full" || isDirectory;
        const showSitemap = currentJobType === "sitemap" || currentJobType === "full";

        $resultsSection.classList.toggle("hidden", !showEmails);
        $sitemapSection.classList.toggle("hidden", !showSitemap);

        // Show/dim metric cards based on relevance (standard metrics only)
        if (!isDirectory) {
            $metricEmailCard.classList.toggle("metric-card--dim", !showEmails);
            $metricUrlsCard.classList.toggle("metric-card--dim", !showSitemap);
        }

        // Show directory options if directory mode
        $directoryOpts.classList.toggle("hidden", !isDirectory);

        // Clear previous results
        $resultsBody.innerHTML = "";
        $emptyState.classList.remove("hidden");
        $sitemapBody.innerHTML = "";
        $sitemapEmpty.classList.remove("hidden");
        $errorsList.innerHTML = "";
        $errorsSection.classList.add("hidden");

    } else if (state === "done") {
        $btnStart.disabled = false;
        $btnSitemap.disabled = false;
        $btnFull.disabled = false;
        $btnDirectory.disabled = false;
        $btnStop.disabled = true;

        // Enable download buttons based on what was generated
        const showEmails  = currentJobType === "email" || currentJobType === "full" || isDirectory;
        const showSitemap = currentJobType === "sitemap" || currentJobType === "full";
        $btnDownload.disabled = !showEmails;
        $btnDownloadSm.disabled = !showSitemap;
    }
}

// --------------- Downloads -----------------------------------------------
function downloadCSV() {
    if (!currentJobId) return;
    window.location.href = `/download/${currentJobId}`;
}

function downloadSitemap() {
    if (!currentJobId) return;
    window.location.href = `/download/sitemap/${currentJobId}`;
}

// --------------- Directory Options Toggle --------------------------------
// Show directory options when hovering the directory button (desktop hint)
if ($btnDirectory) {
    $btnDirectory.addEventListener("mouseenter", () => {
        $directoryOpts.classList.remove("hidden");
    });
}

// --------------- Helpers ------------------------------------------------
function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

function shakeElement(el) {
    el.style.animation = "none";
    void el.offsetHeight; // reflow
    el.style.animation = "shake 0.4s ease";
    setTimeout(() => { el.style.animation = ""; }, 500);
}

// Inject shake keyframes
const shakeStyle = document.createElement("style");
shakeStyle.textContent = `
@keyframes shake {
    0%, 100% { transform: translateX(0); }
    20% { transform: translateX(-6px); }
    40% { transform: translateX(6px); }
    60% { transform: translateX(-4px); }
    80% { transform: translateX(4px); }
}
`;
document.head.appendChild(shakeStyle);
