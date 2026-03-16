"""
SaveMyExams — IGCSE CIE Revision Notes URL Scraper
Robust edition: retries, multiple URL candidates, checkpointing, anti-bot delays.

Install:
    pip install playwright
    playwright install chromium
"""

import asyncio
import json
import random
from collections import deque
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.savemyexams.com"
DOWNLOADS  = Path.home() / "Downloads"
CHECKPOINT = DOWNLOADS / "revision_notes_checkpoint.json"

# ─── Tuning ───────────────────────────────────────────────────────────────────
MAX_RETRIES        = 4      # retries per page before giving up
RETRY_BASE_DELAY   = 2.0    # seconds (doubles each retry)
PAGE_LOAD_TIMEOUT  = 25_000 # ms
BETWEEN_PAGES_MIN  = 0.4    # random delay range between requests (seconds)
BETWEEN_PAGES_MAX  = 1.2
MAX_ERRORS_BEFORE_RESTART = 8  # recreate the browser context after this many consecutive errors
TIMEZONE           = "UTC"          # set to your local timezone e.g. "America/New_York", "Europe/London"

# ─── Subject definitions ───────────────────────────────────────────────────────
# Each subject has an ordered list of (start_url, prefix) candidates.
# The scraper tries each candidate in order and uses the first that resolves.
# Multiple year variants are listed so the script stays useful as years roll over.

SUBJECTS: dict[str, list[tuple[str, str]]] = {

    "English First Language (0500)": [
        (f"{BASE_URL}/igcse/english-language/cie/25/revision-notes/",           f"{BASE_URL}/igcse/english-language/cie/25/"),
        (f"{BASE_URL}/igcse/english-language/cie/23/revision-notes/",           f"{BASE_URL}/igcse/english-language/cie/23/"),
    ],

    "Mathematics (0580) — Extended": [
        (f"{BASE_URL}/igcse/maths/cie/25/extended/revision-notes/",             f"{BASE_URL}/igcse/maths/cie/25/extended/"),
        (f"{BASE_URL}/igcse/maths/cie/23/extended/revision-notes/",             f"{BASE_URL}/igcse/maths/cie/23/extended/"),
    ],

    "Mathematics (0580) — Core": [
        (f"{BASE_URL}/igcse/maths/cie/25/core/revision-notes/",                 f"{BASE_URL}/igcse/maths/cie/25/core/"),
        (f"{BASE_URL}/igcse/maths/cie/23/core/revision-notes/",                 f"{BASE_URL}/igcse/maths/cie/23/core/"),
    ],

    "Computer Science (0478)": [
        (f"{BASE_URL}/igcse/computer-science/cie/25/revision-notes/",           f"{BASE_URL}/igcse/computer-science/cie/25/"),
        (f"{BASE_URL}/igcse/computer-science/cie/23/revision-notes/",           f"{BASE_URL}/igcse/computer-science/cie/23/"),
    ],

    "Physics (0625)": [
        (f"{BASE_URL}/igcse/physics/cie/25/revision-notes/",                    f"{BASE_URL}/igcse/physics/cie/25/"),
        (f"{BASE_URL}/igcse/physics/cie/23/revision-notes/",                    f"{BASE_URL}/igcse/physics/cie/23/"),
    ],

    "Chemistry (0620)": [
        (f"{BASE_URL}/igcse/chemistry/cie/25/revision-notes/",                  f"{BASE_URL}/igcse/chemistry/cie/25/"),
        (f"{BASE_URL}/igcse/chemistry/cie/23/revision-notes/",                  f"{BASE_URL}/igcse/chemistry/cie/23/"),
    ],

    "Business Studies (0450)": [
        (f"{BASE_URL}/igcse/business/cie/25/revision-notes/",                   f"{BASE_URL}/igcse/business/cie/25/"),
        (f"{BASE_URL}/igcse/business/cie/23/revision-notes/",                   f"{BASE_URL}/igcse/business/cie/23/"),
    ],

    "Economics (0455)": [
        (f"{BASE_URL}/igcse/economics/cie/25/revision-notes/",                  f"{BASE_URL}/igcse/economics/cie/25/"),
        (f"{BASE_URL}/igcse/economics/cie/23/revision-notes/",                  f"{BASE_URL}/igcse/economics/cie/23/"),
        (f"{BASE_URL}/igcse/economics/cie/20/revision-notes/",                  f"{BASE_URL}/igcse/economics/cie/20/"),
    ],

    "Accounting (0452)": [
        (f"{BASE_URL}/igcse/accounting/cie/25/revision-notes/",                 f"{BASE_URL}/igcse/accounting/cie/25/"),
        (f"{BASE_URL}/igcse/accounting/cie/23/revision-notes/",                 f"{BASE_URL}/igcse/accounting/cie/23/"),
        (f"{BASE_URL}/igcse/accounting/cie/21/revision-notes/",                 f"{BASE_URL}/igcse/accounting/cie/21/"),
    ],

    "Environmental Management (0680)": [
        (f"{BASE_URL}/igcse/environmental-management/cie/25/revision-notes/",   f"{BASE_URL}/igcse/environmental-management/cie/25/"),
        (f"{BASE_URL}/igcse/environmental-management/cie/23/revision-notes/",   f"{BASE_URL}/igcse/environmental-management/cie/23/"),
        (f"{BASE_URL}/as/environmental-management/cie/20/revision-notes/",      f"{BASE_URL}/as/environmental-management/cie/20/"),
    ],

    "Islamiyat (0493)": [
        (f"{BASE_URL}/igcse/islamiyat/cie/25/revision-notes/",                  f"{BASE_URL}/igcse/islamiyat/cie/25/"),
        (f"{BASE_URL}/igcse/islamiyat/cie/23/revision-notes/",                  f"{BASE_URL}/igcse/islamiyat/cie/23/"),
    ],

    "Global Perspectives (0457)": [
        (f"{BASE_URL}/igcse/global-perspectives/cie/25/revision-notes/",        f"{BASE_URL}/igcse/global-perspectives/cie/25/"),
        (f"{BASE_URL}/igcse/global-perspectives/cie/23/revision-notes/",        f"{BASE_URL}/igcse/global-perspectives/cie/23/"),
    ],

    "Pakistan Studies (0448)": [
        (f"{BASE_URL}/igcse/pakistan-studies/cie/25/revision-notes/",           f"{BASE_URL}/igcse/pakistan-studies/cie/25/"),
        (f"{BASE_URL}/igcse/pakistan-studies/cie/23/revision-notes/",           f"{BASE_URL}/igcse/pakistan-studies/cie/23/"),
    ],

    "Urdu First Language (3247)": [
        (f"{BASE_URL}/igcse/urdu/cie/25/first-language/revision-notes/",        f"{BASE_URL}/igcse/urdu/cie/25/first-language/"),
        (f"{BASE_URL}/igcse/urdu/cie/23/first-language/revision-notes/",        f"{BASE_URL}/igcse/urdu/cie/23/first-language/"),
        (f"{BASE_URL}/igcse/urdu-first-language/cie/23/revision-notes/",        f"{BASE_URL}/igcse/urdu-first-language/cie/23/"),
    ],

    "Urdu as a Second Language (0539)": [
        (f"{BASE_URL}/igcse/urdu/cie/25/second-language/revision-notes/",       f"{BASE_URL}/igcse/urdu/cie/25/second-language/"),
        (f"{BASE_URL}/igcse/urdu/cie/23/second-language/revision-notes/",       f"{BASE_URL}/igcse/urdu/cie/23/second-language/"),
        (f"{BASE_URL}/igcse/urdu-second-language/cie/23/revision-notes/",       f"{BASE_URL}/igcse/urdu-second-language/cie/23/"),
    ],
}

# ─── Browser helpers ──────────────────────────────────────────────────────────

def _random_ua() -> str:
    """Rotate between a handful of realistic user-agent strings."""
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]
    return random.choice(agents)


async def make_context(browser: Browser) -> tuple[BrowserContext, Page]:
    ctx = await browser.new_context(
        user_agent=_random_ua(),
        viewport={"width": random.randint(1200, 1920), "height": random.randint(800, 1080)},
        locale="en-GB",
        timezone_id=TIMEZONE,
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )
    # Mask webdriver flag
    await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    page = await ctx.new_page()
    return ctx, page


# ─── URL helpers ──────────────────────────────────────────────────────────────

def _clean_url(href: str) -> str:
    return href.split("?")[0].split("#")[0].rstrip("/") + "/"


def _is_revision_notes_url(url: str, prefix: str) -> bool:
    """Only keep URLs that are within the prefix AND contain revision-notes."""
    return url.startswith(prefix) and "revision-notes" in url


async def get_links_on_page(page: Page, prefix: str) -> list[str]:
    hrefs: list[str] = await page.evaluate("""
        () => Array.from(document.querySelectorAll('a[href]'))
                   .map(a => a.getAttribute('href') || '')
    """)
    results = set()
    for href in hrefs:
        if href.startswith("/"):
            href = BASE_URL + href
        href = _clean_url(href)
        if _is_revision_notes_url(href, prefix):
            results.add(href)
    return list(results)


# ─── Resilient page loader ────────────────────────────────────────────────────

async def load_page_with_retry(
    page: Page,
    url: str,
    retries: int = MAX_RETRIES,
) -> bool:
    """
    Load a URL, retrying with exponential back-off on failure.
    Returns True if the page loaded successfully, False if all retries exhausted.
    """
    delay = RETRY_BASE_DELAY
    for attempt in range(1, retries + 2):
        try:
            resp = await page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            if resp and resp.status == 200:
                title = (await page.title()).lower()
                if "not found" not in title and "404" not in title and "error" not in title:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(300)
                    return True
                else:
                    return False  # 404 page — no point retrying
            elif resp and resp.status in (403, 429):
                # Rate limited — back off hard
                wait = delay * 3
                print(f"      ⚠ HTTP {resp.status} on attempt {attempt} — waiting {wait:.1f}s")
                await asyncio.sleep(wait)
                delay *= 2
            else:
                status = resp.status if resp else "no response"
                print(f"      ⚠ HTTP {status} on attempt {attempt}/{retries + 1}")
                if attempt <= retries:
                    await asyncio.sleep(delay)
                    delay *= 2
        except Exception as e:
            print(f"      ⚠ Error on attempt {attempt}/{retries + 1}: {type(e).__name__}: {e}")
            if attempt <= retries:
                await asyncio.sleep(delay)
                delay *= 2
    return False


# ─── Candidate resolution ─────────────────────────────────────────────────────

async def resolve_start_url(page: Page, candidates: list[tuple[str, str]]) -> tuple[str, str] | None:
    """Try each (start_url, prefix) candidate in order. Return first that resolves."""
    for start_url, prefix in candidates:
        print(f"    trying {start_url}")
        ok = await load_page_with_retry(page, start_url, retries=2)
        if ok:
            print(f"    ✓ resolved")
            return start_url, prefix
        await asyncio.sleep(1)
    return None


# ─── BFS Crawl ────────────────────────────────────────────────────────────────

async def crawl_subject(
    browser: Browser,
    start_url: str,
    prefix: str,
    already_visited: set[str],
) -> list[str]:
    """
    BFS crawl within `prefix`, starting from `start_url`.
    Recreates the browser context if too many consecutive errors occur.
    `already_visited` allows resuming a partial crawl.
    """
    visited:    set[str]   = set(already_visited)
    queue:      deque[str] = deque([start_url])
    errors:     int        = 0
    found:      list[str]  = list(already_visited)

    ctx, page = await make_context(browser)

    try:
        while queue:
            url = queue.popleft()
            if url in visited:
                continue

            # Random human-like delay
            await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))

            ok = await load_page_with_retry(page, url)
            visited.add(url)

            if not ok:
                errors += 1
                print(f"      ✗ failed (error #{errors}): {url}")
                if errors >= MAX_ERRORS_BEFORE_RESTART:
                    print(f"      ↻ Too many errors — recreating browser context...")
                    await ctx.close()
                    ctx, page = await make_context(browser)
                    errors = 0
                continue

            errors = 0  # reset on success
            found.append(url)

            new_links = await get_links_on_page(page, prefix)
            added = 0
            for link in new_links:
                if link not in visited:
                    queue.append(link)
                    added += 1

            print(f"    [{len(found):>4} found | {len(queue):>4} queued] {url}")

    finally:
        await ctx.close()

    return sorted(set(found))


# ─── Checkpointing ────────────────────────────────────────────────────────────

def load_checkpoint() -> dict[str, list[str]]:
    if CHECKPOINT.exists():
        try:
            with open(CHECKPOINT, encoding="utf-8") as f:
                data = json.load(f)
            print(f"  ↻ Resuming from checkpoint ({len(data)} subjects already done)")
            return data
        except Exception:
            pass
    return {}


def save_checkpoint(results: dict[str, list[str]]) -> None:
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


# ─── Main orchestrator ────────────────────────────────────────────────────────

async def scrape_all() -> dict[str, list[str]]:
    results = load_checkpoint()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # Use one page just for resolving start URLs
        probe_ctx, probe_page = await make_context(browser)

        for subject, candidates in SUBJECTS.items():

            if subject in results:
                print(f"\n  [SKIP — already in checkpoint] {subject}")
                continue

            print(f"\n{'─'*60}")
            print(f"  {subject}")

            resolved = await resolve_start_url(probe_page, candidates)
            if resolved is None:
                print(f"  ✗ No working URL found — skipping")
                results[subject] = []
                save_checkpoint(results)
                continue

            start_url, prefix = resolved
            urls = await crawl_subject(browser, start_url, prefix, set())

            results[subject] = urls
            print(f"  ✓ {len(urls)} URLs collected for {subject}")

            save_checkpoint(results)  # persist after each subject

        await probe_ctx.close()
        await browser.close()

    return results


# ─── Output ───────────────────────────────────────────────────────────────────

def save_results(results: dict[str, list[str]]) -> None:
    json_path = DOWNLOADS / "revision_notes.json"
    txt_path  = DOWNLOADS / "revision_notes_urls.txt"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  JSON saved to : {json_path}")

    with open(txt_path, "w", encoding="utf-8") as f:
        for subject, urls in results.items():
            f.write(f"\n# {subject}\n")
            if urls:
                for url in urls:
                    f.write(f"{url}\n")
            else:
                f.write("# (not available on SaveMyExams)\n")
    print(f"  TXT saved to  : {txt_path}")

    # Clean up checkpoint once we have final output
    if CHECKPOINT.exists():
        CHECKPOINT.unlink()
        print(f"  Checkpoint deleted.")


def print_summary(results: dict[str, list[str]]) -> None:
    print(f"\n{'='*60}")
    print("  FINAL SUMMARY")
    print(f"{'='*60}")
    total = 0
    for subject, urls in results.items():
        tag = f"{len(urls)} URLs" if urls else "NOT AVAILABLE"
        print(f"  {subject:<45}  {tag}")
        total += len(urls)
    print(f"{'='*60}")
    print(f"  Total: {total} URLs across {len(results)} subjects")


async def main():
    print("SaveMyExams — IGCSE CIE Revision Notes Scraper (Robust Edition)")
    print(f"  {len(SUBJECTS)} subjects | retries={MAX_RETRIES} | delay={BETWEEN_PAGES_MIN}–{BETWEEN_PAGES_MAX}s")
    print(f"  Checkpoint file: {CHECKPOINT}\n")

    results = await scrape_all()
    print_summary(results)
    save_results(results)


if __name__ == "__main__":
    asyncio.run(main())