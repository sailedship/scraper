"""
SaveMyExams PDF Downloader — Playwright Edition (Cookie Auth via JSON)
Extracts the PDF CDN URL from each page, then downloads it directly with requests.
Much faster than browser-based download interception.

Install:
    pip install playwright termcolor requests beautifulsoup4
    playwright install chromium

HOW TO GET YOUR COOKIES:
    1. Log into savemyexams.com in Chrome
    2. Install the "Cookie-Editor" extension (cookie-editor.com)
    3. Click the extension → Export → "Export as JSON"
    4. Save the file to: ~/Downloads/cookies.json  (or update COOKIES_FILE below)
"""

import asyncio
import json
import random
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from termcolor import colored
from playwright.async_api import async_playwright, Page, BrowserContext

# ─── Paths ────────────────────────────────────────────────────────────────────
_DOWNLOADS    = Path.home() / "Downloads"
URLS_FILE     = _DOWNLOADS / "urls.txt"
OUTPUT_DIR    = _DOWNLOADS / "SaveMyExams"
COOKIES_FILE  = _DOWNLOADS / "cookies.json"
SKIPPED_FILE  = _DOWNLOADS / "skipped_urls.txt"
PROGRESS_FILE = _DOWNLOADS / "sme_progress.json"

# ─── Settings ─────────────────────────────────────────────────────────────────
MAX_RETRIES            = 4
RETRY_BASE_DELAY       = 2.0    # seconds (doubles each attempt)
PAGE_LOAD_TIMEOUT      = 45_000 # ms
BETWEEN_PAGES_MIN      = 0.4    # reduced — we're not waiting for browser downloads anymore
BETWEEN_PAGES_MAX      = 1.0
MAX_CONSECUTIVE_ERRORS = 6
MIN_PDF_BYTES          = 5_000  # sanity check — real PDFs are always larger than this
DOWNLOAD_CHUNK_SIZE    = 1024 * 256  # 256KB chunks for streaming download
TIMEZONE               = "UTC"  # set to your local timezone e.g. "America/New_York", "Europe/London"


# ─── Banner ───────────────────────────────────────────────────────────────────

def print_banner():
    print(colored(r"""  __                     _
 (_   _.     _  |/|    |_     _. ._ _   _
 __) (_| / (/_ |  | / |_ >< (_| | | | _>
                     /
          """, 'cyan'))


# ─── Cookie loading ───────────────────────────────────────────────────────────

def load_cookies() -> list[dict]:
    if not COOKIES_FILE.is_file():
        print(colored(f"  [!] cookies.json not found at {COOKIES_FILE}", "yellow"))
        print(colored("  [!] Pages may be paywalled. See script header for setup.\n", "yellow"))
        return []
    try:
        raw = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = []
        for c in raw:
            cookie = {
                "name":   c.get("name", ""),
                "value":  c.get("value", ""),
                "domain": c.get("domain", ".savemyexams.com"),
                "path":   c.get("path", "/"),
            }
            if "httpOnly" in c: cookie["httpOnly"] = c["httpOnly"]
            if "secure"   in c: cookie["secure"]   = c["secure"]
            if "sameSite" in c and c["sameSite"] in ("Strict", "Lax", "None"):
                cookie["sameSite"] = c["sameSite"]
            cookies.append(cookie)
        print(colored(f"  [+] Loaded {len(cookies)} cookie(s) from {COOKIES_FILE}", "green"))
        return cookies
    except Exception as e:
        print(colored(f"  [-] Failed to load cookies.json: {e}", "red"))
        return []


def cookies_as_requests_jar(cookies: list[dict]) -> requests.cookies.RequestsCookieJar:
    """Convert Playwright cookie dicts to a requests CookieJar for direct downloads."""
    jar = requests.cookies.RequestsCookieJar()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain", ".savemyexams.com"), path=c.get("path", "/"))
    return jar


async def snapshot_cookies(context: BrowserContext, fallback: list[dict]) -> list[dict]:
    try:
        fresh = await context.cookies(["https://www.savemyexams.com"])
        return fresh if fresh else fallback
    except Exception:
        return fallback


# ─── Checkpointing ────────────────────────────────────────────────────────────

def load_progress() -> set[str]:
    if PROGRESS_FILE.is_file():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            done = set(data.get("done", []))
            print(colored(f"  [↻] Resuming — {len(done)} URLs already done", "yellow"))
            return done
        except Exception:
            pass
    return set()


def save_progress(done: set[str]):
    try:
        PROGRESS_FILE.write_text(json.dumps({"done": list(done)}, indent=2), encoding="utf-8")
    except Exception as e:
        print(colored(f"  [!] Could not save progress: {e}", "yellow"))


# ─── URL loading ──────────────────────────────────────────────────────────────

def load_urls() -> list[str]:
    filepath = URLS_FILE
    if not filepath.is_file():
        print(colored(f"[-] File not found: {filepath}", 'red'))
        filepath = Path(input(colored("[?] Enter full path to urls.txt: ", 'yellow')).strip())
    try:
        lines = filepath.read_text(encoding="utf-8").splitlines()
        urls = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
        print(colored(f"[+] Loaded {len(urls)} URLs from {filepath}", "green"))
        return urls
    except Exception as e:
        print(colored(f"[-] Could not read file: {e}", 'red'))
        input(colored("\nPress [ENTER] to close window", "magenta"))
        raise SystemExit


# ─── Sub-URL loading ──────────────────────────────────────────────────────────

def get_sub_urls(url: str) -> list[str]:
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "html.parser")
            nav = soup.find("nav", class_="resources-nav")
            if nav:
                links = set()
                for a in nav.find_all("a", href=True):
                    href = a["href"]
                    if not href.startswith("http"):
                        href = "https://www.savemyexams.com" + href
                    links.add(href)
                print(colored(f"[+] Found {len(links)} sub-URLs from {url}", "green"))
                return list(links)
            return [url]
        except Exception as e:
            print(colored(f"[-] Attempt {attempt}/3 failed for sub-URLs of {url}: {e}", "red"))
            if attempt < 3:
                time.sleep(2)
    return [url]


# ─── Filename helper ──────────────────────────────────────────────────────────

def sanitize_filename(title: str, url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r'[\\/*?:"<>|]', "", slug)[:40]
    name = re.sub(r'[\\/*?:"<>|]', "", title).strip().strip(".") if title else ""
    result = f"{name} ({slug})" if name else slug or "untitled"
    return result[:180]

# ─── Subject folder mapping ───────────────────────────────────────────────────

SUBJECT_FOLDER_MAP = {
    "english-language":         "English First Language (0500)",
    "maths":                    "Mathematics (0580)",
    "computer-science":         "Computer Science (0478)",
    "physics":                  "Physics (0625)",
    "chemistry":                "Chemistry (0620)",
    "pakistan-studies":         "Pakistan Studies (0448)",
    "urdu":                     "Urdu",
    "islamiyat":                "Islamiyat (0493)",
    "global-perspectives":      "Global Perspectives (0457)",
    "business":                 "Business Studies (0450)",
    "economics":                "Economics (0455)",
    "accounting":               "Accounting (0452)",
    "environmental-management": "Environmental Management (0680)",
}

def subject_folder_for(url: str) -> str:
    """Extract human-readable subject folder name from a SaveMyExams URL."""
    try:
        parts = url.replace("https://www.savemyexams.com/", "").split("/")
        # URL: igcse / {subject} / cie / {year} / revision-notes / ...
        slug = parts[1] if len(parts) > 1 else "unknown"
        name = SUBJECT_FOLDER_MAP.get(slug, slug.replace("-", " ").title())
        if slug == "urdu":
            if "first-language" in url.lower():
                name = "Urdu First Language (3247)"
            elif "second-language" in url.lower():
                name = "Urdu as a Second Language (0539)"
        return name
    except Exception:
        return "Other"

# ─── Resilient page loader ────────────────────────────────────────────────────

async def load_with_retry(page: Page, url: str) -> bool:
    delay = RETRY_BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            resp = await page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            if resp and resp.status == 200:
                await page.wait_for_timeout(800)
                title = (await page.title()).lower()
                if "not found" not in title and "404" not in title:
                    return True
                print(colored(f"  [!] 404 page — skipping", "yellow"))
                return False
            elif resp and resp.status in (429, 403):
                wait = delay * 3
                print(colored(f"  [!] HTTP {resp.status} — waiting {wait:.0f}s", "yellow"))
                await asyncio.sleep(wait)
                delay *= 2
            else:
                print(colored(f"  [!] HTTP {resp.status if resp else 'no response'} attempt {attempt} — retrying", "yellow"))
                await asyncio.sleep(delay)
                delay *= 2
        except Exception as e:
            print(colored(f"  [!] Attempt {attempt}/{MAX_RETRIES + 1}: {type(e).__name__}: {e}", "yellow"))
            if attempt <= MAX_RETRIES:
                await asyncio.sleep(delay)
                delay *= 2
    return False


# ─── Browser context factory ──────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

async def make_context(browser, headless: bool, cookies: list[dict]) -> BrowserContext:
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="en-GB",
        timezone_id=TIMEZONE,
        viewport={"width": 1600, "height": 900},
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    if cookies:
        await ctx.add_cookies(cookies)
    return ctx


# ─── Direct PDF download via requests ────────────────────────────────────────

def download_pdf_direct(pdf_url: str, dest: Path, cookie_jar: requests.cookies.RequestsCookieJar) -> bool:
    """
    Stream-download the PDF directly using requests.
    Much faster than browser download interception — no Chrome overhead.
    Saves to a .part file first to avoid corrupt output on failure.
    """
    tmp = dest.with_suffix(".pdf.part")
    try:
        resp = requests.get(
            pdf_url,
            cookies=cookie_jar,
            stream=True,
            timeout=60,
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Referer":    "https://www.savemyexams.com/",
            },
            allow_redirects=True,
        )
        if resp.status_code != 200:
            print(colored(f"  [-] PDF download returned HTTP {resp.status_code}", "red"))
            return False

        content_type = resp.headers.get("content-type", "")
        if "pdf" not in content_type and "octet-stream" not in content_type:
            print(colored(f"  [!] Unexpected content-type: {content_type} — may not be a PDF", "yellow"))

        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

        size = tmp.stat().st_size
        if size < MIN_PDF_BYTES:
            print(colored(f"  [-] File too small ({size}B) — discarding", "red"))
            tmp.unlink(missing_ok=True)
            return False

        tmp.rename(dest)
        print(colored(f"  [+] Saved: {dest.name} ({size // 1024}KB)", "green"))
        return True

    except Exception as e:
        print(colored(f"  [-] Download failed: {e}", "red"))
        tmp.unlink(missing_ok=True)
        return False


# ─── Main per-page logic ──────────────────────────────────────────────────────

async def process_url(
    url: str,
    context: BrowserContext,
    output_dir: Path,
    cookie_jar: requests.cookies.RequestsCookieJar,
) -> bool:
    page = await context.new_page()
    try:
        # ── 1. Load page ───────────────────────────────────────────────────────
        if not await load_with_retry(page, url):
            return False

        # ── 2. Resolve destination path ────────────────────────────────────────
        title = await page.evaluate(
            "() => { const el = document.querySelector('.resource-title'); "
            "return el ? el.innerText.trim() : document.title.split('|')[0].trim(); }"
        )
        subject_dir = output_dir / subject_folder_for(url)
        subject_dir.mkdir(parents=True, exist_ok=True)
        dest = subject_dir / f"{sanitize_filename(title, url)}.pdf"

        if dest.exists() and dest.stat().st_size >= MIN_PDF_BYTES:
            print(colored(f"  [~] Already exists ({dest.stat().st_size // 1024}KB) — skipping", "yellow"))
            return True

        # ── 3. Find the Download button ────────────────────────────────────────
        btn = await page.query_selector("button[aria-label='Download notes']")
        if not btn:
            # Fallback: any visible button containing "Download PDF"
            btn = await page.query_selector("button:has-text('Download PDF')")
        if not btn:
            print(colored(f"  [-] No download button found — skipping", "red"))
            return False

        # ── 4. Click and catch the new tab that opens ──────────────────────────
        # expect_popup() is the correct Playwright API for this pattern
        async with page.expect_popup(timeout=10_000) as popup_info:
            await btn.click()
        popup = await popup_info.value

        # The popup navigates to the PDF (possibly via redirect)
        # wait_for_url with ** catches any final URL
        try:
            await popup.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass  # If it times out the URL is probably already set (CDN redirect)

        pdf_url = popup.url
        await popup.close()

        if not pdf_url or pdf_url == "about:blank":
            print(colored(f"  [-] Popup opened but URL was blank — skipping", "red"))
            return False

        print(colored(f"  [+] {dest.name}", "green"))
        print(colored(f"  [+] PDF: {pdf_url[:80]}...", "cyan"))

        # ── 5. Download ────────────────────────────────────────────────────────
        return download_pdf_direct(pdf_url, dest, cookie_jar)

    except Exception as e:
        print(colored(f"  [-] Error on {url}: {type(e).__name__}: {e}", "red"))
        return False
    finally:
        await page.close()


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    print_banner()

    cookies    = load_cookies()
    done_urls  = load_progress()

    load_sub = input(colored("[?] Load sub-URLs from nav? [Yes/no]: ", "yellow")).strip().lower()
    raw_urls = load_urls()

    all_urls: list[str] = []
    if not load_sub or load_sub.startswith("y"):
        for url in raw_urls:
            all_urls.extend(get_sub_urls(url))
        seen: set[str] = set()
        all_urls = [u for u in all_urls if not (u in seen or seen.add(u))]
    else:
        all_urls = raw_urls

    remaining = [u for u in all_urls if u not in done_urls]
    print(colored(f"[+] Total: {len(all_urls)} | Done: {len(done_urls)} | Remaining: {len(remaining)}", "green"))

    headless_inp = input(colored("[?] Enable Headless Mode? (recommended) [Yes/no]: ", "yellow")).strip().lower()
    headless = not headless_inp.startswith("n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(colored(f"[+] Output: {OUTPUT_DIR}\n", "green"))

    skipped:            list[str] = []
    consecutive_errors: int       = 0
    start_time = time.time()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--disable-gpu", "--no-sandbox", "--disable-setuid-sandbox", "--ignore-certificate-errors"]
        )
        context    = await make_context(browser, headless, cookies)
        cookie_jar = cookies_as_requests_jar(cookies)

        for i, url in enumerate(remaining, 1):
            elapsed = time.time() - start_time
            if i > 1:
                avg     = elapsed / (i - 1)
                eta     = int(avg * (len(remaining) - i + 1))
                eta_str = f"{eta // 60}m {eta % 60}s"
            else:
                eta_str = "..."

            print(colored(f"\n{'='*55}", "cyan"))
            print(colored(f"[{i}/{len(remaining)}]  ETA: {eta_str}", "cyan"))
            print(colored(url, "cyan"))

            await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))

            success = await process_url(url, context, OUTPUT_DIR, cookie_jar)

            # Refresh cookies after every page
            cookies    = await snapshot_cookies(context, cookies)
            cookie_jar = cookies_as_requests_jar(cookies)

            if success:
                done_urls.add(url)
                save_progress(done_urls)
                consecutive_errors = 0
            else:
                skipped.append(url)
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    print(colored("  [↻] Recreating context...", "yellow"))
                    await context.close()
                    context = await make_context(browser, headless, cookies)
                    consecutive_errors = 0

        await context.close()
        await browser.close()

    if skipped:
        SKIPPED_FILE.write_text("\n".join(skipped), encoding="utf-8")
        print(colored(f"\n  [!] Skipped URLs saved to: {SKIPPED_FILE}", "yellow"))

    if not skipped and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    total = int(time.time() - start_time)
    print(colored(f"\n{'='*55}", "cyan"))
    print(colored(f"[+] Done! PDFs in: {OUTPUT_DIR}", "cyan"))
    print(colored(f"[+] Processed: {len(remaining) - len(skipped)}/{len(remaining)}", "green"))
    print(colored(f"[+] Time: {total // 60}m {total % 60}s", "green"))
    if skipped:
        print(colored(f"[-] Skipped {len(skipped)} — see {SKIPPED_FILE}", "red"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(colored(f"Error: {e}", "red"))
    finally:
        input(colored("\nPress [ENTER] to close window", "magenta"))