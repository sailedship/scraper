"""
SaveMyExams — IGCSE CIE Topic Questions  ·  Combined Scraper + Downloader
=========================================================================
FIXED: Replaced brittle hashed CSS-Module class selectors with stable
       attribute/structural selectors that survive site redeployments.

Phase 1  BFS-crawl every topic-questions page for each subject
Phase 2  For each page:
           • Click "Download PDFs" toggle → dropdown opens
           • Click each option button → intercept /api/proxy/usage/v1/pdf-downloads/
             response → extract CDN URL → download via requests
           • pdf.savemyexams.com/?get=... URLs are decoded to extract the real URL

Install:
    pip install playwright termcolor requests
    playwright install chromium

Cookie setup:
    1. Log into savemyexams.com in Chrome
    2. Cookie-Editor extension → Export as JSON
    3. Save to  ~/Downloads/cookies.json  (or update COOKIES_FILE below)
"""

import asyncio
import json
import random
import re
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from termcolor import colored
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# ══════════════════════════════════════════════════════════════════════════════
#  PATHS
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL          = "https://www.savemyexams.com"
DOWNLOADS         = Path.home() / "Downloads"
OUTPUT_DIR        = DOWNLOADS / "SaveMyExams" / "Topic Questions"
COOKIES_FILE      = DOWNLOADS / "cookies.json"
URLS_FILE         = DOWNLOADS / "topic_questions_urls.txt"
PROGRESS_FILE     = DOWNLOADS / "tq_progress.json"
SKIPPED_FILE      = DOWNLOADS / "tq_skipped.txt"
SCRAPE_CHECKPOINT = DOWNLOADS / "tq_scrape_checkpoint.json"

# ══════════════════════════════════════════════════════════════════════════════
#  TUNING
# ══════════════════════════════════════════════════════════════════════════════
MAX_RETRIES            = 4
RETRY_BASE_DELAY       = 2.0
PAGE_LOAD_TIMEOUT      = 45_000   # ms
PDF_REQUEST_TIMEOUT    = 20_000   # ms
POPUP_WAIT_TIMEOUT     = 15.0     # seconds
DROPDOWN_VISIBLE_WAIT  = 6_000    # ms — increased to give React more time
BETWEEN_PAGES_MIN      = 0.5
BETWEEN_PAGES_MAX      = 1.2
MIN_PDF_BYTES          = 5_000
DOWNLOAD_CHUNK_SIZE    = 256 * 1024
DOWNLOAD_RETRIES       = 3
TIMEZONE               = "UTC"    # set to your local timezone e.g. "America/New_York", "Europe/London"

# ══════════════════════════════════════════════════════════════════════════════
#  DOM SELECTORS  ← THE MAIN FIX
#
#  OLD (brittle — hashed CSS Module names break on every redeploy):
#    SEL_Q_MENU_OPEN = "div.Downloads_dropdownMenu__fXGGg.show"
#    SEL_Q_OPTIONS   = "div.Downloads_dropdownContent__Rtgph button"
#
#  NEW (stable — uses IDs, aria attributes, and structural relationships):
#    • Toggle:    button#desktop-download-dropdown          (ID — very stable)
#    • Open check: aria-expanded="true" on the toggle btn   (standard ARIA)
#    • Options:   any <button> inside the sibling div that
#                 appears after the toggle is clicked        (structural)
# ══════════════════════════════════════════════════════════════════════════════

# Toggle button — ID is stable, falls back to aria-label text
SEL_Q_TOGGLE = (
    "button#desktop-download-dropdown, "
    "button[aria-label*='Download PDF' i], "
    "button[aria-label*='download' i]:not([id*='answer'])"
)

# "Is the dropdown open?" — check aria-expanded on the toggle rather than a
# hashed class on the menu div.  Works regardless of CSS Module hash.
# We also look for the dropdown panel itself via a data-testid fallback.
SEL_Q_MENU_OPEN_ARIA = (
    "button#desktop-download-dropdown[aria-expanded='true'], "
    "button[aria-label*='Download PDF' i][aria-expanded='true']"
)

# Options inside the open dropdown.
# Strategy: find any <button> that is a descendant of the *next sibling* div
# after the toggle, OR inside an element with [role='menu'] / [role='listbox'],
# OR inside a div whose id/class loosely contains 'download' and 'menu'/'list'.
# This deliberately avoids any hashed class name.
SEL_Q_OPTIONS = (
    "button#desktop-download-dropdown + div button, "
    "button#desktop-download-dropdown ~ div button, "
    "[role='menu'] button, "
    "[role='listbox'] button, "
    "[data-testid*='download'] button"
)

# ── PDF URL constants ─────────────────────────────────────────────────────────
PDF_API_PATH      = "/api/proxy/usage/v1/pdf-downloads"
PDF_RENDERER_HOST = "pdf.savemyexams.com"
CDN_UPLOAD_RE     = re.compile(
    r'https://cdn\.savemyexams\.com/(?:uploads|pdf)/[^\s"\'<>]+',
    re.IGNORECASE,
)
BLANKS = {"about:blank", "chrome://newtab/", ""}

# ══════════════════════════════════════════════════════════════════════════════
#  SUBJECT / URL CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════
SUBJECTS: dict[str, list[tuple[str, str]]] = {
    "Computer Science (0478)": [
        (f"{BASE_URL}/igcse/computer-science/cie/25/topic-questions/",      f"{BASE_URL}/igcse/computer-science/cie/25/"),
        (f"{BASE_URL}/igcse/computer-science/cie/23/topic-questions/",      f"{BASE_URL}/igcse/computer-science/cie/23/"),
    ],
    "Business Studies (0450)": [
        (f"{BASE_URL}/igcse/business/cie/25/topic-questions/",              f"{BASE_URL}/igcse/business/cie/25/"),
        (f"{BASE_URL}/igcse/business/cie/23/topic-questions/",              f"{BASE_URL}/igcse/business/cie/23/"),
    ],
    "Economics (0455)": [
        (f"{BASE_URL}/igcse/economics/cie/25/topic-questions/",             f"{BASE_URL}/igcse/economics/cie/25/"),
        (f"{BASE_URL}/igcse/economics/cie/23/topic-questions/",             f"{BASE_URL}/igcse/economics/cie/23/"),
    ],
    "Accounting (0452)": [
        (f"{BASE_URL}/igcse/accounting/cie/25/topic-questions/",            f"{BASE_URL}/igcse/accounting/cie/25/"),
        (f"{BASE_URL}/igcse/accounting/cie/23/topic-questions/",            f"{BASE_URL}/igcse/accounting/cie/23/"),
    ],
    "Environmental Management (0680)": [
        (f"{BASE_URL}/igcse/environmental-management/cie/25/topic-questions/", f"{BASE_URL}/igcse/environmental-management/cie/25/"),
        (f"{BASE_URL}/igcse/environmental-management/cie/23/topic-questions/", f"{BASE_URL}/igcse/environmental-management/cie/23/"),
    ],
}

SUBJECT_FOLDER_MAP = {
    "english-language":         "English First Language (0500)",
    "maths":                    "Mathematics (0580)",
    "computer-science":         "Computer Science (0478)",
    "physics":                  "Physics (0625)",
    "chemistry":                "Chemistry (0620)",
    "biology":                  "Biology (0610)",
    "pakistan-studies":         "Pakistan Studies (0448)",
    "islamiyat":                "Islamiyat (0493)",
    "global-perspectives":      "Global Perspectives (0457)",
    "business":                 "Business Studies (0450)",
    "economics":                "Economics (0455)",
    "accounting":               "Accounting (0452)",
    "environmental-management": "Environmental Management (0680)",
}

PAPER_TYPE_SLUGS = {
    "multiple-choice-questions", "theory-questions",
    "alternative-to-practical-questions", "calculator-questions",
    "non-calculator-questions", "structured-questions",
    "data-response-questions", "extended-response-questions",
    "written-questions", "teacher-only-questions", "exam-questions",
    "paper-1", "paper-2", "paper-3", "paper-4",
}

def subject_folder_for(url: str) -> str:
    try:
        parts = url.replace(f"{BASE_URL}/", "").split("/")
        slug  = parts[1] if len(parts) > 1 else "unknown"
        name  = SUBJECT_FOLDER_MAP.get(slug, slug.replace("-", " ").title())
        if slug == "maths":
            if "extended" in url: name = "Mathematics (0580) — Extended"
            elif "core"   in url: name = "Mathematics (0580) — Core"
        return name
    except Exception:
        return "Other"

def is_downloadable_page(url: str) -> bool:
    return url.rstrip("/").split("/")[-1] in PAPER_TYPE_SLUGS and "topic-questions" in url


# ══════════════════════════════════════════════════════════════════════════════
#  BROWSER / COOKIE HELPERS
# ══════════════════════════════════════════════════════════════════════════════
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

def load_cookies() -> list[dict]:
    if not COOKIES_FILE.is_file():
        print(colored("  [!] cookies.json not found — pages may be paywalled", "yellow"))
        return []
    try:
        raw = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        out = []
        for c in raw:
            ck = {
                "name":   c["name"],
                "value":  c["value"],
                "domain": c.get("domain", ".savemyexams.com"),
                "path":   c.get("path", "/"),
            }
            for k in ("httpOnly", "secure"):
                if k in c:
                    ck[k] = c[k]
            if c.get("sameSite") in ("Strict", "Lax", "None"):
                ck["sameSite"] = c["sameSite"]
            out.append(ck)
        print(colored(f"  [+] Loaded {len(out)} cookies", "green"))
        return out
    except Exception as e:
        print(colored(f"  [-] Cookie load failed: {e}", "red"))
        return []

def cookies_to_jar(cookies: list[dict]) -> requests.cookies.RequestsCookieJar:
    jar = requests.cookies.RequestsCookieJar()
    for c in cookies:
        domain = c.get("domain", ".savemyexams.com")
        jar.set(c["name"], c["value"], domain=domain, path=c.get("path", "/"))
        for subdomain in (".savemyexams.com", "cdn.savemyexams.com",
                          "pdf.savemyexams.com", "www.savemyexams.com"):
            if domain in (".savemyexams.com", "savemyexams.com"):
                jar.set(c["name"], c["value"], domain=subdomain, path=c.get("path", "/"))
    return jar

async def make_context(browser: Browser, cookies: list[dict] | None = None) -> BrowserContext:
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": random.randint(1280, 1920), "height": random.randint(800, 1080)},
        locale="en-GB",
        timezone_id=TIMEZONE,
        extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    if cookies:
        await ctx.add_cookies(cookies)
    return ctx


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE LOADER
# ══════════════════════════════════════════════════════════════════════════════
async def load_page(page: Page, url: str, retries: int = MAX_RETRIES) -> bool:
    delay = RETRY_BASE_DELAY
    for attempt in range(1, retries + 2):
        try:
            resp = await page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            if resp and resp.status == 200:
                # Wait for React hydration — look for the toggle button, any heading, or nav.
                # Increased timeout to 10s to let client-side JS fully mount.
                try:
                    await page.wait_for_selector(
                        f"{SEL_Q_TOGGLE}, h1, nav",
                        timeout=10_000, state="attached",
                    )
                except Exception:
                    await page.wait_for_timeout(1_500)
                title = (await page.title()).lower()
                if "not found" not in title and "404" not in title:
                    return True
                return False
            elif resp and resp.status in (403, 429):
                w = delay * 3
                print(colored(f"    [!] HTTP {resp.status} — waiting {w:.0f}s", "yellow"))
                await asyncio.sleep(w)
                delay *= 2
            elif resp:
                if attempt <= retries:
                    await asyncio.sleep(delay)
                    delay *= 2
        except Exception as e:
            print(colored(f"    [!] {type(e).__name__} attempt {attempt}/{retries + 1}", "yellow"))
            if attempt <= retries:
                await asyncio.sleep(delay)
                delay *= 2
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  PDF URL UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def resolve_pdf_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url
    if PDF_RENDERER_HOST in raw_url:
        try:
            parsed    = urlparse(raw_url)
            get_param = parse_qs(parsed.query).get("get", [""])[0]
            if get_param:
                inner = json.loads(get_param)
                url   = inner.get("url", "")
                if url.startswith("http"):
                    return url
        except Exception:
            pass
        return raw_url
    if "cdn-cgi/image/" in raw_url:
        parts = re.split(r"(?<!\:)//", raw_url)
        for part in reversed(parts):
            candidate = "https://" + part if not part.startswith("http") else part
            if "cdn.savemyexams.com" in candidate and "cdn-cgi" not in candidate:
                return candidate
        m = re.search(r'/(https?://cdn\.savemyexams\.com/(?!cdn-cgi)[^\s"\'<>]+)', raw_url)
        if m:
            return m.group(1)
    return raw_url

def sanitize(text: str, maxlen: int = 160) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", text or "").strip().strip(".")[:maxlen]


# ══════════════════════════════════════════════════════════════════════════════
#  PDF DOWNLOAD (requests-based)
# ══════════════════════════════════════════════════════════════════════════════

def download_pdf(
    pdf_url: str,
    dest: Path,
    jar: requests.cookies.RequestsCookieJar,
    retries: int = DOWNLOAD_RETRIES,
) -> bool:
    if dest.exists() and dest.stat().st_size >= MIN_PDF_BYTES:
        print(colored(f"    [~] {dest.name} — already exists", "yellow"))
        return True
    tmp   = dest.with_suffix(f".{id(dest) & 0xFFFFFF:06x}.part")
    delay = 2.0
    for attempt in range(1, retries + 2):
        try:
            r = requests.get(
                pdf_url, cookies=jar, stream=True, timeout=60,
                headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Referer":    "https://www.savemyexams.com/",
                    "Accept":     "application/pdf,*/*",
                },
                allow_redirects=True,
            )
            if r.status_code in (403, 429):
                print(colored(f"    [!] HTTP {r.status_code} attempt {attempt} — waiting {delay:.0f}s", "yellow"))
                time.sleep(delay); delay *= 2
                continue
            if r.status_code != 200:
                print(colored(f"    [-] HTTP {r.status_code} for PDF", "red"))
                return False
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
            sz = tmp.stat().st_size
            if sz < MIN_PDF_BYTES:
                print(colored(f"    [-] Too small ({sz}B) — discarding", "red"))
                tmp.unlink(missing_ok=True)
                if attempt <= retries:
                    time.sleep(delay); delay *= 2
                    continue
                return False
            tmp.rename(dest)
            print(colored(f"    [✓] {dest.name}  ({sz // 1024} KB)", "green"))
            return True
        except Exception as e:
            print(colored(f"    [-] Download error attempt {attempt}: {e}", "red"))
            tmp.unlink(missing_ok=True)
            if attempt <= retries:
                time.sleep(delay); delay *= 2
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  DROPDOWN HELPERS  ← FIXED
# ══════════════════════════════════════════════════════════════════════════════

async def _find_toggle(page: Page) -> object | None:
    """Find the download toggle button using multiple fallback selectors."""
    # Primary: ID selector
    btn = await page.query_selector("button#desktop-download-dropdown")
    if btn:
        return btn
    # Fallback 1: aria-label containing "download"
    for sel in (
        "button[aria-label*='Download PDF' i]",
        "button[aria-label*='download' i]",
        # Fallback 2: button whose visible text contains "Download"
        # evaluated in JS because :has-text() is Playwright-only, safer via evaluate
    ):
        btn = await page.query_selector(sel)
        if btn:
            return btn
    # Fallback 3: any button whose innerText starts with "Download"
    btn = await page.evaluate_handle(
        "() => Array.from(document.querySelectorAll('button'))"
        ".find(b => /^download/i.test(b.innerText.trim()))"
    )
    try:
        # evaluate_handle returns JSHandle; None if JS returned undefined/null
        if btn and await btn.json_value() is not None:
            # Convert JSHandle → ElementHandle via query within document
            label = await page.evaluate("b => b.innerText.trim()", btn)
            if label:
                return await page.query_selector(
                    f"button:text-is('{label}')"  # Playwright locator syntax
                )
    except Exception:
        pass
    return None


async def _is_dropdown_open(page: Page) -> bool:
    """
    Check if the download dropdown is open.
    Uses aria-expanded on the toggle (stable) instead of a hashed CSS class.
    """
    # Primary: aria-expanded on the toggle
    result = await page.evaluate(
        """() => {
            const sels = [
                'button#desktop-download-dropdown',
                'button[aria-label*="Download PDF" i]',
                'button[aria-label*="download" i]',
            ];
            for (const sel of sels) {
                const el = document.querySelector(sel);
                if (el) return el.getAttribute('aria-expanded') === 'true';
            }
            return false;
        }"""
    )
    if result:
        return True
    # Fallback: look for a visible sibling/child div with buttons (the open panel)
    result2 = await page.evaluate(
        """() => {
            const btn = document.querySelector('button#desktop-download-dropdown');
            if (!btn) return false;
            // Walk next siblings looking for a div containing buttons
            let sib = btn.nextElementSibling;
            while (sib) {
                if (sib.tagName === 'DIV' && sib.querySelectorAll('button').length > 0) {
                    const style = window.getComputedStyle(sib);
                    return style.display !== 'none' && style.visibility !== 'hidden';
                }
                sib = sib.nextElementSibling;
            }
            return false;
        }"""
    )
    return bool(result2)


async def _get_option_buttons_selector(page: Page) -> str:
    """
    Dynamically find the correct CSS selector for the dropdown option buttons.
    Tries multiple strategies and returns the first one that yields buttons.
    This avoids hardcoding any hashed class names.
    """
    candidates = [
        # Structural: immediate/nearby sibling div of the toggle button
        "button#desktop-download-dropdown + div button",
        "button#desktop-download-dropdown ~ div button",
        # ARIA roles
        "[role='menu'] button",
        "[role='listbox'] button",
        # data-testid attributes
        "[data-testid*='download'] button",
        "[data-testid*='dropdown'] button",
        # Any div with 'download' in its class that contains buttons
        # (works even with hashed names since we just need *a* match)
        "div[class*='download' i] button",
        "div[class*='Download'] button",
        "div[class*='dropdown' i] button",
        "div[class*='Dropdown'] button",
    ]
    for sel in candidates:
        count = await page.evaluate(
            f"() => document.querySelectorAll({json.dumps(sel)}).length"
        )
        if count and count > 0:
            return sel
    # Last resort: all buttons visible near the top of the page that appeared
    # after clicking (can't distinguish easily, so return a broad selector)
    return "button#desktop-download-dropdown ~ div button"


async def open_dropdown(page: Page, *_args) -> bool:
    """
    Click the download toggle and wait for the dropdown to open.
    Parameters after `page` are accepted but ignored (backwards compat).
    """
    if await _is_dropdown_open(page):
        return True
    toggle = await _find_toggle(page)
    if not toggle:
        return False
    try:
        await toggle.scroll_into_view_if_needed()
        await toggle.click()
    except Exception as e:
        print(colored(f"    [!] Toggle click error: {e}", "yellow"))
        return False

    deadline = time.monotonic() + DROPDOWN_VISIBLE_WAIT / 1000
    while time.monotonic() < deadline:
        if await _is_dropdown_open(page):
            return True
        await page.wait_for_timeout(150)
    # Last-ditch: check if any buttons appeared near the toggle
    sel = await _get_option_buttons_selector(page)
    count = await page.evaluate(f"() => document.querySelectorAll({json.dumps(sel)}).length")
    return bool(count and count > 0)


async def close_dropdown(page: Page, *_args) -> None:
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(200)
    except Exception:
        pass


async def read_option_labels(page: Page, *_args) -> list[str]:
    """Read button text labels from the open dropdown."""
    sel = await _get_option_buttons_selector(page)
    labels: list[str] = await page.evaluate(
        """(sel) => Array.from(document.querySelectorAll(sel)).map(b => {
            const span = b.querySelector('span');
            return (span ? span.innerText : b.innerText).trim();
        })""",
        sel,
    )
    return [l for l in labels if l]


async def click_option_at_index(page: Page, *_args_and_index) -> None:
    """
    click_option_at_index(page, options_sel, index)  — old signature
    click_option_at_index(page, index)               — new signature
    Both are handled for backwards compatibility.
    """
    if len(_args_and_index) == 2:
        # old: (options_sel, index)
        _, index = _args_and_index
    else:
        # new: (index,)
        index = _args_and_index[0]

    sel  = await _get_option_buttons_selector(page)
    btns = await page.query_selector_all(sel)
    if index >= len(btns):
        raise IndexError(f"Option {index} out of range ({len(btns)} found)")
    await btns[index].scroll_into_view_if_needed()
    await btns[index].click()


# ══════════════════════════════════════════════════════════════════════════════
#  POPUP URL CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

async def _wait_for_popup_url(popup, timeout: float = POPUP_WAIT_TIMEOUT) -> str | None:
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            url = popup.url
            if url not in BLANKS and url.startswith("http"):
                break
            await asyncio.sleep(0.15)
        else:
            await popup.close()
            return None
        try:
            await popup.wait_for_load_state(
                "load", timeout=max(100, int((deadline - time.monotonic()) * 1000))
            )
        except Exception:
            pass
        url = popup.url
        await popup.close()
        return url if (url not in BLANKS and url.startswith("http")) else None
    except Exception:
        try:
            await popup.close()
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  PDF URL CAPTURE (route interception)
# ══════════════════════════════════════════════════════════════════════════════

async def capture_pdf_url_by_label(
    page: Page,
    toggle_sel: str,   # kept for backwards compat, now ignored
    menu_open_sel: str,
    options_sel: str,
    label: str,
) -> str | None:
    if not await open_dropdown(page):
        return None
    sel = await _get_option_buttons_selector(page)
    index: int | None = await page.evaluate(
        """(sel, lbl) => {
            const btns = Array.from(document.querySelectorAll(sel));
            const idx = btns.findIndex(b => {
                const span = b.querySelector('span');
                return (span ? span.innerText : b.innerText).trim() === lbl;
            });
            return idx >= 0 ? idx : null;
        }""",
        sel, label,
    )
    await close_dropdown(page)
    if index is None:
        print(colored(f"    [!] Button '{label}' not found in dropdown", "yellow"))
        return None
    return await capture_pdf_url(page, toggle_sel, menu_open_sel, options_sel, index)


async def capture_pdf_url(
    page: Page,
    toggle_sel: str,
    menu_open_sel: str,
    options_sel: str,
    option_index: int,
) -> str | None:
    if not await open_dropdown(page):
        print(colored("    [!] Dropdown failed to open", "yellow"))
        return None

    captured: list[str] = []
    popup_q: asyncio.Queue = asyncio.Queue()

    def on_popup(p):
        popup_q.put_nowait(p)

    async def handle_route(route, request):
        if PDF_API_PATH not in request.url:
            try:
                await route.continue_()
            except Exception:
                pass
            return
        try:
            resp = await route.fetch()
            loc = resp.headers.get("location", "")
            if loc.startswith("http") and not captured:
                captured.append(loc)
                await route.fulfill(response=resp)
                return
            try:
                body = await resp.json()
                for key in ("url", "downloadUrl", "pdfUrl", "pdf_url", "signedUrl", "link"):
                    val = body.get(key, "")
                    if isinstance(val, str) and val.startswith("http") and not captured:
                        captured.append(val)
                        break
            except Exception:
                pass
            if not captured:
                try:
                    text = await resp.text()
                    m = CDN_UPLOAD_RE.search(text)
                    if m:
                        captured.append(m.group(0).rstrip('",}]\\'))
                except Exception:
                    pass
            await route.fulfill(response=resp)
        except Exception:
            try:
                await route.continue_()
            except Exception:
                pass

    page.on("popup", on_popup)
    await page.route(f"**{PDF_API_PATH}*", handle_route)

    try:
        await click_option_at_index(page, options_sel, option_index)

        deadline = time.monotonic() + PDF_REQUEST_TIMEOUT / 1000
        while time.monotonic() < deadline:
            if captured:
                return resolve_pdf_url(captured[0])
            try:
                popup = popup_q.get_nowait()
                raw = await _wait_for_popup_url(popup)
                if raw:
                    return resolve_pdf_url(raw)
            except asyncio.QueueEmpty:
                pass
            await asyncio.sleep(0.1)

        print(colored(f"    [!] No PDF URL captured within {PDF_REQUEST_TIMEOUT}ms", "yellow"))
        return None
    except Exception as e:
        print(colored(f"    [!] Capture error: {type(e).__name__}: {e}", "yellow"))
        return None
    finally:
        try:
            await page.unroute(f"**{PDF_API_PATH}*", handle_route)
        except Exception:
            pass
        page.remove_listener("popup", on_popup)
        await close_dropdown(page)


# ══════════════════════════════════════════════════════════════════════════════
#  DEBUG HELPER  — dumps all button text + selectors on the page
# ══════════════════════════════════════════════════════════════════════════════

async def debug_dump_buttons(page: Page):
    """Print every button on the page and its attributes — useful for diagnosing selector issues."""
    buttons = await page.evaluate(
        """() => Array.from(document.querySelectorAll('button')).map(b => ({
            id:       b.id,
            text:     b.innerText.trim().slice(0, 60),
            ariaLabel: b.getAttribute('aria-label') || '',
            ariaExpanded: b.getAttribute('aria-expanded') || '',
            classes:  b.className.slice(0, 80),
        }))"""
    )
    print(colored("    [DEBUG] All buttons on page:", "yellow"))
    for b in buttons:
        print(colored(
            f"      id={b['id']!r:30s} aria-label={b['ariaLabel']!r:30s} "
            f"text={b['text']!r:40s} aria-expanded={b['ariaExpanded']!r}",
            "yellow"
        ))

async def debug_dump_requests(page: Page, *_args):
    if not await open_dropdown(page):
        return
    reqs: list[str] = []
    listener = lambda r: reqs.append(r.url)
    page.on("request", listener)
    try:
        await click_option_at_index(page, 0)
        await page.wait_for_timeout(6_000)
    finally:
        page.remove_listener("request", listener)
    print(colored("    [DEBUG] Requests after click:", "yellow"))
    for r in reqs:
        print(colored(f"      {r}", "yellow"))


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN PER-PAGE DOWNLOAD LOGIC
# ══════════════════════════════════════════════════════════════════════════════

async def download_page_variants(
    page: Page,
    dest_dir: Path,
    jar: requests.cookies.RequestsCookieJar,
    debug: bool = False,
) -> tuple[bool, bool]:
    title = await page.evaluate(
        "() => { const el = document.querySelector('.resource-title, h1'); "
        "return el ? el.innerText.trim() : document.title.split('|')[0].trim(); }"
    )
    base = sanitize(title or page.url.rstrip("/").split("/")[-1])

    q_ok = False
    q_toggle = await _find_toggle(page)

    if not q_toggle:
        # Extra wait: the button might still be rendering
        await page.wait_for_timeout(2_000)
        q_toggle = await _find_toggle(page)

    if not q_toggle:
        if debug:
            await debug_dump_buttons(page)
        print(colored("    [-] No 'Download PDFs' button found", "yellow"))
        q_ok = True  # no button = nothing to download
    else:
        if not await open_dropdown(page):
            print(colored("    [-] Questions dropdown failed to open", "red"))
        else:
            labels = await read_option_labels(page)
            await close_dropdown(page)
            print(colored(f"    [→] Questions: {labels}", "cyan"))

            if debug:
                await debug_dump_requests(page)
                return False, False

            if not labels:
                print(colored(
                    "    [!] Dropdown opened but no options found — "
                    "JS may not have rendered yet. Will retry next run.", "yellow"
                ))
            q_all_done = bool(labels)
            for label in labels:
                dest = dest_dir / f"{base} — {sanitize(label)}.pdf"
                if dest.exists() and dest.stat().st_size >= MIN_PDF_BYTES:
                    print(colored(f"    [~] {dest.name} — already exists", "yellow"))
                    continue
                print(colored(f"    [↓] {label}…", "cyan"))
                pdf_url = await capture_pdf_url_by_label(
                    page, SEL_Q_TOGGLE, SEL_Q_MENU_OPEN_ARIA, SEL_Q_OPTIONS, label
                )
                if pdf_url:
                    print(colored(f"       {pdf_url[:90]}", "cyan"))
                    if not download_pdf(pdf_url, dest, jar):
                        q_all_done = False
                else:
                    print(colored(f"    [-] No URL for '{label}'", "red"))
                    q_all_done = False
                await page.wait_for_timeout(400)
            q_ok = q_all_done

    return q_ok, True


# ══════════════════════════════════════════════════════════════════════════════
#  PER-URL ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════════

async def process_url(
    url: str,
    ctx: BrowserContext,
    jar: requests.cookies.RequestsCookieJar,
    debug: bool = False,
) -> bool:
    page = await ctx.new_page()
    try:
        if not await load_page(page, url):
            return False
        dest_dir = OUTPUT_DIR / subject_folder_for(url)
        dest_dir.mkdir(parents=True, exist_ok=True)
        q_ok, a_ok = await download_page_variants(page, dest_dir, jar, debug=debug)
        return q_ok and a_ok
    except Exception as e:
        print(colored(f"    [-] Unhandled error: {type(e).__name__}: {e}", "red"))
        return False
    finally:
        try:
            await page.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 1 — BFS SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def _clean(href: str) -> str:
    return href.split("?")[0].split("#")[0].rstrip("/") + "/"

async def links_on_page(page: Page, prefix: str) -> tuple[list[str], list[str]]:
    hrefs = await page.evaluate(
        "() => Array.from(document.querySelectorAll('a[href]'))"
        ".map(a => a.getAttribute('href') || '')"
    )
    to_crawl  = set()
    to_record = set()
    for h in hrefs:
        if h.startswith("/"): h = BASE_URL + h
        h = _clean(h)
        if not h.startswith(prefix):
            continue
        if "topic-questions" not in h:
            continue
        to_crawl.add(h)
        if is_downloadable_page(h):
            to_record.add(h)
    return list(to_crawl), list(to_record)

async def resolve_start(page: Page, candidates: list[tuple[str, str]]) -> tuple[str, str] | None:
    for url, prefix in candidates:
        print(f"    trying {url}")
        if await load_page(page, url, retries=2):
            print(f"    ✓ resolved")
            return url, prefix
        await asyncio.sleep(1)
    return None

async def bfs_crawl(browser: Browser, start: str, prefix: str) -> list[str]:
    visited: set[str] = set()
    queued:  set[str] = {start}
    queue:   deque[str] = deque([start])
    found:   list[str] = []
    errors = 0

    ctx  = await make_context(browser)
    page = await ctx.new_page()
    try:
        while queue:
            url = queue.popleft()
            if url in visited:
                continue
            await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))
            ok = await load_page(page, url)
            visited.add(url)
            if not ok:
                errors += 1
                if errors >= 8:
                    print(colored("    [↻] Recreating scrape context…", "yellow"))
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                    ctx  = await make_context(browser)
                    page = await ctx.new_page()
                    errors = 0
                continue
            errors = 0
            if is_downloadable_page(url) and url not in found:
                found.append(url)
            to_crawl, to_record = await links_on_page(page, prefix)
            for lnk in to_record:
                if lnk not in found:
                    found.append(lnk)
            for lnk in to_crawl:
                if lnk not in visited and lnk not in queued:
                    queued.add(lnk)
                    queue.append(lnk)
            print(f"    [{len(found):>4} found | {len(queue):>4} queued]  {url}")
    finally:
        try:
            await ctx.close()
        except Exception:
            pass
    return sorted(set(found))

async def run_scraper(browser: Browser) -> list[str]:
    done: dict[str, list[str]] = {}
    if SCRAPE_CHECKPOINT.exists():
        try:
            done = json.loads(SCRAPE_CHECKPOINT.read_text())
            print(colored(f"  [↻] Resuming scrape ({len(done)} subjects done)", "yellow"))
        except Exception:
            pass

    probe_ctx  = await make_context(browser)
    probe_page = await probe_ctx.new_page()
    try:
        for subject, candidates in SUBJECTS.items():
            if subject in done:
                print(colored(f"  [SKIP] {subject}", "yellow"))
                continue
            print(colored(f"\n  ── {subject}", "cyan"))
            resolved = await resolve_start(probe_page, candidates)
            if not resolved:
                print(colored("  ✗ no working URL", "red"))
                done[subject] = []
            else:
                start, prefix = resolved
                urls = await bfs_crawl(browser, start, prefix)
                done[subject] = urls
                print(colored(f"  ✓ {len(urls)} URLs", "green"))
            SCRAPE_CHECKPOINT.write_text(json.dumps(done, indent=2))
    finally:
        try:
            await probe_ctx.close()
        except Exception:
            pass

    all_urls: list[str] = []
    seen: set[str] = set()
    for urls in done.values():
        for u in urls:
            if u not in seen:
                all_urls.append(u)
                seen.add(u)

    with open(URLS_FILE, "w", encoding="utf-8") as f:
        for u in all_urls:
            f.write(u + "\n")
    print(colored(f"\n  [+] {len(all_urls)} URLs saved to {URLS_FILE}", "green"))
    SCRAPE_CHECKPOINT.unlink(missing_ok=True)
    return all_urls


# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 2 — DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════

def load_progress() -> set[str]:
    if PROGRESS_FILE.is_file():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            done = set(data.get("done", []))
            print(colored(f"  [↻] Resuming — {len(done)} URLs done", "yellow"))
            return done
        except Exception:
            pass
    return set()

def save_progress(done: set[str]) -> None:
    PROGRESS_FILE.write_text(json.dumps({"done": list(done)}, indent=2))

async def run_downloader(urls: list[str], cookies: list[dict], debug: bool = False) -> None:
    done    = load_progress()
    todo    = [u for u in urls if u not in done]
    skipped: list[str] = []
    jar     = cookies_to_jar(cookies)

    print(colored(f"  [+] {len(todo)} pages to process  ({len(urls)-len(todo)} already done)\n", "green"))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    start         = time.time()
    consec_errors = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-gpu", "--no-sandbox", "--disable-setuid-sandbox"],
        )
        ctx = await make_context(browser, cookies)

        for i, url in enumerate(todo, 1):
            elapsed = time.time() - start
            if i > 1:
                eta_s   = int(elapsed / (i - 1) * (len(todo) - i + 1))
                eta_str = f"{eta_s // 60}m {eta_s % 60}s"
            else:
                eta_str = "…"

            print(colored(f"\n{'─'*60}", "cyan"))
            print(colored(f"  [{i}/{len(todo)}]  ETA: {eta_str}", "cyan"))
            print(colored(f"  {url}", "cyan"))

            await asyncio.sleep(random.uniform(BETWEEN_PAGES_MIN, BETWEEN_PAGES_MAX))
            ok = await process_url(url, ctx, jar, debug=debug)

            try:
                fresh = await ctx.cookies(["https://www.savemyexams.com"])
                if fresh:
                    jar = cookies_to_jar(fresh)
            except Exception:
                pass

            if ok:
                done.add(url)
                save_progress(done)
                consec_errors = 0
            else:
                skipped.append(url)
                consec_errors += 1
                if consec_errors >= 6:
                    print(colored("  [↻] Too many errors — recreating browser context…", "yellow"))
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                    ctx = await make_context(browser, cookies)
                    consec_errors = 0

        try:
            await ctx.close()
        except Exception:
            pass
        try:
            await browser.close()
        except Exception:
            pass

    if skipped:
        SKIPPED_FILE.write_text("\n".join(skipped))
        print(colored(f"\n  [!] {len(skipped)} skipped — see {SKIPPED_FILE}", "yellow"))
    elif PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    total = int(time.time() - start)
    print(colored(
        f"\n  Done!  {len(todo)-len(skipped)}/{len(todo)} pages  "
        f"({total//60}m {total%60}s)",
        "green"
    ))
    print(colored(f"  Output: {OUTPUT_DIR}", "green"))


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def print_banner():
    print(colored(r"""
  _____          _        ___       _
 |_   _|__  _ __(_) ___  / _ \ _  _| |_ ___
   | |/ _ \| '_ \ |/ __|| | | | || |  _(_-<
   |_|\___/| .__/_|\__ \| |_| | || | |_/__/
           |_|          \__\_\\_,_|\__\___/
  SaveMyExams — Topic Questions Downloader
""", "cyan"))

async def main():
    print_banner()
    cookies = load_cookies()

    if URLS_FILE.is_file():
        existing = [l.strip() for l in URLS_FILE.read_text().splitlines()
                    if l.strip() and not l.startswith("#")]
        ans = input(colored(
            f"[?] Found {len(existing)} URLs in {URLS_FILE.name}. Re-scrape? [yes/NO]: ",
            "yellow")).strip().lower()
        if ans.startswith("y"):
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                urls = await run_scraper(browser)
                await browser.close()
        else:
            urls = existing
            print(colored(f"  [+] Using {len(urls)} URLs", "green"))
    else:
        print(colored("  [~] No URLs file — running scraper first…", "yellow"))
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            urls = await run_scraper(browser)
            await browser.close()

    if not urls:
        print(colored("  [-] No URLs — exiting", "red"))
        return

    print(colored(f"\n  [+] {len(urls)} total URLs\n", "green"))
    ans = input(colored("[?] Proceed with download? [YES/no]: ", "yellow")).strip().lower()
    if ans.startswith("n"):
        print("Exiting.")
        return

    DEBUG_MODE = False
    await run_downloader(urls, cookies, debug=DEBUG_MODE)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(colored("\n  Interrupted.", "yellow"))
    except Exception as e:
        print(colored(f"  Fatal error: {e}", "red"))
    finally:
        input(colored("\nPress [ENTER] to close", "magenta"))