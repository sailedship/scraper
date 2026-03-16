#!/usr/bin/env python3
"""
CAIE IGCSE Past Papers Downloader
- Fetches exact file list from XtremePapers directory API (zero 404 probing)
- Each subject downloads in its own process simultaneously
- Sequential requests within each process (server-friendly)
- PDF magic byte validation

Setup:
    pip install requests beautifulsoup4 tqdm

Run:
    python caie_scraper.py
"""

import os
import re
import time
import requests
import multiprocessing
from bs4 import BeautifulSoup
from tqdm import tqdm

# ─── Config ───────────────────────────────────────────────────────────────────

OUTPUT_DIR     = "./CAIE_Papers"
DOWNLOAD_DELAY = 0.3
MAX_RETRIES    = 3
START_YEAR     = 2019

SUBJECTS = {
    "English First Language (0500)":    "English - First Language (0500)",
    "Mathematics (0580)":               "Mathematics (0580)",
    "Computer Science (0478)":          "Computer Science (0478)",
    "Physics (0625)":                   "Physics (0625)",
    "Chemistry (0620)":                 "Chemistry (0620)",
    "Pakistan Studies (0448)":          "Pakistan Studies (0448)",
    "Urdu First Language (3247)":       "Urdu - First Language (3247)",
    "Islamiyat (0493)":                 "Islamiyat (0493)",
    "Global Perspectives (0457)":       "Global Perspectives (0457)",
    "Business Studies (0450)":          "Business Studies (0450)",
    "Economics (0455)":                 "Economics (0455)",
    "Accounting (0452)":                "Accounting (0452)",
    "Environmental Management (0680)":  "Environmental Management (0680)",
    "Urdu as a Second Language (0539)": "Urdu as a Second Language (0539)",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def safe(name):
    return re.sub(r'[<>:"/\\|?*]', "_", name).strip()

def year_from_filename(fname):
    """Extract year from CAIE filename e.g. 0580_s23_qp_41.pdf -> 2023"""
    m = re.search(r'_[smwy](\d{2})_', fname)
    return (2000 + int(m.group(1))) if m else None

def session_label_from_filename(fname):
    """e.g. 0580_s23_qp_41.pdf -> 'May June', w -> 'Oct Nov', m -> 'Feb March'"""
    m = re.search(r'_([smwy])(\d{2})_', fname)
    if not m:
        return "Other"
    return {"s": "May June", "w": "Oct Nov", "m": "Feb March", "y": "Specimen"}.get(m.group(1), "Other")

# ─── Directory listing ────────────────────────────────────────────────────────

def get_file_list(subject_folder):
    """
    Fetch the XtremePapers directory listing for a subject and return
    list of (filename, url) for all PDFs from START_YEAR onwards.
    """
    dirpath = f"./CAIE/IGCSE/{subject_folder}/"
    url = f"https://papers.xtremepape.rs/index.php?dirpath={requests.utils.quote(dirpath)}&order=1"

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  ✗ Could not fetch directory listing: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        fname = href.split("/")[-1]
        year = year_from_filename(fname)
        if year is None or year < START_YEAR:
            continue
        full_url = f"https://papers.xtremepape.rs/{href}"
        results.append((fname, full_url))

    return results

# ─── Download ─────────────────────────────────────────────────────────────────

def download_file(url, dest):
    """Returns 'ok' | 'exists' | 'fail'"""
    if os.path.exists(dest):
        return "exists"
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20, stream=True)
            r.raise_for_status()
            data = r.content
            if not data.startswith(b"%PDF"):
                return "fail"
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
            return "ok"
        except requests.RequestException:
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
    return "fail"

# ─── Per-subject worker (runs in its own process) ─────────────────────────────

def download_subject(args):
    subject_name, subject_folder = args
    code = re.search(r'\((\w+)\)', subject_folder).group(1)

    print(f"  🔍 [{code}] Fetching file list...")
    files = get_file_list(subject_folder)

    if not files:
        print(f"  ✗ [{code}] No files found — check folder name")
        return subject_name, 0, 0, 0

    print(f"  📋 [{code}] Found {len(files)} files to download")

    ok = fail = already = 0

    for fname, url in tqdm(files, desc=f"  {code}", unit="pdf", position=list(SUBJECTS.keys()).index(subject_name)):
        year = year_from_filename(fname)
        session = session_label_from_filename(fname)
        dest_dir = os.path.join(OUTPUT_DIR, safe(subject_folder), f"{year} {session}")
        dest = os.path.join(dest_dir, fname)

        result = download_file(url, dest)
        if result == "ok":
            ok += 1
            time.sleep(DOWNLOAD_DELAY)
        elif result == "exists":
            already += 1
        else:
            fail += 1
            tqdm.write(f"  ✗ [{code}] Failed: {fname}")

    print(f"  ✅ [{code}] {ok} downloaded | {already} already had | {fail} failed")
    return subject_name, ok, already, fail

# ─── Cleanup ──────────────────────────────────────────────────────────────────

def delete_corrupted():
    removed = 0
    for root, _, files in os.walk(OUTPUT_DIR):
        for f in files:
            if not f.endswith(".pdf"):
                continue
            path = os.path.join(root, f)
            try:
                with open(path, "rb") as fh:
                    if not fh.read(4).startswith(b"%PDF"):
                        os.remove(path)
                        removed += 1
            except Exception:
                pass
    print(f"  🧹 Removed {removed} corrupted files" if removed else "  ✅ No corrupted files found")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("CAIE IGCSE Past Papers Downloader")
    print("=" * 50)

    if os.path.exists(OUTPUT_DIR):
        print("Scanning for corrupted files...")
        delete_corrupted()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\nOutput : {os.path.abspath(OUTPUT_DIR)}")
    print(f"Years  : {START_YEAR} → latest")
    print(f"Subjects: {len(SUBJECTS)} (each in its own process)\n")

    args = [(name, folder) for name, folder in SUBJECTS.items()]

    # One process per subject — all run simultaneously
    with multiprocessing.Pool(processes=len(SUBJECTS)) as pool:
        results = pool.map(download_subject, args)

    print("\n\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    total_ok = total_already = total_fail = 0
    for subject_name, ok, already, fail in results:
        print(f"  {subject_name}: {ok} downloaded | {already} cached | {fail} failed")
        total_ok += ok
        total_already += already
        total_fail += fail

    print(f"\n  Total: {total_ok} downloaded | {total_already} already had | {total_fail} failed")
    print(f"\n🎉 Done! Papers saved to: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()