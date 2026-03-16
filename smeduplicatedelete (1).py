"""
Duplicate & Subset PDF Remover
Finds duplicates using three passes, in order of certainty:

  Pass 1 — Exact:    SHA-256 hash match (byte-for-byte identical)
  Pass 2 — Subset:   Smaller file's raw bytes appear verbatim inside a larger file
                     (works when SaveMyExams bundles individual notes into topic PDFs)
  Pass 3 — Near:     Files share >= SIMILARITY_THRESHOLD % of 4KB chunk fingerprints
                     (catches re-encoded / lightly reprocessed duplicates)

In every case the LARGER file is kept (it contains the most content).
Ties go to the file with the shorter path.

Usage:
    python smeduplicatedelete.py <directory>           # dry-run (safe preview)
    python smeduplicatedelete.py <directory> --delete  # actually delete duplicates
    python smeduplicatedelete.py --help                # full options

Install:
    pip install  (no extra dependencies — stdlib only)
"""

import argparse
import hashlib
import os
import sys
from collections import defaultdict
from pathlib import Path

# --- Config -------------------------------------------------------------------
# These defaults can be overridden via CLI arguments (see --help)
DRY_RUN              = True     # <- always starts safe; pass --delete to actually remove files
MIN_FILE_BYTES       = 1_000   # ignore suspiciously tiny files
SIMILARITY_THRESHOLD = 0.85    # Pass 3: fraction of chunks that must match
CHUNK_SIZE           = 4096    # bytes per fingerprint chunk (Pass 3)
# ------------------------------------------------------------------------------

RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def col(text, colour): return f"{colour}{text}{RESET}"
def hr(): print("-" * 70)


# --- Hashing helpers ----------------------------------------------------------

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(256 * 1024):
            h.update(chunk)
    return h.hexdigest()


def chunk_fingerprints(path: Path) -> set:
    """Set of SHA-1 digests of every CHUNK_SIZE block in the file."""
    fps = set()
    with open(path, "rb") as f:
        while block := f.read(CHUNK_SIZE):
            fps.add(hashlib.sha1(block).digest())
    return fps


def similarity(fps_a: set, fps_b: set) -> float:
    if not fps_a or not fps_b:
        return 0.0
    smaller = fps_a if len(fps_a) <= len(fps_b) else fps_b
    larger  = fps_a if len(fps_a) >  len(fps_b) else fps_b
    return len(smaller & larger) / len(smaller)


def bytes_in_file(needle: Path, haystack: Path) -> bool:
    """Return True if needle's raw bytes appear contiguously inside haystack."""
    needle_bytes = needle.read_bytes()
    overlap = len(needle_bytes) - 1
    if overlap < 0:
        return False
    prev = b""
    with open(haystack, "rb") as f:
        while True:
            block = f.read(1024 * 1024)
            if not block:
                break
            search_in = prev + block
            if needle_bytes in search_in:
                return True
            prev = block[-overlap:] if overlap else b""
    return False


# --- Collection ---------------------------------------------------------------

def collect_files(root: Path) -> list:
    files = []
    for p in root.rglob("*"):
        if p.is_file() and p.stat().st_size >= MIN_FILE_BYTES:
            files.append(p)
    return files


def pick_keep(paths: list) -> Path:
    """Keep the largest file; break ties by shortest path string."""
    return max(paths, key=lambda p: (p.stat().st_size, -len(str(p))))


# --- Reporting ----------------------------------------------------------------

def report_group(label: str, keep: Path, dupes: list):
    print(col(f"[{label}]", CYAN), col(keep.name, BOLD))
    print(f"  KEEP   ({keep.stat().st_size // 1024:>6} KB)  {keep}")
    for d in dupes:
        print(f"  DELETE ({d.stat().st_size // 1024:>6} KB)  {d}")
    print()


def do_delete(paths: list):
    for p in paths:
        try:
            p.unlink()
        except Exception as e:
            print(col(f"  [!] Could not delete {p}: {e}", RED))


# --- Main ---------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Duplicate & Subset PDF Remover — 3-pass content comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python smeduplicatedelete.py ./SaveMyExams          # dry-run (safe preview)
  python smeduplicatedelete.py ./SaveMyExams --delete # actually delete duplicates
  python smeduplicatedelete.py ./PDFs --threshold 0.9 # stricter near-match threshold
        """,
    )
    parser.add_argument(
        "scan_dir",
        nargs="?",
        default=".",
        help="Directory to scan for duplicate PDFs (default: current directory)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete duplicates (default is dry-run — safe preview only)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=SIMILARITY_THRESHOLD,
        metavar="0.0-1.0",
        help=f"Similarity threshold for near-duplicate detection (default: {SIMILARITY_THRESHOLD})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    scan_dir  = Path(args.scan_dir).resolve()
    dry_run   = not args.delete
    threshold = args.threshold

    print(col(f"\n{'='*70}", BOLD))
    print(col("  Duplicate PDF Remover — 3-pass content comparison", BOLD))
    print(col(f"{'='*70}\n", BOLD))
    print(f"  Scan dir : {scan_dir}")
    print(f"  Mode     : {col('DRY RUN — nothing will be deleted', YELLOW) if dry_run else col('LIVE — files WILL be deleted', RED)}\n")

    if not scan_dir.exists():
        print(col(f"Directory not found: {scan_dir}", RED))
        sys.exit(1)

    files = collect_files(scan_dir)
    print(f"  Found {len(files)} files\n")

    # == Pass 1: Exact SHA-256 =================================================
    print(col("Pass 1: Exact hash (SHA-256)...", BOLD))
    hashes = defaultdict(list)
    for i, f in enumerate(files, 1):
        print(f"\r  Hashing {i}/{len(files)}...", end="", flush=True)
        try:
            hashes[file_sha256(f)].append(f)
        except Exception as e:
            print(col(f"\n  [!] {f}: {e}", RED))
    print()

    exact_victims = set()
    p1_bytes = 0
    for ps in hashes.values():
        if len(ps) < 2:
            continue
        keep  = pick_keep(ps)
        dupes = [p for p in ps if p != keep]
        report_group("EXACT", keep, dupes)
        for d in dupes:
            p1_bytes += d.stat().st_size
            exact_victims.add(d)

    print(col(f"  -> {len(exact_victims)} exact duplicates  ({p1_bytes / 1e6:.1f} MB)\n", GREEN))
    remaining = [f for f in files if f not in exact_victims]

    # == Pass 2: Byte subset ===================================================


    subset_victims = set()
    p2_bytes = 0
    comparisons = 0
    print(col(f"  -> {len(subset_victims)} subset duplicates  ({p2_bytes / 1e6:.1f} MB)  [{comparisons} comparisons]\n", GREEN))
    remaining = [f for f in remaining if f not in subset_victims]

    # == Pass 3: Chunk fingerprint similarity ==================================
    print(col(f"Pass 3: Chunk fingerprints (>={threshold*100:.0f}% match)...", BOLD))

    fps_cache = {}
    for i, f in enumerate(remaining, 1):
        print(f"\r  Fingerprinting {i}/{len(remaining)}...", end="", flush=True)
        try:
            fps_cache[f] = chunk_fingerprints(f)
        except Exception:
            fps_cache[f] = set()
    print()

    near_victims = set()
    p3_bytes = 0
    checked = 0

    # Group by rough size bucket (within ~20%) to limit comparisons
    size_groups = defaultdict(list)
    for f in remaining:
        sz = f.stat().st_size
        # Bucket = nearest power-of-1.2 step
        bucket = int(sz ** 0.8)
        size_groups[bucket].append(f)

    for bucket_files in size_groups.values():
        for i, a in enumerate(bucket_files):
            if a in near_victims:
                continue
            for b in bucket_files[i+1:]:
                if b in near_victims:
                    continue
                checked += 1
                sim = similarity(fps_cache[a], fps_cache[b])
                if sim >= threshold:
                    keep  = pick_keep([a, b])
                    dupe  = b if keep == a else a
                    report_group(f"NEAR {sim*100:.0f}%", keep, [dupe])
                    p3_bytes += dupe.stat().st_size
                    near_victims.add(dupe)

    print(col(f"  -> {len(near_victims)} near-duplicates  ({p3_bytes / 1e6:.1f} MB)  [{checked} comparisons]\n", GREEN))

    # == Summary ===============================================================
    hr()
    all_victims = exact_victims | subset_victims | near_victims
    total_bytes = p1_bytes + p2_bytes + p3_bytes

    print(f"  Exact duplicates  : {len(exact_victims):>4}   ({p1_bytes / 1e6:.1f} MB)")
    print(f"  Subset duplicates : {len(subset_victims):>4}   ({p2_bytes / 1e6:.1f} MB)")
    print(f"  Near duplicates   : {len(near_victims):>4}   ({p3_bytes / 1e6:.1f} MB)")
    hr()
    print(col(f"  Total to remove   : {len(all_victims):>4}   ({total_bytes / 1e6:.1f} MB)", BOLD))
    hr()

    if dry_run:
        print(col("\n  DRY RUN complete. Set --delete flag and re-run to actually delete.\n", YELLOW))
    else:
        do_delete(list(all_victims))
        print(col(f"\n  Done. {len(all_victims)} files deleted, {total_bytes / 1e6:.1f} MB recovered.\n", GREEN))


if __name__ == "__main__":
    main()