#!/usr/bin/env python3
"""
DOJ Epstein File Downloader
============================
Bulk-download PDFs from the DOJ's Epstein file releases (Datasets 1-12).

The DOJ released ~12 datasets of documents related to the Jeffrey Epstein case.
Datasets 1-7 and 12 are available as zip files. Datasets 8-11 are individual PDFs
that must be downloaded one at a time.

This script brute-forces the EFTA number ranges for each dataset, skipping 404s
and resuming where it left off. Only requires the `requests` library.

Requirements:
    pip install requests

Usage:
    python doj_epstein_downloader.py bruteforce 10                  # Download dataset 10
    python doj_epstein_downloader.py bruteforce 11 --workers 16     # Faster with more workers
    python doj_epstein_downloader.py bruteforce 10 --delay 0.1      # Shorter delay between batches
    python doj_epstein_downloader.py generate 10                    # Just generate URL list
    python doj_epstein_downloader.py status                         # Show download progress

How it works:
    1. The DOJ requires an age verification cookie to access files. This script
       sets that cookie automatically (justiceGovAgeVerified=true).
    2. Each PDF is named EFTA{number}.pdf. We know the EFTA number ranges per dataset.
    3. The script tries each number, downloads real files, and skips 404s.
    4. It's fully resumable - existing files on disk are skipped.
    5. Rate limiting is handled with automatic backoff.

EFTA ranges (discovered from DOJ listing pages):
    Dataset 8:  EFTA00000001 - EFTA00423792  (needs verification)
    Dataset 9:  EFTA00423793 - EFTA01262781  (needs verification)
    Dataset 10: EFTA01262782 - EFTA02212882  (~950K possible URLs)
    Dataset 11: EFTA02212883 - EFTA02730264  (~517K possible URLs)

Note: Not every EFTA number has a file. Many will 404. The script handles this.

Datasets 1-7 and 12 are available as zip downloads from the DOJ website directly:
    https://www.justice.gov/epstein/doj-disclosures
"""

import os
import sys
import re
import shutil
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# --- Configuration ---

BASE_DIR = Path("./epstein_files")
DOWNLOAD_DIR = BASE_DIR / "downloads"
URL_LIST_DIR = BASE_DIR / "url_lists"

DOJ_BASE = "https://www.justice.gov"

# Known EFTA ranges per dataset
# Ranges for 8 and 9 are estimated - adjust if you find the actual boundaries
DATASET_RANGES = {
    8:  {"start": 1,       "end": 423792,  "dir": "DataSet%208"},
    9:  {"start": 423793,  "end": 1262781, "dir": "DataSet%209"},
    10: {"start": 1262782, "end": 2212882, "dir": "DataSet%2010"},
    11: {"start": 2212883, "end": 2730264, "dir": "DataSet%2011"},
}


def get_session():
    """Create a requests session with DOJ age verification cookie."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
    })
    session.cookies.set("justiceGovAgeVerified", "true", domain=".justice.gov")
    return session


def download_pdf(url: str, dest_dir: Path, session: requests.Session):
    """Download a single PDF. Returns 'ok', 'skip', or 'fail'."""
    filename = requests.utils.unquote(url.split("/")[-1])
    dest = dest_dir / filename

    if dest.exists() and dest.stat().st_size > 100:
        return "skip"

    try:
        resp = session.get(url, timeout=60)

        if resp.status_code == 404:
            return "skip"
        if resp.status_code == 403:
            return "ratelimit"

        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        if "text/html" in content_type and len(resp.content) < 200_000:
            return "fail"  # got age gate or error page instead of PDF
        if len(resp.content) < 100:
            return "skip"

        dest.write_bytes(resp.content)
        return "ok"
    except Exception:
        return "fail"


def generate_url_list(dataset_num: int):
    """Generate a URL list for all EFTA numbers in a dataset's range."""
    if dataset_num not in DATASET_RANGES:
        print(f"No known range for dataset {dataset_num}")
        print(f"Known datasets: {sorted(DATASET_RANGES.keys())}")
        return

    r = DATASET_RANGES[dataset_num]
    URL_LIST_DIR.mkdir(parents=True, exist_ok=True)
    out_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"

    total = r["end"] - r["start"] + 1
    print(f"\n=== GENERATING URL LIST FOR DATASET {dataset_num} ===")
    print(f"  Range: EFTA{r['start']:08d} - EFTA{r['end']:08d}")
    print(f"  Total URLs: {total:,}")

    with open(out_file, "w") as f:
        for n in range(r["start"], r["end"] + 1):
            f.write(f"{DOJ_BASE}/epstein/files/{r['dir']}/EFTA{n:08d}.pdf\n")

    print(f"  Saved to: {out_file}")
    return total


def download_bruteforce(dataset_num: int, workers: int = 4, delay: float = 0.3):
    """Download a dataset by trying every EFTA number in the range.

    Fully resumable - skips files already on disk.
    Handles rate limiting with automatic backoff.
    """
    url_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"

    if not url_file.exists():
        print(f"No URL list for dataset {dataset_num}. Generating...")
        generate_url_list(dataset_num)

    if not url_file.exists():
        return

    session = get_session()

    urls = [u.strip() for u in url_file.read_text().strip().split("\n") if u.strip()]

    dest_dir = DOWNLOAD_DIR / f"DataSet{dataset_num}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    existing = set(f.name for f in dest_dir.glob("*.pdf"))
    remaining = [u for u in urls if requests.utils.unquote(u.split("/")[-1]) not in existing]

    print(f"\n=== BRUTE-FORCE DOWNLOADING DATASET {dataset_num} ===", flush=True)
    print(f"  Total URLs to try: {len(urls):,}", flush=True)
    print(f"  Already have: {len(existing):,}", flush=True)
    print(f"  Remaining: {len(remaining):,}", flush=True)

    stat = shutil.disk_usage(BASE_DIR)
    print(f"  Disk space: {stat.free / (1024**3):.1f}GB free", flush=True)
    print(f"  Workers: {workers}, delay: {delay}s\n", flush=True)

    if not remaining:
        print("  Nothing to download!", flush=True)
        return

    ok = 0
    skipped = 0
    failed = 0
    ratelimited = 0
    start_time = time.time()

    batch_size = 500

    with ThreadPoolExecutor(max_workers=workers) as executor:
        batch_start = 0

        while batch_start < len(remaining):
            batch_end = min(batch_start + batch_size, len(remaining))
            batch = remaining[batch_start:batch_end]

            futures = {
                executor.submit(download_pdf, url, dest_dir, session): url
                for url in batch
            }

            batch_ratelimited = 0
            for future in as_completed(futures):
                result = future.result()
                if result == "ok":
                    ok += 1
                elif result == "skip":
                    skipped += 1
                elif result == "ratelimit":
                    ratelimited += 1
                    batch_ratelimited += 1
                else:
                    failed += 1

            total_done = ok + skipped + failed + ratelimited
            elapsed = time.time() - start_time
            rate = total_done / elapsed if elapsed > 0 else 0
            eta_s = (len(remaining) - total_done) / rate if rate > 0 else 0
            print(
                f"  {total_done:,}/{len(remaining):,} "
                f"({ok:,} downloaded, {skipped:,} 404, {ratelimited:,} 403, {failed:,} err) "
                f"[{rate:.0f}/s, ETA {eta_s/60:.0f}m]",
                flush=True,
            )

            # Rate limit backoff
            if batch_ratelimited > batch_size * 0.5:
                backoff = 60
                print(
                    f"  [RATE LIMITED] {batch_ratelimited}/{len(batch)} got 403. "
                    f"Backing off {backoff}s...",
                    flush=True,
                )
                time.sleep(backoff)
            elif batch_ratelimited > 0:
                time.sleep(delay * 5)
            else:
                time.sleep(delay)

            # Disk space check
            if total_done % 5000 == 0:
                stat = shutil.disk_usage(BASE_DIR)
                if stat.free / (1024**3) < 5:
                    print("  [ABORT] Less than 5GB free", flush=True)
                    break

            batch_start = batch_end

    elapsed = time.time() - start_time
    print(
        f"\n  Done in {elapsed/60:.1f}m: {ok:,} downloaded, {skipped:,} skipped (404), "
        f"{ratelimited:,} rate-limited, {failed:,} failed",
        flush=True,
    )


def show_status():
    """Show download progress for all datasets."""
    print("\n=== DOWNLOAD STATUS ===\n")

    if not BASE_DIR.exists():
        print("No epstein_files directory found. Run a download first.")
        return

    stat = shutil.disk_usage(BASE_DIR)
    print(f"Disk space: {stat.free / (1024**3):.1f}GB free\n")

    # URL lists
    URL_LIST_DIR.mkdir(parents=True, exist_ok=True)
    for f in sorted(URL_LIST_DIR.glob("*.txt")):
        count = sum(1 for _ in open(f))
        print(f"URL list: {f.name} ({count:,} URLs)")

    print()

    # Downloaded files
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for d in sorted(DOWNLOAD_DIR.iterdir()):
        if d.is_dir():
            pdfs = list(d.glob("*.pdf"))
            size = sum(f.stat().st_size for f in pdfs)
            print(f"{d.name}: {len(pdfs):,} PDFs ({size / (1024**3):.1f}GB)")

    # Zip files
    zips = list(DOWNLOAD_DIR.glob("*.zip"))
    if zips:
        print(f"\nZip files: {len(zips)}")
        for z in zips:
            print(f"  {z.name}: {z.stat().st_size / (1024**3):.1f}GB")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1].lower()

    if command == "bruteforce":
        if len(sys.argv) < 3:
            print("Usage: python doj_epstein_downloader.py bruteforce <dataset_num> [--workers N] [--delay S]")
            return
        workers = 4
        delay = 0.3
        if "--workers" in sys.argv:
            workers = int(sys.argv[sys.argv.index("--workers") + 1])
        if "--delay" in sys.argv:
            delay = float(sys.argv[sys.argv.index("--delay") + 1])
        download_bruteforce(int(sys.argv[2]), workers=workers, delay=delay)

    elif command == "generate":
        if len(sys.argv) < 3:
            print("Usage: python doj_epstein_downloader.py generate <dataset_num>")
            return
        generate_url_list(int(sys.argv[2]))

    elif command == "status":
        show_status()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
