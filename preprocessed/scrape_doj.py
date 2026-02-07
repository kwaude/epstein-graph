#!/usr/bin/env python3
"""
Scrape DOJ Epstein file listing pages and bulk-download PDFs.

Usage:
    python scrape_doj.py bruteforce 11              # Brute-force download dataset 11
    python scrape_doj.py bruteforce 10 --workers 8  # With fewer workers
    python scrape_doj.py generate 11                # Just generate URL list
    python scrape_doj.py download 9                 # Download from existing URL list
    python scrape_doj.py scrape 10                  # Scrape listing pages (slow, gets 403'd)
    python scrape_doj.py status                     # Show progress

Requires browser cookies from a session that passed the DOJ age gate.
"""

import os
import sys
import re
import random
import shutil
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_DIR = Path("./epstein_files")
DOWNLOAD_DIR = BASE_DIR / "downloads"
URL_LIST_DIR = BASE_DIR / "url_lists"

DOJ_BASE = "https://www.justice.gov"

# Datasets that need page scraping (no zip available)
SCRAPE_DATASETS = {
    10: {
        "url": "/epstein/doj-disclosures/data-set-10-files",
        "file_prefix": "/epstein/files/DataSet%2010/",
    },
    11: {
        "url": "/epstein/doj-disclosures/data-set-11-files",
        "file_prefix": "/epstein/files/DataSet%2011/",
    },
}


def get_session():
    """Create a requests session with DOJ age verification cookie."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    })
    session.cookies.set('justiceGovAgeVerified', 'true', domain='.justice.gov')
    return session


def scrape_dataset_urls(dataset_num: int, session: requests.Session = None):
    """Scrape all PDF URLs from a DOJ dataset listing page."""
    if dataset_num not in SCRAPE_DATASETS:
        print(f"Dataset {dataset_num} not configured for scraping")
        return

    config = SCRAPE_DATASETS[dataset_num]
    if session is None:
        session = get_session()

    URL_LIST_DIR.mkdir(parents=True, exist_ok=True)
    out_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"

    all_urls = []
    page = 0
    consecutive_errors = 0
    consecutive_empty = 0

    # Resume from existing partial scrape
    if out_file.exists():
        existing = out_file.read_text().strip().split('\n')
        existing = [u for u in existing if u.strip()]
        if existing:
            all_urls = existing
            # Estimate what page we were on (50 per page)
            page = len(existing) // 50
            print(f"  Resuming from {len(existing)} existing URLs (page ~{page})")

    print(f"\n=== SCRAPING DATASET {dataset_num} URLs ===\n")

    pattern = re.compile(
        r'href="(' + re.escape(config['file_prefix']) + r'[^"]+\.pdf)"',
        re.IGNORECASE
    )

    while True:
        url = f"{DOJ_BASE}{config['url']}?page={page}"
        try:
            # Randomize delay to avoid bot detection
            time.sleep(1.5 + random.uniform(0.5, 2.0))

            resp = session.get(url, timeout=60)

            if resp.status_code == 403:
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    # Back off longer
                    backoff = min(30, 5 * consecutive_errors)
                    print(f"  [RATE LIMITED] Backing off {backoff}s...")
                    time.sleep(backoff)
                if consecutive_errors >= 10:
                    print(f"  [ABORT] Too many 403s. Saving {len(all_urls)} URLs so far.")
                    break
                page += 1
                continue

            resp.raise_for_status()
            consecutive_errors = 0

            matches = pattern.findall(resp.text)

            if not matches:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            consecutive_empty = 0
            for m in matches:
                full_url = DOJ_BASE + m
                if full_url not in all_urls:
                    all_urls.append(full_url)

            if page % 50 == 0:
                print(f"  Page {page}: {len(all_urls)} URLs so far...")
                # Save progress periodically
                with open(out_file, 'w') as f:
                    f.write('\n'.join(all_urls) + '\n')

            page += 1

        except requests.exceptions.HTTPError as e:
            if '403' in str(e):
                consecutive_errors += 1
                backoff = min(30, 5 * consecutive_errors)
                print(f"  [403] Page {page}, backing off {backoff}s (attempt {consecutive_errors})")
                time.sleep(backoff)
                if consecutive_errors >= 10:
                    print(f"  [ABORT] Too many 403s.")
                    break
                continue
            print(f"  [ERROR] Page {page}: {e}")
            page += 1
        except Exception as e:
            print(f"  [ERROR] Page {page}: {e}")
            consecutive_errors += 1
            if consecutive_errors >= 10:
                break
            time.sleep(3)
            page += 1

    # Write URL list
    with open(out_file, 'w') as f:
        for u in all_urls:
            f.write(u + "\n")

    print(f"\n  Scraped {len(all_urls)} URLs across {page} pages")
    print(f"  Saved to: {out_file}")
    return all_urls


def download_pdf(url: str, dest_dir: Path, session: requests.Session) -> bool:
    """Download a single PDF."""
    filename = url.split("/")[-1]
    # URL decode the filename
    filename = requests.utils.unquote(filename)
    dest = dest_dir / filename

    if dest.exists() and dest.stat().st_size > 100:
        return True  # already downloaded

    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()

        content_type = resp.headers.get('content-type', '')
        if 'text/html' in content_type and len(resp.content) < 200_000:
            return False  # got age gate page

        dest.write_bytes(resp.content)
        return True
    except Exception:
        return False


def download_dataset(dataset_num, session: requests.Session = None, workers: int = 8):
    """Download all PDFs for a dataset from its URL list."""
    url_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"
    if not url_file.exists():
        print(f"No URL list found for dataset {dataset_num}")
        print(f"Run: python scrape_doj.py scrape {dataset_num}")
        return

    if session is None:
        session = get_session()

    urls = url_file.read_text().strip().split('\n')
    urls = [u.strip() for u in urls if u.strip()]

    dest_dir = DOWNLOAD_DIR / f"DataSet{dataset_num}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Check what's already downloaded
    existing = set(f.name for f in dest_dir.glob("*.pdf"))
    remaining = [u for u in urls if requests.utils.unquote(u.split("/")[-1]) not in existing]

    print(f"\n=== DOWNLOADING DATASET {dataset_num} ===")
    print(f"  Total files: {len(urls)}")
    print(f"  Already downloaded: {len(existing)}")
    print(f"  Remaining: {len(remaining)}")

    stat = shutil.disk_usage(BASE_DIR)
    print(f"  Disk space: {stat.free / (1024**3):.1f}GB free\n")

    if not remaining:
        print("  Nothing to download!")
        return

    downloaded = 0
    failed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(download_pdf, url, dest_dir, session): url
            for url in remaining
        }

        for future in as_completed(futures):
            if future.result():
                downloaded += 1
            else:
                failed += 1

            total_done = downloaded + failed
            if total_done % 500 == 0:
                elapsed = time.time() - start_time
                rate = total_done / elapsed if elapsed > 0 else 0
                eta_s = (len(remaining) - total_done) / rate if rate > 0 else 0
                print(f"  Progress: {total_done}/{len(remaining)} "
                      f"({downloaded} ok, {failed} failed) "
                      f"[{rate:.0f}/s, ETA {eta_s/60:.0f}m]")

                # Check disk space every 500 files
                stat = shutil.disk_usage(BASE_DIR)
                if stat.free / (1024**3) < 5:
                    print("  [ABORT] Less than 5GB free — stopping downloads")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

    elapsed = time.time() - start_time
    print(f"\n  Done: {downloaded} downloaded, {failed} failed in {elapsed/60:.1f}m")


# Known EFTA ranges per dataset (from analysis of DOJ pages)
DATASET_RANGES = {
    8:  {"start": 1,       "end": 423792,  "dir": "DataSet%208"},
    9:  {"start": 423793,  "end": 1262781, "dir": "DataSet%209"},
    10: {"start": 1262782, "end": 2212882, "dir": "DataSet%2010"},
    11: {"start": 2212883, "end": 2730264, "dir": "DataSet%2011"},
}


def generate_url_list(dataset_num: int):
    """Generate a URL list by brute-forcing the EFTA number range."""
    if dataset_num not in DATASET_RANGES:
        print(f"No known range for dataset {dataset_num}")
        print(f"Known ranges: {list(DATASET_RANGES.keys())}")
        return

    r = DATASET_RANGES[dataset_num]
    URL_LIST_DIR.mkdir(parents=True, exist_ok=True)
    out_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"

    total = r["end"] - r["start"] + 1
    print(f"\n=== GENERATING URL LIST FOR DATASET {dataset_num} ===")
    print(f"  Range: EFTA{r['start']:08d} - EFTA{r['end']:08d}")
    print(f"  Total URLs: {total:,}")

    with open(out_file, 'w') as f:
        for n in range(r["start"], r["end"] + 1):
            f.write(f"{DOJ_BASE}/epstein/files/{r['dir']}/EFTA{n:08d}.pdf\n")

    print(f"  Saved to: {out_file}")
    return total


def download_pdf_tolerant(url: str, dest_dir: Path, session: requests.Session):
    """Download a single PDF, tolerating 404s. Returns 'ok', 'skip', or 'fail'."""
    filename = url.split("/")[-1]
    filename = requests.utils.unquote(filename)
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

        content_type = resp.headers.get('content-type', '')
        if 'text/html' in content_type and len(resp.content) < 200_000:
            return "fail"

        if len(resp.content) < 100:
            return "skip"

        dest.write_bytes(resp.content)
        return "ok"
    except Exception:
        return "fail"


def download_bruteforce(dataset_num: int, workers: int = 4, delay: float = 0.3, start_from: int = 0):
    """Download dataset by trying every EFTA number in the range.

    Uses conservative defaults to avoid rate limiting.
    """
    url_file = URL_LIST_DIR / f"dataset{dataset_num}_urls.txt"

    if not url_file.exists():
        print(f"No URL list for dataset {dataset_num}. Generating...")
        generate_url_list(dataset_num)

    if not url_file.exists():
        return

    session = get_session()

    urls = url_file.read_text().strip().split('\n')
    urls = [u.strip() for u in urls if u.strip()]

    # Skip URLs before start_from line number (1-indexed)
    if start_from > 0:
        urls = urls[start_from - 1:]
        print(f"  Resuming from line {start_from:,}", flush=True)

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

    with ThreadPoolExecutor(max_workers=workers) as executor:
        batch_start = 0
        batch_size = 500  # process in small batches so we can pause between

        while batch_start < len(remaining):
            batch_end = min(batch_start + batch_size, len(remaining))
            batch = remaining[batch_start:batch_end]

            futures = {
                executor.submit(download_pdf_tolerant, url, dest_dir, session): url
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
            print(f"  {total_done:,}/{len(remaining):,} "
                  f"({ok:,} ok, {skipped:,} 404, {ratelimited:,} 403, {failed:,} err) "
                  f"[{rate:.0f}/s, ETA {eta_s/60:.0f}m]", flush=True)

            # If we're getting rate limited, back off
            if batch_ratelimited > batch_size * 0.5:
                backoff = 60
                print(f"  [RATE LIMITED] {batch_ratelimited}/{len(batch)} got 403. "
                      f"Backing off {backoff}s...", flush=True)
                time.sleep(backoff)
            elif batch_ratelimited > 0:
                time.sleep(delay * 5)
            else:
                time.sleep(delay)

            # Check disk space
            if total_done % 5000 == 0:
                stat = shutil.disk_usage(BASE_DIR)
                if stat.free / (1024**3) < 5:
                    print("  [ABORT] Less than 5GB free", flush=True)
                    break

            batch_start = batch_end

    elapsed = time.time() - start_time
    print(f"\n  Done in {elapsed/60:.1f}m: {ok:,} downloaded, {skipped:,} skipped, "
          f"{ratelimited:,} rate-limited, {failed:,} failed", flush=True)


def show_status():
    """Show download progress for all datasets."""
    print("\n=== DOWNLOAD STATUS ===\n")

    stat = shutil.disk_usage(BASE_DIR)
    print(f"Disk space: {stat.free / (1024**3):.1f}GB free\n")

    # Check URL lists
    URL_LIST_DIR.mkdir(parents=True, exist_ok=True)
    for f in sorted(URL_LIST_DIR.glob("*.txt")):
        count = sum(1 for _ in open(f))
        print(f"URL list: {f.name} ({count:,} URLs)")

    print()

    # Check downloaded files per dataset
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for d in sorted(DOWNLOAD_DIR.iterdir()):
        if d.is_dir():
            pdfs = list(d.glob("*.pdf"))
            size = sum(f.stat().st_size for f in pdfs)
            print(f"{d.name}: {len(pdfs):,} PDFs ({size / (1024**3):.1f}GB)")

    # Check zip files
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

    if command == "scrape":
        if len(sys.argv) < 3:
            print("Usage: python scrape_doj.py scrape <dataset_num>")
            return
        ds = int(sys.argv[2])
        scrape_dataset_urls(ds)

    elif command == "download":
        if len(sys.argv) < 3:
            print("Usage: python scrape_doj.py download <dataset_num|all>")
            return

        workers = 8
        if "--workers" in sys.argv:
            idx = sys.argv.index("--workers")
            workers = int(sys.argv[idx + 1])

        target = sys.argv[2]
        if target == "all":
            for f in sorted(URL_LIST_DIR.glob("*.txt")):
                # extract dataset number from filename
                match = re.search(r'dataset(\d+)', f.name)
                if match:
                    download_dataset(int(match.group(1)), workers=workers)
        else:
            download_dataset(int(target), workers=workers)

    elif command == "generate":
        if len(sys.argv) < 3:
            print("Usage: python scrape_doj.py generate <dataset_num>")
            return
        generate_url_list(int(sys.argv[2]))

    elif command == "bruteforce":
        if len(sys.argv) < 3:
            print("Usage: python scrape_doj.py bruteforce <dataset_num> [--workers N] [--delay S]")
            return
        workers = 4
        delay = 0.3
        if "--workers" in sys.argv:
            idx = sys.argv.index("--workers")
            workers = int(sys.argv[idx + 1])
        if "--delay" in sys.argv:
            idx = sys.argv.index("--delay")
            delay = float(sys.argv[idx + 1])
        start_from = 0
        if "--start-from" in sys.argv:
            idx = sys.argv.index("--start-from")
            start_from = int(sys.argv[idx + 1])
        download_bruteforce(int(sys.argv[2]), workers=workers, delay=delay, start_from=start_from)

    elif command == "status":
        show_status()

    else:
        print(f"Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
