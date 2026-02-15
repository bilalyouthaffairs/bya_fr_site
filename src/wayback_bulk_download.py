import os
import re
import time
import json
import random
import argparse
from pathlib import Path

import requests
from tqdm import tqdm

WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
WAYBACK_WEB = "https://web.archive.org/web/"

def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; BilalWaybackScraper/1.0)"
    })
    return s

def wayback_url(ts, original):
    return f"{WAYBACK_WEB}{ts}id_/{original}"

def cdx(session, url_prefix, match_type="prefix"):
    params = [
        ("url", url_prefix),
        ("matchType", match_type),
        ("output", "json"),
        ("fl", "timestamp,original,mimetype,statuscode,digest"),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
    ]
    r = session.get(WAYBACK_CDX, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data or len(data) < 2:
        return []
    header = data[0]
    rows = [dict(zip(header, x)) for x in data[1:]]
    return rows

def safe_filename(s):
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s[:180]

def fetch(session, url, retries=4):
    last = None
    for i in range(retries):
        try:
            r = session.get(url, timeout=60)
            if r.status_code == 200 and ("<html" in r.text.lower() or "text/html" in r.headers.get("Content-Type","").lower()):
                return r.text
            last = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last = e
        time.sleep(1.2 * (i + 1) + random.random())
    raise last

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url-prefix", required=True, help="Example: https://bilalmasjid.com/Calendar/Calcium/Calcium40.php")
    ap.add_argument("--outdir", default="wayback_dump")
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--max", type=int, default=0, help="0 means no limit")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    session = make_session()
    print("Listing captures via CDX (HTML only)")
    caps = cdx(session, args.url_prefix, match_type="prefix")

    if args.max and args.max > 0:
        caps = caps[:args.max]

    print(f"Captures found: {len(caps)}")

    manifest = []
    for c in tqdm(caps, desc="Downloading"):
        ts = c["timestamp"]
        orig = c["original"]
        snap = wayback_url(ts, orig)

        sub = outdir / ts[:6]
        sub.mkdir(parents=True, exist_ok=True)

        fn = safe_filename(orig)
        path = sub / f"{ts}_{fn}.html"

        if path.exists():
            manifest.append({"timestamp": ts, "original": orig, "snapshot_url": snap, "path": str(path)})
            continue

        try:
            html = fetch(session, snap)
            path.write_text(html, encoding="utf-8", errors="ignore")
            manifest.append({"timestamp": ts, "original": orig, "snapshot_url": snap, "path": str(path)})
        except Exception as e:
            manifest.append({"timestamp": ts, "original": orig, "snapshot_url": snap, "path": "", "error": str(e)})

        time.sleep(args.sleep + random.random() * 0.2)

    (outdir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Saved manifest: {outdir / 'manifest.json'}")

if __name__ == "__main__":
    main()