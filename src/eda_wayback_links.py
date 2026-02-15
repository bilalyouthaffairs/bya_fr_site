import re
import json
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pandas as pd


def safe_get(d, k):
    return d[k] if k in d else None


def classify_url(original_url: str):
    """
    Very lightweight heuristics to group page types.
    You can refine later once you see output.
    """
    u = original_url.lower()

    if "calcium" in u:
        return "calendar_calcium"

    if "calendar" in u and "type=block" in u:
        return "calendar_month_view"

    if "calendar" in u:
        return "calendar_other"

    if "rss" in u or "feed" in u:
        return "feed"

    if "sitemap" in u:
        return "sitemap"

    if u.endswith(".xml"):
        return "xml_other"

    if u.endswith(".php"):
        return "php_page"

    return "other"


def extract_query_keys(url):
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        return sorted(list(qs.keys()))
    except Exception:
        return []


def normalize_path(url):
    try:
        parsed = urlparse(url)
        return parsed.path.lower()
    except Exception:
        return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Path to manifest.json")
    ap.add_argument("--outdir", default="eda_results")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(exist_ok=True)

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))

    rows = []
    for item in manifest:
        original = safe_get(item, "original")
        ts = safe_get(item, "timestamp")
        path = safe_get(item, "path")

        if not original:
            continue

        rows.append({
            "timestamp": ts,
            "year": int(ts[:4]) if ts else None,
            "month": int(ts[4:6]) if ts else None,
            "original": original,
            "path_only": normalize_path(original),
            "query_keys": ",".join(extract_query_keys(original)),
            "page_type_guess": classify_url(original),
            "has_file": bool(path),
        })

    df = pd.DataFrame(rows)

    # --- High-level counts ---
    summary_page_types = (
        df.groupby("page_type_guess")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    summary_paths = (
        df.groupby("path_only")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(50)
    )

    summary_queries = (
        df.groupby("query_keys")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(50)
    )

    # Per year counts
    per_year = (
        df.groupby("year")
        .size()
        .reset_index(name="captures")
        .sort_values("year")
    )

    # Calendar-only subset (likely useful)
    calendar_df = df[df["page_type_guess"].str.contains("calendar", na=False)]

    calendar_paths = (
        calendar_df.groupby("path_only")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    # Save CSVs
    summary_page_types.to_csv(outdir / "page_types.csv", index=False)
    summary_paths.to_csv(outdir / "top_paths.csv", index=False)
    summary_queries.to_csv(outdir / "query_key_patterns.csv", index=False)
    per_year.to_csv(outdir / "captures_per_year.csv", index=False)
    calendar_paths.to_csv(outdir / "calendar_paths.csv", index=False)

    # Helpful console output
    print("\n=== HIGH LEVEL ===")
    print(summary_page_types.head(15).to_string(index=False))

    print("\n=== TOP URL PATHS ===")
    print(summary_paths.head(15).to_string(index=False))

    print("\n=== TOP QUERY PARAM SETS ===")
    print(summary_queries.head(15).to_string(index=False))

    print("\n=== CAPTURES PER YEAR ===")
    print(per_year.to_string(index=False))

    print(f"\nResults written to: {outdir}/")


if __name__ == "__main__":
    main()