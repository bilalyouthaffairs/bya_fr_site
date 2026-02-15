import re
import json
import argparse
from pathlib import Path
from collections import defaultdict, Counter

from bs4 import BeautifulSoup
from tqdm import tqdm


def read_manifest(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def iter_existing_files(manifest_items):
    for it in manifest_items:
        p = it.get("path")
        if p:
            pp = Path(p)
            if pp.exists():
                yield it, pp


def classify_html(html: str):
    low = html.lower()

    # Event popup
    if 'class="eventpopup"' in low or "eventpopup .summary" in low or 'id="eventsummary"' in low:
        return "event_popup"

    # Month block view
    if "class=\"calblock\"" in low or "class='calblock'" in low:
        if "class=\"blockview\"" in low or "class='blockview'" in low:
            return "month_block"
        return "calendar_grid_other"

    # Feeds
    if "<rss" in low or "<feed" in low or "op=rss" in low:
        return "feed"

    # Calcium but not the above
    if "calcium web calendar" in low or "brownbearsw.com" in low:
        return "calcium_other"

    return "other"


def is_xml_doc(html: str) -> bool:
    head = html.lstrip()[:200].lower()
    # Very common signals
    return head.startswith("<?xml") and "<html" not in head


def soup_parse(html: str):
    # For audit we can skip pure XML docs rather than parsing them
    if is_xml_doc(html):
        return None
    return BeautifulSoup(html, "lxml")


def extract_month_block_features(html: str):
    soup = soup_parse(html)
    if soup is None:
        return None

    dh = soup.find("div", class_=re.compile(r"(?i)\bDateHeader\b"))
    date_header = " ".join(dh.stripped_strings) if dh else None

    calevents = len(soup.find_all("div", class_=re.compile(r"(?i)\bCalEvent\b")))
    days_with_events = len(soup.find_all("div", class_=re.compile(r"(?i)\bDayWithEvents\b")))

    return {
        "date_header": date_header,
        "calevents": calevents,
        "days_with_events": days_with_events,
    }


def extract_event_popup_features(html: str):
    soup = soup_parse(html)
    if soup is None:
        return None

    def txt(node):
        if not node:
            return None
        return " ".join(node.stripped_strings)

    return {
        "date_header": txt(soup.find(id="DateHeader")),
        "summary": txt(soup.find(id="EventSummary")),
        "details": txt(soup.find(id="EventDetails")),
        "has_custom_fields": soup.find(id="CustomFields") is not None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="bilal_wayback_html/manifest.json")
    ap.add_argument("--out", default="bilal_wayback_html/audit_report.json")
    ap.add_argument("--sample-per-type", type=int, default=8)
    ap.add_argument("--max-files", type=int, default=0, help="0 means no limit")
    ap.add_argument("--progress-every", type=int, default=2500, help="print counts every N files")
    args = ap.parse_args()

    manifest = read_manifest(Path(args.manifest))
    files = list(iter_existing_files(manifest))

    if args.max_files and args.max_files > 0:
        files = files[:args.max_files]

    counts = Counter()
    examples = defaultdict(list)
    sanity = {"month_block": [], "event_popup": []}
    skipped_xml = 0
    skipped_empty = 0

    for i, (it, path) in enumerate(tqdm(files, desc="Auditing HTML files", unit="file"), start=1):
        html = path.read_text(encoding="utf-8", errors="ignore")
        if not html.strip():
            skipped_empty += 1
            continue

        if is_xml_doc(html):
            skipped_xml += 1
            # Still classify as feed or xml if you want, but skipping is fine for this audit
            # You can optionally set: counts["xml"] += 1
            continue

        t = classify_html(html)
        counts[t] += 1

        if len(examples[t]) < args.sample_per_type:
            examples[t].append({
                "timestamp": it.get("timestamp"),
                "original": it.get("original"),
                "path": str(path),
            })

        if t == "month_block" and len(sanity["month_block"]) < 30:
            f = extract_month_block_features(html)
            if f:
                sanity["month_block"].append({"original": it.get("original"), **f})

        if t == "event_popup" and len(sanity["event_popup"]) < 30:
            f = extract_event_popup_features(html)
            if f:
                sanity["event_popup"].append({"original": it.get("original"), **f})

        if args.progress_every and i % args.progress_every == 0:
            top = ", ".join([f"{k}:{v}" for k, v in counts.most_common(4)])
            print(f"\nProcessed {i} files. Top types: {top}. Skipped xml:{skipped_xml}, empty:{skipped_empty}")

    report = {
        "counts": dict(counts),
        "skipped": {"xml_docs": skipped_xml, "empty_files": skipped_empty},
        "examples": dict(examples),
        "sanity_samples": sanity,
        "notes": {
            "keep_types_for_fundraiser": ["month_block", "event_popup"],
            "month_block_signatures": ["div.BlockView", "table.CalBlock", "div.DateHeader", "div.CalEvent"],
            "event_popup_signatures": ["body.EventPopup", "#EventSummary", "#EventDetails", "#CustomFields"],
        }
    }

    Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("\nWrote audit report:", args.out)
    print("Final counts:")
    for k, v in counts.most_common():
        print(f"  {k}: {v}")
    print(f"Skipped xml docs: {skipped_xml}")
    print(f"Skipped empty files: {skipped_empty}")


if __name__ == "__main__":
    main()