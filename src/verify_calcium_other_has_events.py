import json
import re
import random
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

AUDIT = Path("bilal_wayback_html/audit_report.json")
MANIFEST = Path("bilal_wayback_html/manifest.json")

report = json.loads(AUDIT.read_text(encoding="utf-8"))
manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

# Build a quick lookup from path to type using the examples plus a lightweight reclassify
def classify_html(html: str):
    low = html.lower()
    if 'class="eventpopup"' in low or 'id="eventsummary"' in low:
        return "event_popup"
    if "class=\"calblock\"" in low or "class='calblock'" in low:
        if "class=\"blockview\"" in low or "class='blockview'" in low:
            return "month_block"
        return "calendar_grid_other"
    if "<rss" in low or "<feed" in low or "op=rss" in low:
        return "feed"
    if "calcium web calendar" in low or "brownbearsw.com" in low:
        return "calcium_other"
    return "other"

# Collect calcium_other paths
calcium_other_paths = []
for it in manifest:
    p = it.get("path")
    if not p:
        continue
    path = Path(p)
    if not path.exists():
        continue
    html = path.read_text(encoding="utf-8", errors="ignore")
    if classify_html(html) == "calcium_other":
        calcium_other_paths.append((it.get("original"), path))

print("calcium_other files:", len(calcium_other_paths))

# Sample to keep it fast
SAMPLE_N = min(800, len(calcium_other_paths))
sample = random.sample(calcium_other_paths, SAMPLE_N)

hits = []
for original, path in tqdm(sample, desc="Scanning calcium_other sample", unit="file"):
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    cal_events = len(soup.find_all("div", class_=re.compile(r"(?i)\bCalEvent\b")))
    day_with_events = len(soup.find_all("div", class_=re.compile(r"(?i)\bDayWithEvents\b")))
    event_summary = soup.find(id="EventSummary") is not None

    if cal_events > 0 or day_with_events > 0 or event_summary:
        hits.append({
            "original": original,
            "path": str(path),
            "CalEvent": cal_events,
            "DayWithEvents": day_with_events,
            "EventSummary": event_summary
        })

print("\nHits with event like content:", len(hits))
for h in hits[:25]:
    print(h)