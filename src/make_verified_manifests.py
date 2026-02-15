import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

def get_qs(url):
    p = urlparse(url)
    return {k: v[0] for k, v in parse_qs(p.query).items()}

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

manifest_path = Path("bilal_wayback_html/manifest.json")
data = json.loads(manifest_path.read_text(encoding="utf-8"))

month_block = []
event_popup = []
day_block = []
other = []

for it in data:
    p = it.get("path")
    orig = it.get("original", "")
    if not p or not Path(p).exists():
        continue

    html = Path(p).read_text(encoding="utf-8", errors="ignore")
    html_type = classify_html(html)
    qs = get_qs(orig)
    op = qs.get("Op", "")

    if html_type == "month_block" and op in ("ShowIt", "ShowMonth"):
        month_block.append(it)
        continue

    if html_type == "event_popup":
        event_popup.append(it)
        continue

    # Day pages: Op=ShowDay and has CalEvent blocks inside
    if op == "ShowDay" and re.search(r'(?i)\bCalEvent\b', html):
        day_block.append(it)
        continue

    other.append(it)

Path("bilal_wayback_html/manifest_month_block.json").write_text(json.dumps(month_block, indent=2), encoding="utf-8")
Path("bilal_wayback_html/manifest_event_popup.json").write_text(json.dumps(event_popup, indent=2), encoding="utf-8")
Path("bilal_wayback_html/manifest_day_block.json").write_text(json.dumps(day_block, indent=2), encoding="utf-8")
Path("bilal_wayback_html/manifest_other.json").write_text(json.dumps(other, indent=2), encoding="utf-8")

print("month_block:", len(month_block))
print("event_popup:", len(event_popup))
print("day_block:", len(day_block))
print("other:", len(other))