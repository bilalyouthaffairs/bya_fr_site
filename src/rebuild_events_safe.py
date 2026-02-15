import json
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

def clean(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

def normalize_title(t):
    t = clean(t).lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def normalize_loc(loc):
    if not loc:
        return None
    loc = clean(loc)
    if loc.lower().startswith("c_"):
        loc = loc[2:]
    return loc

def get_qs(url):
    p = urlparse(url)
    return {k: v[0] for k, v in parse_qs(p.query).items()}

def parse_date_str(d):
    parts = d.split("/")
    if len(parts) < 3:
        return None
    try:
        return f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    except:
        return None

def extract_events(html, calendar_name, source_type):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for ev in soup.find_all("div", class_=re.compile(r"(?i)\bCalEvent\b")):
        # location: prefer inner div class like Masjid
        location_code = None
        inner = ev.find("div")
        for d in ev.find_all("div"):
            classes = d.get("class", [])
            for c in classes:
                if c not in ("CalEvent", "EventLink", "DayWithEvents"):
                    location_code = c
                    break
            if location_code:
                break
        location_code = normalize_loc(location_code)

        # time
        time_span = ev.find("span", class_=re.compile(r"(?i)\bTimeLabel\b"))
        time_label = clean(time_span.get_text(" ")) if time_span else None

        # title
        a = ev.find("a")
        title = clean(a.get_text(" ")) if a else None
        if not title:
            title = clean(ev.get_text(" "))

        event_id = None
        date = None

        # noscript link often has the canonical fields
        ns = ev.find("noscript")
        if ns:
            link = ns.find("a")
            if link and link.get("href"):
                qs = get_qs(link["href"])
                event_id = qs.get("ID")
                date = parse_date_str(qs.get("Date", ""))

        if not date:
            # PopupWindow ('BilalEvents', '2021/5/21', '899', ...)
            txt = str(ev)
            m = re.search(r"PopupWindow\s*\([^,]+,\s*'([^']+)'\s*,\s*'(\d+)'", txt)
            if m:
                date = parse_date_str(m.group(1))
                event_id = m.group(2)

        if not (title and date):
            continue

        rows.append({
            "date": date,
            "year_month": date[:7],
            "year": date[:4],
            "calendar_name": calendar_name,
            "title": title,
            "title_norm": normalize_title(title),
            "time_label": time_label,
            "location_code": location_code,
            "event_id": event_id,
            "source_type": source_type,
        })

    return rows

def parse_manifest(path, source_type):
    items = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = []
    for it in tqdm(items, desc=f"Parsing {source_type}", unit="file"):
        html = Path(it["path"]).read_text(encoding="utf-8", errors="ignore")
        qs = get_qs(it.get("original",""))
        cal = qs.get("CalendarName")
        rows.extend(extract_events(html, cal, source_type))
    return pd.DataFrame(rows)

# 1) parse month_block
df_month = parse_manifest("bilal_wayback_html/manifest_month_block.json", "month_block")

# optional: focus only BilalEvents
df_month = df_month[df_month["calendar_name"] == "BilalEvents"]

month_months = set(df_month["year_month"].dropna().unique())

# 2) parse day_block, but only for missing months
df_day = parse_manifest("bilal_wayback_html/manifest_day_block.json", "day_block")
df_day = df_day[df_day["calendar_name"] == "BilalEvents"]

df_day_missing = df_day[~df_day["year_month"].isin(month_months)]

# 3) combine
df = pd.concat([df_month, df_day_missing], ignore_index=True)

# 4) dedupe in two passes
# Prefer records with an event_id and with a location_code
df["has_event_id"] = df["event_id"].notna().astype(int)
df["has_location"] = df["location_code"].notna().astype(int)

# Source priority: month_block beats day_block
df["source_rank"] = df["source_type"].map({"month_block": 0, "day_block": 1}).fillna(9)

df = df.sort_values(by=["date", "title_norm", "source_rank", "has_event_id", "has_location"], ascending=[True, True, True, False, False])

# If event_id exists, use it for dedupe (strongest)
df_with_id = df[df["event_id"].notna()].drop_duplicates(subset=["date", "event_id"], keep="first")

# For those without id, dedupe by date + title, ignoring location to avoid double counting
df_no_id = df[df["event_id"].isna()].drop_duplicates(subset=["date", "title_norm"], keep="first")

df_final = pd.concat([df_with_id, df_no_id], ignore_index=True)

# 5) stats
per_year = df_final.groupby("year").size().reset_index(name="events").sort_values("year")
print(per_year.to_string(index=False))

print("\nTotal events:", len(df_final))
print("Earliest:", df_final["date"].min())
print("Latest:", df_final["date"].max())

# Save for inspection
df_final.to_csv("bilal_wayback_html/events_master_clean.csv", index=False)
print("\nWrote bilal_wayback_html/events_master_clean.csv")

d2019 = df_final[df_final["year"] == "2019"]
print("\n2019 events per day, top 20:")
print(d2019.groupby("date").size().sort_values(ascending=False).head(20).to_string())

print("\nTop titles:")
print(df_final.groupby("title_norm").size().sort_values(ascending=False).head(20).to_string())

# coverage check

df = pd.read_csv("bilal_wayback_html/events_master_clean.csv")

coverage = (
    df.groupby("year")["year_month"]
      .nunique()
      .reset_index(name="months_with_events")
)

print(coverage.to_string(index=False))