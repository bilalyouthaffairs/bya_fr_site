import json
import re
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


# ---------- helpers ----------

def clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()

def normalize_title(t):
    t = clean(t).lower()
    t = re.sub(r"[^\w\s]", "", t)
    return t

def get_qs(url):
    p = urlparse(url)
    return {k: v[0] for k, v in parse_qs(p.query).items()}

def parse_date_str(d):
    # expects 2021/5/21
    parts = d.split("/")
    if len(parts) < 3:
        return None
    y, m, day = parts[0], parts[1], parts[2]
    try:
        return f"{int(y):04d}-{int(m):02d}-{int(day):02d}"
    except:
        return None


# ---------- month/day event extraction ----------

def extract_events_from_html(html, calendar_name, source_type):
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # each event block
    for ev in soup.find_all("div", class_=re.compile(r"(?i)\bCalEvent\b")):

        # location code from inner div class
        location_code = None
        for d in ev.find_all("div"):
            classes = d.get("class", [])
            for c in classes:
                if c not in ("CalEvent", "EventLink"):
                    if c != "DayWithEvents":
                        location_code = c
                        break
            if location_code:
                break

        # time
        time_span = ev.find("span", class_=re.compile(r"(?i)\bTimeLabel\b"))
        time_label = clean(time_span.get_text(" ")) if time_span else None

        # title
        a = ev.find("a")
        title = clean(a.get_text(" ")) if a else None
        if not title:
            title = clean(ev.get_text(" "))

        # event id + date from popup link if present
        event_id = None
        date = None

        noscript_a = ev.find("noscript")
        if noscript_a:
            link = noscript_a.find("a")
            if link and link.get("href"):
                qs = get_qs(link["href"])
                event_id = qs.get("ID")
                date = parse_date_str(qs.get("Date", ""))

        if not date:
            # fallback from javascript popup call
            txt = str(ev)
            m = re.search(r"PopupWindow\s*\([^']*'([^']+)'\s*,\s*'(\d+)'\)", txt)
            if m:
                date = parse_date_str(m.group(1))
                event_id = m.group(2)

        if not title:
            continue

        rows.append({
            "date": date,
            "title": title,
            "title_norm": normalize_title(title),
            "time_label": time_label,
            "location_code": location_code,
            "calendar_name": calendar_name,
            "event_id": event_id,
            "source_type": source_type
        })

    return rows


# ---------- popup extraction ----------

def extract_popup(html, original_url):
    soup = BeautifulSoup(html, "lxml")

    summary = soup.find(id="EventSummary")
    date_header = soup.find(id="DateHeader")

    if not summary:
        return None

    qs = get_qs(original_url)
    event_id = qs.get("ID")

    return {
        "event_id": event_id,
        "popup_summary": clean(summary.get_text(" ")),
        "popup_date_header": clean(date_header.get_text(" ")) if date_header else None
    }


# ---------- main ----------

def process_manifest(manifest_path, source_type):
    items = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    rows = []

    for it in tqdm(items, desc=f"Parsing {source_type}", unit="file"):
        path = Path(it["path"])
        html = path.read_text(encoding="utf-8", errors="ignore")
        qs = get_qs(it.get("original", ""))

        calendar_name = qs.get("CalendarName")

        rows.extend(
            extract_events_from_html(html, calendar_name, source_type)
        )

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="bilal_events_master.xlsx")
    args = ap.parse_args()

    # ---- parse month and day ----
    df_month = process_manifest(
        "bilal_wayback_html/manifest_month_block.json", "month_block"
    )

    df_day = process_manifest(
        "bilal_wayback_html/manifest_day_block.json", "day_block"
    )

    # month is priority
    all_events = pd.concat([df_month, df_day], ignore_index=True)

    # remove rows without date
    all_events = all_events[all_events["date"].notna()]

    # dedupe across snapshots
    all_events = all_events.drop_duplicates(
        subset=["date", "title_norm", "location_code"]
    )

    # ---- popup enrichment ----
    popup_items = json.loads(
        Path("bilal_wayback_html/manifest_event_popup.json").read_text(encoding="utf-8")
    )

    pop_rows = []
    for it in tqdm(popup_items, desc="Parsing popups", unit="file"):
        html = Path(it["path"]).read_text(encoding="utf-8", errors="ignore")
        p = extract_popup(html, it.get("original", ""))
        if p:
            pop_rows.append(p)

    df_pop = pd.DataFrame(pop_rows)

    if not df_pop.empty:
        all_events = all_events.merge(df_pop, on="event_id", how="left")

    # ---- stats ----
    all_events["year"] = all_events["date"].str[:4]
    all_events["year_month"] = all_events["date"].str[:7]

    per_month = all_events.groupby("year_month").size().reset_index(name="events")
    per_year = all_events.groupby("year").size().reset_index(name="events")
    by_location = (
        all_events.groupby(["year_month", "location_code"])
        .size()
        .reset_index(name="events")
    )

    # ---- output ----
    with pd.ExcelWriter(args.out, engine="openpyxl") as w:
        all_events.to_excel(w, index=False, sheet_name="events_raw")
        per_month.to_excel(w, index=False, sheet_name="events_per_month")
        per_year.to_excel(w, index=False, sheet_name="events_per_year")
        by_location.to_excel(w, index=False, sheet_name="events_by_location")

    print("Wrote:", args.out)
    print("Total unique events:", len(all_events))
    print("Earliest date:", all_events["date"].min())
    print("Latest date:", all_events["date"].max())


if __name__ == "__main__":
    main()