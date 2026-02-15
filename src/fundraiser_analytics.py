import re
from pathlib import Path
import pandas as pd


IN_CSV = Path("bilal_wayback_html/events_master_clean.csv")
OUT_DIR = Path("bilal_wayback_html/fundraiser_outputs")


def clean(s) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.replace("\xa0", " ")).strip()


def canonical_title(t: str) -> str:
    t = clean(t).lower()

    # Remove "host ..." suffixes
    t = re.sub(r"\bhost\b.*$", "", t).strip()

    # Normalize punctuation to spaces
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    # Hard coded typo and alias fixes
    # Add more entries here as you find issues
    FIXES = {
        "arabic scool at bilal": "arabic school at bilal",
        "arabic scool": "arabic school",
    }
    if t in FIXES:
        t = FIXES[t]

    return t


def canonical_loc(loc: str) -> str:
    loc = clean(loc)
    if not loc:
        return ""
    if loc.lower().startswith("c_"):
        loc = loc[2:]
    return loc


def pick_time_series(df: pd.DataFrame) -> pd.Series:
    """
    Returns a best-effort time label column.
    If no known time columns exist, returns empty strings.
    """
    candidates = [
        "time_label",
        "TimeLabel",
        "time",
        "start_time",
        "end_time",
        "time_start",
        "time_end",
        "start",
        "end",
    ]
    for c in candidates:
        if c in df.columns:
            return df[c].map(clean).fillna("")
    return pd.Series([""] * len(df), index=df.index)


def make_event_key(df: pd.DataFrame) -> pd.Series:
    """
    Event identity for de-duping multi-venue duplicates:
    date + title_canonical + time_label
    """
    date = df["date"].astype(str).map(clean)
    title = df["title_canonical"].astype(str).map(clean).str.lower()
    time_label = df["time_label"].astype(str).map(clean).str.lower()
    return date + "||" + title + "||" + time_label


def pct_change(cur, prev):
    if prev is None or pd.isna(prev) or prev == 0:
        return None
    return 100.0 * (cur - prev) / prev


def main():
    if not IN_CSV.exists():
        raise FileNotFoundError(f"Missing input file: {IN_CSV}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_CSV)

    # Normalize and fill expected columns
    if "date" not in df.columns:
        raise ValueError("events_master_clean.csv must contain a 'date' column in YYYY-MM-DD format")

    df["date"] = df["date"].astype(str).map(clean)
    df["year_int"] = df["date"].str[:4]
    df = df[df["year_int"].str.match(r"^\d{4}$", na=False)]
    df["year_int"] = df["year_int"].astype(int)
    df["year_month"] = df["date"].str[:7]

    base_title = df.get("title", "").astype(str).map(clean)

    if "title_canonical" in df.columns:
        tc = df["title_canonical"].astype(str).map(clean)
        # if title_canonical is blank, fall back to title
        tc = tc.where(tc.str.len() > 0, base_title)
        df["title_canonical"] = tc.map(canonical_title)
    else:
        df["title_canonical"] = base_title.map(canonical_title)

    # Location
    if "location_code" in df.columns:
        df["location_code"] = df["location_code"].astype(str).map(canonical_loc)
    else:
        df["location_code"] = ""

    # Time
    df["time_label"] = pick_time_series(df)

    # Drop empty canonical titles (defensive)
    df = df[df["title_canonical"].str.len() > 0].copy()

    # Event key
    df["event_key"] = make_event_key(df)

    # Two derived datasets:
    # 1) unique events for event counting
    events_unique = df.drop_duplicates(subset=["event_key"]).copy()

    # 2) venue bookings for venue usage counting
    venue_bookings = (
        df[df["location_code"].str.len() > 0]
        .drop_duplicates(subset=["event_key", "location_code"])
        .copy()
    )

    # Yearly summary based on unique events (not raw rows)
    yearly = (
        events_unique.groupby("year_int")
        .agg(
            events=("event_key", "size"),
            months=("year_month", "nunique"),
            unique_programs=("title_canonical", "nunique"),
            unique_days=("date", "nunique"),
        )
        .reset_index()
        .sort_values("year_int")
    )
    yearly["events_per_month"] = (yearly["events"] / yearly["months"]).round(1)
    yearly["events_per_week_est"] = (yearly["events"] / 52.0).round(1)

    # Growth
    yearly["events_prev"] = yearly["events"].shift(1)
    yearly["growth_pct"] = [
        None if pd.isna(prev) else round(pct_change(cur, prev), 1)
        for cur, prev in zip(yearly["events"], yearly["events_prev"])
    ]

    # Venue usage summary per year (based on bookings)
    venue_year = (
        venue_bookings.groupby("year_int")
        .size()
        .reset_index(name="venue_bookings")
        .sort_values("year_int")
    )
    yearly = yearly.merge(venue_year, on="year_int", how="left").fillna({"venue_bookings": 0})
    yearly["venue_bookings"] = yearly["venue_bookings"].astype(int)

    # Average venues per event by year
    venues_per_event = (
        venue_bookings.groupby(["year_int", "event_key"])
        .size()
        .reset_index(name="venues_used")
        .groupby("year_int")["venues_used"]
        .mean()
        .reset_index(name="avg_venues_per_event")
    )
    venues_per_event["avg_venues_per_event"] = venues_per_event["avg_venues_per_event"].round(2)
    yearly = yearly.merge(venues_per_event, on="year_int", how="left").fillna({"avg_venues_per_event": 0})

    # Notes
    def make_notes(r):
        notes = []
        if r["months"] < 10:
            notes.append("Partial year coverage in archive")
        if r["events_per_month"] < 5:
            notes.append("Low activity year or events moved off calendar")
        return "; ".join(notes)

    yearly["notes"] = yearly.apply(make_notes, axis=1)

    # Write yearly summary
    yearly.to_csv(OUT_DIR / "yearly_summary.csv", index=False)

    # Top programs overall (unique events only)
    prog_overall = (
        events_unique.groupby("title_canonical")
        .size()
        .reset_index(name="occurrences")
        .sort_values("occurrences", ascending=False)
    )
    prog_overall.head(200).to_csv(OUT_DIR / "top_programs_overall.csv", index=False)

    # Programs by year (unique events only)
    prog_year = (
        events_unique.groupby(["year_int", "title_canonical"])
        .size()
        .reset_index(name="occurrences")
        .sort_values(["year_int", "occurrences"], ascending=[True, False])
    )
    prog_year.to_csv(OUT_DIR / "programs_by_year_all.csv", index=False)
    prog_year.groupby("year_int").head(20).to_csv(OUT_DIR / "top20_programs_by_year.csv", index=False)

    # Venue events by year (venue bookings)
    venue_events_by_year = (
        venue_bookings.groupby(["year_int", "location_code"])
        .size()
        .reset_index(name="venue_bookings")
        .sort_values(["year_int", "venue_bookings"], ascending=[True, False])
    )
    venue_events_by_year.to_csv(OUT_DIR / "venue_events_by_year.csv", index=False)

    # Venue first seen year (venue bookings)
    venue_first = (
        venue_bookings.groupby("location_code")["year_int"]
        .min()
        .reset_index(name="first_seen_year")
        .sort_values(["first_seen_year", "location_code"])
    )
    venue_first.to_csv(OUT_DIR / "venue_first_seen_year.csv", index=False)

    # Top days by year (unique events only)
    day_counts = (
        events_unique.groupby(["year_int", "date"])
        .size()
        .reset_index(name="events_that_day")
        .sort_values(["year_int", "events_that_day"], ascending=[True, False])
    )
    day_counts.groupby("year_int").head(10).to_csv(OUT_DIR / "top_days_by_year.csv", index=False)

    # Fundraiser brief
    lines = []
    lines.append("Bilal Masjid calendar history (Wayback archive) fundraiser brief")
    lines.append("")
    lines.append(f"Total unique events in dataset: {len(events_unique)}")
    lines.append(f"Date range: {events_unique['date'].min()} to {events_unique['date'].max()}")
    lines.append("")
    lines.append("Year by year summary (unique events)")
    for _, r in yearly.iterrows():
        y = int(r["year_int"])
        ev = int(r["events"])
        m = int(r["months"])
        epm = r["events_per_month"]
        venues_used = int(r["venue_bookings"])
        avgv = float(r["avg_venues_per_event"])
        g = r["growth_pct"]
        note = clean(r["notes"])

        s = f"{y}: {ev} events across {m} months, about {epm} per month. Venue bookings: {venues_used}. Avg venues per event: {avgv}."
        if g is not None:
            s += f" Change vs prior year: {g}%."
        if note:
            s += f" Note: {note}."
        lines.append(s)

    (OUT_DIR / "fundraiser_brief.txt").write_text("\n".join(lines), encoding="utf-8")

    print("Wrote outputs to:", OUT_DIR)
    print("Key files:")
    print("  yearly_summary.csv")
    print("  venue_events_by_year.csv")
    print("  top20_programs_by_year.csv")
    print("  fundraiser_brief.txt")


if __name__ == "__main__":
    main()