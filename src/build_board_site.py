import re
import calendar
from pathlib import Path
import pandas as pd


IN_EVENTS = Path("bilal_wayback_html/events_master_clean.csv")
IN_OUTDIR = Path("bilal_wayback_html/fundraiser_outputs")
SITE_DIR = Path("bilal_wayback_html/fundraiser_site")
YEAR_DIR = SITE_DIR / "year"


def clean(s):
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
    date = df["date"].astype(str).map(clean)
    title = df["title_canonical"].astype(str).map(clean).str.lower()
    time_label = df["time_label"].astype(str).map(clean).str.lower()
    return date + "||" + title + "||" + time_label


def html_escape(s: str) -> str:
    s = "" if s is None else str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def fmt_int(x):
    try:
        return f"{int(x):,}"
    except:
        return ""


def fmt_float(x, digits=1):
    try:
        return f"{float(x):.{digits}f}"
    except:
        return ""


def write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


CSS = """
:root{
  --bg:#ffffff;
  --fg:#111111;
  --muted:#444444;
  --border:#222222;
  --soft:#f3f3f3;
  --link:#0033cc;
}
html,body{background:var(--bg);color:var(--fg);margin:0;padding:0;font-family:Arial, Helvetica, sans-serif;font-size:20px;line-height:1.45;}
a{color:var(--link);text-decoration:underline;}
a:focus{outline:3px solid #000;}
.container{max-width:1100px;margin:0 auto;padding:24px;}
.header{border-bottom:4px solid var(--border);padding-bottom:14px;margin-bottom:18px;}
.h1{font-size:40px;font-weight:800;margin:0 0 6px 0;}
.sub{color:var(--muted);font-size:20px;margin:0;}
.nav{margin:18px 0 10px 0;}
.nav a{display:inline-block;margin-right:14px;margin-bottom:10px;padding:10px 14px;border:2px solid var(--border);border-radius:10px;text-decoration:none;color:var(--fg);background:var(--soft);font-weight:700;}
.nav a:hover{background:#e9e9e9;}
.card{border:3px solid var(--border);border-radius:14px;padding:16px 16px;margin:16px 0;background:#fff;}
.card h2{font-size:28px;margin:0 0 10px 0;}
.kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;}
.kpi{border:2px solid var(--border);border-radius:12px;padding:12px;background:var(--soft);}
.kpi .label{color:var(--muted);font-weight:700;font-size:18px;margin-bottom:4px;}
.kpi .value{font-weight:900;font-size:28px;}
.note{font-size:18px;color:var(--muted);margin-top:8px;}
.tablewrap{overflow-x:auto;}
table{border-collapse:collapse;width:100%;font-size:18px;}
th,td{border:2px solid var(--border);padding:10px 10px;vertical-align:top;}
th{background:var(--soft);text-align:left;font-weight:900;}
.small{font-size:18px;color:var(--muted);}
.footer{border-top:4px solid var(--border);margin-top:22px;padding-top:12px;color:var(--muted);font-size:18px;}
.printtip{font-size:18px;color:var(--muted);margin-top:10px;}

.calgrid{border-collapse:collapse;width:100%;font-size:18px;}
.calgrid th,.calgrid td{border:2px solid var(--border);padding:8px;vertical-align:top;}
.calgrid th{background:var(--soft);font-weight:900;text-align:center;}
.daynum{font-weight:900;font-size:18px;margin-bottom:6px;}
.daycell{min-height:92px;}
.eventitem{margin:0 0 6px 0;padding:0;}
.monthwrap{margin:14px 0;}
details{border:3px solid var(--border);border-radius:14px;padding:10px 12px;background:#fff;margin:14px 0;}
summary{font-weight:900;font-size:22px;cursor:pointer;}
summary:focus{outline:3px solid #000;}

@media print{
  a{color:#000;text-decoration:none;}
  .nav{display:none;}
  .container{max-width:none;padding:0 14px;}
  .card{break-inside:avoid;}
}
"""


def page_shell(title, body_html):
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html_escape(title)}</title>
<style>{CSS}</style>
</head>
<body>
<div class="container">
{body_html}
<div class="footer">
<div>Built from Wayback archive exports. Counts use unique events. Venue usage counts venue bookings.</div>
<div class="printtip">Tip: Use your browser Print button to make a PDF for sharing.</div>
</div>
</div>
</body>
</html>
"""


def nav_links(years, current_year=None):
    if current_year is None:
        links = ['<a href="index.html">Summary</a>']
        for y in years:
            links.append(f'<a href="year/{y}.html">{y}</a>')
        return '<div class="nav">' + "\n".join(links) + "</div>"

    links = ['<a href="../index.html">Summary</a>']
    for y in years:
        links.append(f'<a href="{y}.html">{y}</a>')
    return '<div class="nav">' + "\n".join(links) + "</div>"


def load_yearly_summary_fallback(events_unique: pd.DataFrame, venue_bookings: pd.DataFrame) -> pd.DataFrame:
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
    yearly["growth_pct"] = yearly["events"].pct_change().mul(100).round(1)

    venue_year = (
        venue_bookings.groupby("year_int")
        .size()
        .reset_index(name="venue_bookings")
        .sort_values("year_int")
    )
    yearly = yearly.merge(venue_year, on="year_int", how="left").fillna({"venue_bookings": 0})
    yearly["venue_bookings"] = yearly["venue_bookings"].astype(int)
    yearly["notes"] = ""
    return yearly


def load_yearly_summary(events_unique: pd.DataFrame, venue_bookings: pd.DataFrame) -> pd.DataFrame:
    yearly_path = IN_OUTDIR / "yearly_summary.csv"
    if yearly_path.exists():
        y = pd.read_csv(yearly_path)
        if "year_int" not in y.columns:
            if "year" in y.columns:
                y["year_int"] = y["year"]
            else:
                raise ValueError("yearly_summary.csv missing year_int/year")
        y["year_int"] = y["year_int"].astype(int)
        if "venue_bookings" not in y.columns:
            y["venue_bookings"] = 0
        if "notes" not in y.columns:
            y["notes"] = ""
        return y.sort_values("year_int")

    return load_yearly_summary_fallback(events_unique, venue_bookings)


def render_yearly_table(yearly):
    cols = ["year_int", "events", "months", "events_per_month", "unique_programs", "venue_bookings", "growth_pct", "notes"]
    y = yearly.copy()
    for c in cols:
        if c not in y.columns:
            y[c] = ""

    rows = []
    rows.append(
        "<tr>"
        "<th>Year</th>"
        "<th>Total events</th>"
        "<th>Months covered</th>"
        "<th>Events per month</th>"
        "<th>Distinct programs</th>"
        "<th>Venue bookings</th>"
        "<th>Change vs prior year</th>"
        "<th>Notes</th>"
        "</tr>"
    )

    for _, r in y.iterrows():
        year = int(r["year_int"])
        link = f'<a href="year/{year}.html">{year}</a>'
        growth = "" if pd.isna(r.get("growth_pct")) else f"{fmt_float(r.get('growth_pct'), 1)}%"

        rows.append(
            "<tr>"
            f"<td>{link}</td>"
            f"<td>{fmt_int(r.get('events'))}</td>"
            f"<td>{fmt_int(r.get('months'))}</td>"
            f"<td>{fmt_float(r.get('events_per_month'), 1)}</td>"
            f"<td>{fmt_int(r.get('unique_programs'))}</td>"
            f"<td>{fmt_int(r.get('venue_bookings'))}</td>"
            f"<td>{html_escape(growth)}</td>"
            f"<td>{html_escape(clean(r.get('notes')))}</td>"
            "</tr>"
        )

    return '<div class="tablewrap"><table>' + "\n".join(rows) + "</table></div>"


def top_list_from_df(df_in: pd.DataFrame, group_col: str, count_col: str, n=15, label=""):
    g = (
        df_in.groupby(group_col)[count_col]
        .sum()
        .reset_index()
        .sort_values(count_col, ascending=False)
        .head(n)
    )
    items = []
    for _, r in g.iterrows():
        name = clean(r[group_col])
        if not name:
            name = "(unknown)"
        items.append(f"<li><b>{html_escape(name)}</b> ({fmt_int(r[count_col])})</li>")
    title = f"<h2>{html_escape(label)}</h2>" if label else ""
    return title + "<ol>" + "\n".join(items) + "</ol>"


def top_programs(events_unique_year: pd.DataFrame, n=20) -> str:
    g = (
        events_unique_year.groupby("title_canonical")
        .size()
        .reset_index(name="events")
        .sort_values("events", ascending=False)
        .head(n)
    )
    items = []
    for _, r in g.iterrows():
        items.append(f"<li><b>{html_escape(r['title_canonical'])}</b> ({fmt_int(r['events'])})</li>")
    return "<h2>Top programs</h2><ol>" + "\n".join(items) + "</ol>"


def top_days(events_unique_year: pd.DataFrame, n=12) -> str:
    g = (
        events_unique_year.groupby("date")
        .size()
        .reset_index(name="events")
        .sort_values("events", ascending=False)
        .head(n)
    )
    rows = ["<tr><th>Date</th><th>Events that day</th></tr>"]
    for _, r in g.iterrows():
        rows.append(f"<tr><td>{html_escape(r['date'])}</td><td>{fmt_int(r['events'])}</td></tr>")
    return '<div class="tablewrap"><table>' + "\n".join(rows) + "</table></div>"


def month_name(year: int, month: int) -> str:
    return f"{calendar.month_name[month]} {year}"


def render_month_calendar(events_unique_year: pd.DataFrame, venue_bookings_year: pd.DataFrame, year: int, month: int, max_events_per_day: int = 6) -> str:
    """
    Calendar grid (Sunday to Saturday).
    Shows unique events once per day, with merged venues in parentheses.
    """
    cal = calendar.Calendar(firstweekday=6)  # Sunday
    weeks = cal.monthdayscalendar(year, month)

    month_str = f"{year:04d}-{month:02d}"
    ev_m = events_unique_year[events_unique_year["date"].str.startswith(month_str)].copy()
    vb_m = venue_bookings_year[venue_bookings_year["date"].str.startswith(month_str)].copy()

    venues_by_event = (
        vb_m.groupby("event_key")["location_code"]
        .apply(lambda s: sorted({x for x in s if x}))
        .to_dict()
    )

    # Day map: date -> list of (display_text, event_key)
    day_map = {}
    for d, grp in ev_m.groupby("date"):
        items = []
        for _, r in grp.iterrows():
            ek = r["event_key"]
            title = clean(r["title_canonical"])
            tlabel = clean(r.get("time_label", ""))
            vlist = venues_by_event.get(ek, [])

            parts = []
            if tlabel:
                parts.append(tlabel)
            parts.append(title)

            text = " ".join([p for p in parts if p])

            if vlist:
                text += " (" + ", ".join(vlist) + ")"

            items.append(text)

        items = sorted(items)
        day_map[d] = items

    header = "<tr>" + "".join(
        ["<th>Sun</th><th>Mon</th><th>Tue</th><th>Wed</th><th>Thu</th><th>Fri</th><th>Sat</th>"]
    ) + "</tr>"

    rows = [header]

    for week in weeks:
        tds = []
        for day in week:
            if day == 0:
                tds.append('<td class="daycell" aria-label="Empty day"></td>')
                continue

            date_key = f"{year:04d}-{month:02d}-{day:02d}"
            items = day_map.get(date_key, [])

            shown = items[:max_events_per_day]
            extra = len(items) - len(shown)

            lines = [f'<div class="daynum">{day}</div>']
            for it in shown:
                lines.append(f'<div class="eventitem">{html_escape(it)}</div>')
            if extra > 0:
                lines.append(f'<div class="eventitem"><i>+ {extra} more</i></div>')

            tds.append(f'<td class="daycell" aria-label="{html_escape(date_key)}">{"".join(lines)}</td>')

        rows.append("<tr>" + "".join(tds) + "</tr>")

    return '<div class="tablewrap"><table class="calgrid">' + "\n".join(rows) + "</table></div>"


def render_year_calendar_section(events_unique_year: pd.DataFrame, venue_bookings_year: pd.DataFrame, year: int) -> str:
    blocks = []
    blocks.append('<div class="card"><h2>Calendar view by month</h2>')
    blocks.append('<div class="note">Click a month to open the calendar grid. Each day lists unique events. Venues are merged in parentheses.</div>')

    for m in range(1, 13):
        title = month_name(year, m)
        grid = render_month_calendar(events_unique_year, venue_bookings_year, year, m, max_events_per_day=6)
        blocks.append(
            "<details>"
            f"<summary>{html_escape(title)}</summary>"
            f'<div class="monthwrap">{grid}</div>'
            "</details>"
        )

    blocks.append("</div>")
    return "\n".join(blocks)


def build():
    if not IN_EVENTS.exists():
        raise FileNotFoundError(f"Missing {IN_EVENTS}. Run your extraction first.")

    df = pd.read_csv(IN_EVENTS)

    if "date" not in df.columns:
        raise ValueError("events_master_clean.csv must contain a 'date' column in YYYY-MM-DD format")

    df["date"] = df["date"].astype(str).map(clean)
    df["year_int"] = df["date"].str[:4]
    df = df[df["year_int"].str.match(r"^\d{4}$", na=False)].copy()
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

    if "location_code" in df.columns:
        df["location_code"] = df["location_code"].astype(str).map(canonical_loc)
    else:
        df["location_code"] = ""

    df["time_label"] = pick_time_series(df)

    df = df[df["title_canonical"].str.len() > 0].copy()

    df["event_key"] = make_event_key(df)

    events_unique = df.drop_duplicates(subset=["event_key"]).copy()
    venue_bookings = (
        df[df["location_code"].str.len() > 0]
        .drop_duplicates(subset=["event_key", "location_code"])
        .copy()
    )

    yearly = load_yearly_summary(events_unique, venue_bookings)
    years = sorted(yearly["year_int"].astype(int).unique().tolist())

    total_events = len(events_unique)
    date_min = events_unique["date"].min()
    date_max = events_unique["date"].max()

    header = f"""
<div class="header">
  <div class="h1">Bilal Masjid Calendar Summary</div>
  <p class="sub">Archive range: {html_escape(date_min)} to {html_escape(date_max)}. Total unique events: {fmt_int(total_events)}.</p>
  <p class="sub">Totals use unique events. Venue usage counts venue bookings.</p>
</div>
{nav_links(years)}
"""

    story_card = """
<div class="card">
  <h2>How to read this report</h2>
  <div class="small">
    This report is built from historical calendar snapshots.
    An event that appears in multiple venues is counted once as an event, but each venue is counted as a booking.
  </div>
</div>
"""

    summary_card = f"""
<div class="card">
  <h2>Year by year comparison</h2>
  {render_yearly_table(yearly)}
  <div class="note">Click a year to view the detailed page.</div>
</div>
"""

    index_html = page_shell(
        "Bilal Masjid Calendar Summary",
        header + story_card + summary_card
    )
    write(SITE_DIR / "index.html", index_html)

    # Year pages
    for y in years:
        ev_y = events_unique[events_unique["year_int"] == y].copy()
        vb_y = venue_bookings[venue_bookings["year_int"] == y].copy()

        events_count = len(ev_y)
        months = ev_y["year_month"].nunique()
        programs = ev_y["title_canonical"].nunique()
        epm = (events_count / months) if months else 0

        venue_bookings_count = len(vb_y)
        venues_distinct = len(set([v for v in vb_y["location_code"].tolist() if v]))

        yr_row = yearly[yearly["year_int"] == y].head(1)
        growth = ""
        notes = ""
        if len(yr_row) == 1:
            gr = yr_row.iloc[0].get("growth_pct")
            growth = "" if pd.isna(gr) else f"{fmt_float(gr, 1)}%"
            notes = clean(yr_row.iloc[0].get("notes"))

        header_y = f"""
<div class="header">
  <div class="h1">{y} Detailed View</div>
  <p class="sub">Board friendly calendar summary for {y}.</p>
</div>
{nav_links(years, current_year=y)}
"""

        kpis = f"""
<div class="card">
  <h2>Key totals</h2>
  <div class="kpis">
    <div class="kpi"><div class="label">Total events (unique)</div><div class="value">{fmt_int(events_count)}</div></div>
    <div class="kpi"><div class="label">Months covered</div><div class="value">{fmt_int(months)}</div></div>
    <div class="kpi"><div class="label">Events per month</div><div class="value">{fmt_float(epm, 1)}</div></div>
    <div class="kpi"><div class="label">Distinct programs</div><div class="value">{fmt_int(programs)}</div></div>
    <div class="kpi"><div class="label">Venue bookings</div><div class="value">{fmt_int(venue_bookings_count)}</div></div>
    <div class="kpi"><div class="label">Distinct venues used</div><div class="value">{fmt_int(venues_distinct)}</div></div>
    <div class="kpi"><div class="label">Change vs prior year</div><div class="value">{html_escape(growth) if growth else "N/A"}</div></div>
    <div class="kpi"><div class="label">Notes</div><div class="value">{html_escape(notes) if notes else "None"}</div></div>
  </div>
</div>
"""

        # Events by month (unique)
        months_table = (
            ev_y.groupby("year_month")
            .size()
            .reset_index(name="events")
            .sort_values("year_month")
        )
        rows = ["<tr><th>Month</th><th>Events</th></tr>"]
        for _, r in months_table.iterrows():
            rows.append(f"<tr><td>{html_escape(r['year_month'])}</td><td>{fmt_int(r['events'])}</td></tr>")
        month_card = f"""
<div class="card">
  <h2>Events by month</h2>
  <div class="tablewrap"><table>
    {''.join(rows)}
  </table></div>
</div>
"""

        calendar_card = render_year_calendar_section(ev_y, vb_y, y)

        programs_card = f"""
<div class="card">
  {top_programs(ev_y, n=20)}
  <div class="note">Programs are merged using a cleanup rule that removes host text and punctuation.</div>
</div>
"""

        # Venues card based on venue bookings
        if len(vb_y) > 0:
            vb_counts = (
                vb_y.groupby("location_code")
                .size()
                .reset_index(name="venue_bookings")
            )
            venues_card = f"""
<div class="card">
  {top_list_from_df(vb_counts, "location_code", "venue_bookings", n=15, label="Top venues by bookings")}
  <div class="note">Venue bookings count each unique event and venue combination.</div>
</div>
"""
        else:
            venues_card = """
<div class="card">
  <h2>Top venues by bookings</h2>
  <div class="small">No venue data available for this year.</div>
</div>
"""

        days_card = f"""
<div class="card">
  <h2>Busiest days (unique events)</h2>
  {top_days(ev_y, n=12)}
</div>
"""

        year_html = page_shell(
            f"{y} Bilal Masjid Calendar",
            header_y + kpis + month_card + calendar_card + programs_card + venues_card + days_card
        )
        write(YEAR_DIR / f"{y}.html", year_html)

    print("Built site at:", SITE_DIR)
    print("Open this file in your browser:")
    print("  bilal_wayback_html/fundraiser_site/index.html")


if __name__ == "__main__":
    build()