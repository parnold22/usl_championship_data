'''
Starting with setting all of the competitions and seasons we want to scrape
Adding some default variables like base URLs
'''

import csv
import glob
import os
import pandas as pd
from playwright.sync_api import sync_playwright
from seleniumbase import sb_cdp


FBREF_BASE_URL = "https://fbref.com"
_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
file_directory = _scraped_data_base + "/"
match_stats_file_directory = _scraped_data_base + "/match_stats/"
season_dims_file_directory = _scraped_data_base + "/season_dims/"


MATCH_STAT_TYPES = {
        "season_id": "Season ID", # season id
        "round": "Round", # round
        "date": "Date", # date
        "start_time": "Kickoff Time", # start time
        "home_team": "Home Team", # home team
        "away_team": "Away Team", # away team
        "score": "Score", # score
        "attendance": "Attendance", # attendance
        "referee": "Referee", # referee
        "venue": "Venue", # venue
        "match_report": "Match URL",  # match report
        "notes": "Notes",  # notes (e.g. Match Cancelled, Match awarded)
}


'''
starting to scrape below
first starting with the league fixture list, we need to extract the details for each match and their URLs
Target DOM: .fb -> #wrap -> #content -> #all_sched -> #switcher_sched -> .table_container -> table.stats_table -> tbody
Extract each row's data-stat cells per MATCH_STAT_TYPES; for match_report use the anchor href.
'''
# Set by __main__ or by caller (e.g. fbref_in_season_updater) before get_match_data()
sb = None
endpoint_url = None


def _normalize_score(s):
    """Replace en-dash, em-dash, and common mojibake with ASCII hyphen so scores are e.g. 0-2."""
    if not s:
        return s
    s = s.replace("\u2013", "-")   # en-dash
    s = s.replace("\u2014", "-")   # em-dash
    s = s.replace("‚Äì", "-")     # en-dash mojibake (UTF-8 read as Latin-1)
    return s.strip() or None


def _match_id_from_report(match_report_url):
    """Extract unique match id from match_report URL, e.g. 2382ecc0 from .../en/matches/2382ecc0/..."""
    if not match_report_url or "/en/matches/" not in match_report_url:
        return None
    parts = match_report_url.split("/en/matches/", 1)[1].split("/")
    return parts[0] if parts else None


# Set True to print _is_match_date_in_past input/output for debugging date filtering
DEBUG_DATE_FILTER = False


def _is_match_date_in_past(date_val):
    """Return True if date_val is a date that has already passed (strictly before today). Used to skip future matches."""
    today = pd.Timestamp.now().date()
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        if DEBUG_DATE_FILTER:
            print(f"[_is_match_date_in_past] date_val={repr(date_val)} -> False (empty/null)")
        return False
    try:
        dt = pd.to_datetime(date_val, errors="coerce")
        if pd.isna(dt):
            if DEBUG_DATE_FILTER:
                print(f"[_is_match_date_in_past] date_val={repr(date_val)} -> False (parse returned NaT)")
            return False
        in_past = dt.date() < today
        if DEBUG_DATE_FILTER:
            print(f"[_is_match_date_in_past] date_val={repr(date_val)} -> parsed={dt.date()}, today={today} -> {in_past}")
        return in_past
    except Exception as e:
        if DEBUG_DATE_FILTER:
            print(f"[_is_match_date_in_past] date_val={repr(date_val)} -> False (exception: {e})")
        return False

# CSS path to schedule table tbody (table_container and stats_table may have extra classes).
# Try in order; some pages use #switcher_sched, others do not.
SCHED_TBODY_SELECTOR = (
    ".fb #wrap #content #all_sched #switcher_sched "
    "div.table_container table.stats_table tbody"
)
SCHED_TBODY_SELECTOR_2 = (
    ".fb #wrap #content #all_sched "
    "div.table_container table.stats_table tbody"
)
SCHED_TBODY_SELECTORS = (SCHED_TBODY_SELECTOR, SCHED_TBODY_SELECTOR_2)

# Schedule scraping timeouts and retries
SCHED_PAGE_TIMEOUT_MS = 60_000
SCHED_TABLE_VISIBLE_TIMEOUT_MS = 30_000
SCHED_DELAY_BEFORE_GOTO_SEC = 8   # pause before each schedule URL to reduce ERR_BLOCKED_BY_RESPONSE
SCHED_RETRY_DELAY_SEC = 10
SCHED_BLOCKED_RETRY_DELAY_SEC = 45   # longer backoff when server blocks (ERR_BLOCKED_BY_RESPONSE)
SCHED_MAX_ATTEMPTS = 2

# Columns required by get_match_data()
SCHEDULE_REQUIRED_COLUMNS = ["league_id", "league_name", "season_id", "season_name", "url"]


def load_season_dims_schedule(season_dims_dir):
    """
    Load and append all CSV files in scraped_data/season_dims into one DataFrame.
    Returns a DataFrame with columns required by get_match_data(): league_id, league_name, season_id, season_name, url.
    """
    pattern = os.path.join(season_dims_dir, "*.csv")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame(columns=SCHEDULE_REQUIRED_COLUMNS)
    dfs = []
    for path in sorted(files):
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            # Keep only columns that exist and are required; drop extras so concat is consistent
            cols = [c for c in SCHEDULE_REQUIRED_COLUMNS if c in df.columns]
            if cols:
                dfs.append(df[cols])
        except Exception as e:
            print(f"Warning: skipped {path}: {e}")
    if not dfs:
        return pd.DataFrame(columns=SCHEDULE_REQUIRED_COLUMNS)
    combined = pd.concat(dfs, ignore_index=True)
    # Ensure all required columns present (fill missing with None)
    for c in SCHEDULE_REQUIRED_COLUMNS:
        if c not in combined.columns:
            combined[c] = None
    return combined[SCHEDULE_REQUIRED_COLUMNS]


def get_match_data(schedule_df):
    """
    Accept a DataFrame like league_fixture_urls (columns: league_id, league_name, season_id, season_name, url).
    Return a list of match dicts; each row includes season_id (and league_id, league_name, season_name) from the schedule row.
    """
    required = ["league_id", "league_name", "season_id", "season_name", "url"]
    if not isinstance(schedule_df, pd.DataFrame) or not all(c in schedule_df.columns for c in required):
        raise ValueError("schedule_df must be a DataFrame with columns: league_id, league_name, season_id, season_name, url")

    collected = []
    for _, sched in schedule_df.iterrows():
        url = sched["url"]
        loaded = False
        for attempt in range(1, SCHED_MAX_ATTEMPTS + 1):
            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(endpoint_url)
                context = browser.contexts[0]
                page = context.pages[0]
                try:
                    sb.sleep(SCHED_DELAY_BEFORE_GOTO_SEC)
                    page.goto(url, wait_until="domcontentloaded", timeout=SCHED_PAGE_TIMEOUT_MS)
                    sb.sleep(10)
                    sched_selector = None
                    for sel in SCHED_TBODY_SELECTORS:
                        try:
                            page.locator(sel).first.wait_for(state="visible", timeout=SCHED_TABLE_VISIBLE_TIMEOUT_MS)
                            sched_selector = sel
                            break
                        except Exception:
                            continue
                    if sched_selector is not None:
                        loaded = True
                    elif attempt == SCHED_MAX_ATTEMPTS:
                        print(f"Schedule table not found with any selector for: {url}")
                except Exception as e:
                    if attempt < SCHED_MAX_ATTEMPTS:
                        delay = SCHED_BLOCKED_RETRY_DELAY_SEC if "ERR_BLOCKED_BY_RESPONSE" in str(e) else SCHED_RETRY_DELAY_SEC
                        if "ERR_BLOCKED_BY_RESPONSE" in str(e):
                            print(f"Blocked by server, retrying in {delay}s: {url}")
                        sb.sleep(delay)
                    else:
                        print(f"Skipping schedule URL (timeout or error after {SCHED_MAX_ATTEMPTS} attempts): {url} — {e}")
                    # exit with block → connection closed; next attempt reopens browser
                if loaded:
                    all_tbodies = page.locator(sched_selector).all()
                    seen_data_rows = set()
                    for tbody in all_tbodies:
                        rows = tbody.locator("tr").all()
                        for row in rows:
                            if "thead" in (row.get_attribute("class") or ""):
                                continue
                            data_row = row.get_attribute("data-row")
                            if data_row is not None and data_row in seen_data_rows:
                                continue
                            row_data = {}
                            for stat_key in MATCH_STAT_TYPES:
                                cell = row.locator(f"[data-stat='{stat_key}']").first
                                if cell.count() == 0:
                                    row_data[stat_key] = None
                                    continue
                                if stat_key == "match_report":
                                    link = cell.locator("a").first
                                    href = link.get_attribute("href") if link.count() > 0 else None
                                    row_data[stat_key] = (FBREF_BASE_URL + href) if href else None
                                elif stat_key == "round":
                                    link = cell.locator("a").first
                                    if link.count() > 0:
                                        row_data[stat_key] = (link.inner_text() or "").strip() or None
                                    else:
                                        row_data[stat_key] = (cell.inner_text() or "").strip() or None
                                elif stat_key == "score":
                                    raw = (cell.inner_text() or "").strip()
                                    row_data[stat_key] = _normalize_score(raw) or None
                                elif stat_key == "start_time":
                                    venuetime_el = cell.locator(".venuetime").first
                                    if venuetime_el.count() > 0:
                                        row_data[stat_key] = (venuetime_el.inner_text() or "").strip() or None
                                    else:
                                        row_data[stat_key] = None
                                else:
                                    row_data[stat_key] = (cell.inner_text() or "").strip() or None
                            score_val = row_data.get("score")
                            if score_val and "-" in score_val:
                                parts = score_val.split("-", 1)
                                row_data["home_score"] = parts[0].strip() or None
                                row_data["away_score"] = parts[1].strip() if len(parts) > 1 else None
                            else:
                                row_data["home_score"] = None
                                row_data["away_score"] = None
                            if row_data.get("match_report") or any(v for v in row_data.values() if v):
                                if not _is_match_date_in_past(row_data.get("date")):
                                    continue
                                if data_row is not None:
                                    seen_data_rows.add(data_row)
                                row_data["match_id"] = _match_id_from_report(row_data.get("match_report"))
                                row_data["season_id"] = sched["season_id"]
                                collected.append(row_data)
                    print(f"After {url}: {len(collected)} match(es) total")
                    break
            if loaded:
                break
            if attempt < SCHED_MAX_ATTEMPTS:
                sb.sleep(SCHED_RETRY_DELAY_SEC)
        if not loaded:
            continue
    return collected


def _export_matches_df_to_season_csvs(matches_df):
    """Write matches_df to match_level_data_{safe_id}.csv per season_id. Applies score column formatting."""
    # Force ID columns to string so they are quoted in CSV and never written as scientific notation
    for col in ["match_id", "season_id"]:
        if col in matches_df.columns:
            matches_df[col] = matches_df[col].astype(str).replace("nan", "").replace("<NA>", "")
    # Keep score columns as strings; prefix with apostrophe so Excel/Sheets don't parse as dates
    _score_cols = ["score"]
    for col in _score_cols:
        if col in matches_df.columns:
            matches_df[col] = matches_df[col].fillna("").astype(str).replace("nan", "")
            matches_df[col] = matches_df[col].apply(lambda x: "'" + x if x else "")
    for season_id in matches_df["season_id"].unique():
        subset = matches_df[matches_df["season_id"] == season_id]
        safe_id = str(season_id).replace("/", "_").replace(" ", "_")
        csv_path_2 = match_stats_file_directory + f"match_level_data_{safe_id}.csv"
        subset.to_csv(csv_path_2, index=False, date_format="%Y-%m-%d", quoting=csv.QUOTE_NONNUMERIC)
        print(f"Exported {len(subset)} rows to {csv_path_2}")


def csv_appender(file_directory, export_directory, export_name):
    all_files = glob.glob(os.path.join(file_directory, "*.csv"))
    new_df = []
    for file in all_files:
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
        new_df.append(df)
    final_df = pd.concat(new_df, ignore_index=True)
    final_df.to_csv(
        os.path.join(export_directory, export_name),
        index=False,
        date_format="%Y-%m-%d",
        quoting=csv.QUOTE_NONNUMERIC,
    )


if __name__ == "__main__":
    sb = sb_cdp.Chrome()
    endpoint_url = sb.get_endpoint_url()

    cleaned_export_file_directory = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
    match_stats_export_name = "all_match_stats.csv"

    league_fixture_urls = load_season_dims_schedule(season_dims_file_directory)
    print(f"Loaded {len(league_fixture_urls)} schedule row(s) from {season_dims_file_directory}")

    all_matches = get_match_data(league_fixture_urls)
    matches_df = pd.DataFrame(all_matches)
    _export_matches_df_to_season_csvs(matches_df)

    match_stats_files = glob.glob(os.path.join(match_stats_file_directory, "*.csv"))
    csv_appender(match_stats_file_directory, cleaned_export_file_directory, match_stats_export_name)
    print(f"Exported {len(match_stats_files)} row(s) to {match_stats_export_name}")