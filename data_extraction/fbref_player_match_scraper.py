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
_cleaned_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
file_directory = _scraped_data_base + "/"
player_match_stats_file_directory = _scraped_data_base + "/player_match_stats/"
CLEANED_ALL_MATCH_STATS_PATH = os.path.join(_cleaned_data_base, "all_match_stats.csv")


OUTFIELD_PLAYER_MATCH_STAT_TYPES = {
        "player": "Player Name",
        "nationality": "Nationality",
        "position": "Position",
        "age": "Age", # age
        "minutes": "Minutes Played",
        "goals": "Goals",
        "assists": "Assists",
        "pens_made": "Penalty Goals Scored",
        "pens_att": "Penalty Kicks Attempted",
        "shots": "Shots",
        "shots_on_target": "Shots on Target",
        "cards_yellow": "Yellow Cards",
        "cards_red": "Red Cards",
        "fouls": "Fouls Committed",
        "fouled": "Fouls Suffered",
        "offsides": "Offsides",
        "crosses": "Crosses",
        "tackles_wom": "Tackles Won",
        "interceptions": "Interceptions",
        "own_goals": "Own Goals",
        "pens_won": "Penalties Won",
        "penalties_conceded": "Penalties Conceded",
        "lineup_status": "Lineup Status",

}

KEEPER_PLAYER_MATCH_STAT_TYPES = {
        "player": "Player Name",
        "nationality": "Nationality",
        "gk_shots_on_target_against": "Shots on Target Faced",
        "gk_goals_against": "Goals Conceded",
        "gk_saves": "Saves",
        "gk_save_pct": "Save Percentage",

}

# Keeper-only columns to join onto outfield (non-keepers get 0)
GK_JOIN_COLUMNS = ["gk_shots_on_target_against", "gk_goals_against", "gk_saves", "gk_save_pct"]

# Player-match scraping: timeouts and retries (reduce rate-limit and transient failures)
PAGE_NAV_TIMEOUT_MS = 90_000
TABLE_VISIBLE_TIMEOUT_MS = 30_000
DELAY_BEFORE_GOTO_SEC = 3   # pause before each navigation to avoid back-to-back requests / ERR_ABORTED
DELAY_AFTER_GOTO_SEC = 10
RETRY_DELAY_SEC = 15        # longer backoff before retry (helps with rate limit / transient failures)
MAX_ATTEMPTS_PER_MATCH = 3
URL_CHUNK_SIZE = 25        # process this many match URLs per browser connection, then disconnect to reset



'''
starting to scrape below
first starting with the league fixture list, we need to extract the details for each match and their URLs
Target DOM: .fb -> #wrap -> #content -> #all_sched -> #switcher_sched -> .table_container -> table.stats_table -> tbody
Extract each row's data-stat cells per MATCH_STAT_TYPES; for match_report use the anchor href.
'''
# Set by __main__ or by caller before using CDP; __main__ starts Chrome and sets these
sb = None
endpoint_url = None


# Player stats table: .fb #wrap #content div[id^="all_player_stats_"] .table_container table.stats_table
OUTFIELD_PLAYER_TABLE_SELECTOR = (
    '.fb #wrap #content div[id^="all_player_stats_"] '
    'div.table_container table.stats_table'
)

# Keeper stats table: .fb #wrap #content div[id^="all_keeper_stats_"] .table_container table.stats_table
KEEPER_PLAYER_TABLE_SELECTOR = (
    '.fb #wrap #content div[id^="all_keeper_stats_"] '
    'div.table_container table.stats_table'
)

# Text stat keys: empty stays empty; numeric stat keys: empty -> 0
OUTFIELD_TEXT_KEYS = {"player", "nationality", "position"}
KEEPER_TEXT_KEYS = {"player", "nationality"}

PLAYER_STATS_TABLE_SUFFIX = "  Player Stats Table"
PLAYER_STATS_TABLE_SUFFIX_ONE_SPACE = " Player Stats Table"
KEEPER_STATS_TABLE_SUFFIX = "  Goalkeeper Stats Table"
KEEPER_STATS_TABLE_SUFFIX_ONE_SPACE = " Goalkeeper Stats Table"
TABLE_NAME_SUFFIXES = (
    PLAYER_STATS_TABLE_SUFFIX,
    PLAYER_STATS_TABLE_SUFFIX_ONE_SPACE,
    KEEPER_STATS_TABLE_SUFFIX,
    KEEPER_STATS_TABLE_SUFFIX_ONE_SPACE,
)


MATCH_LEVEL_DATA_REQUIRED_COLUMNS = ["match_url", "season_id", "date"]


def load_match_level_data_from_cleaned_seed(csv_path):
    """
    Load match-level rows from cleaned all_match_stats.csv for player-level scraping.
    Expects columns: season_id, date, match_report (URL); renames match_report -> match_url.
    Keeps notes if present. Returns DataFrame with match_url, season_id, date [, notes].
    """
    if not os.path.isfile(csv_path):
        return pd.DataFrame(columns=MATCH_LEVEL_DATA_REQUIRED_COLUMNS)
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # all_match_stats.csv uses "match_report" for the match page URL
    if "match_report" in df.columns and "match_url" not in df.columns:
        df = df.rename(columns={"match_report": "match_url"})
    # Ensure required columns exist
    for c in MATCH_LEVEL_DATA_REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = None
    # Keep required columns plus notes for skip logic
    keep = [c for c in MATCH_LEVEL_DATA_REQUIRED_COLUMNS + ["notes"] if c in df.columns]
    return df[keep].copy()


def _match_id_from_url(url):
    """Extract match id from match page URL, e.g. 37bc406c from .../en/matches/37bc406c/..."""
    if not url or "/en/matches/" not in url:
        return None
    parts = url.split("/en/matches/", 1)[1].split("/")
    return parts[0] if parts else None


def _player_id_from_url(player_url):
    """Extract player id from player URL, e.g. ca885102 from .../en/players/ca885102/..."""
    if not player_url or "/en/players/" not in player_url:
        return None
    parts = player_url.split("/en/players/", 1)[1].split("/")
    return parts[0] if parts else None


# Substitutes have "&nbsp;&nbsp;&nbsp;" (or Unicode \u00a0) before the player link in the cell HTML
SUBSTITUTE_INDICATOR_HTML = "&nbsp;&nbsp;&nbsp;"
SUBSTITUTE_INDICATOR_UNICODE = "\u00a0\u00a0\u00a0"


def _lineup_status_from_player_cell(cell):
    """Return 'Sub' if the player cell HTML contains the substitute indicator before the link, else 'Starter'."""
    try:
        html = cell.inner_html() or ""
        if SUBSTITUTE_INDICATOR_HTML in html or SUBSTITUTE_INDICATOR_UNICODE in html:
            return "Sub"
    except Exception:
        pass
    return "Starter"


def _player_name_from_url(player_url):
    """Extract player name from player URL slug; replace '-' with ' '. e.g. .../55975b0c/Valentin-Noel -> Valentin Noel."""
    if not player_url or "/en/players/" not in player_url:
        return None
    parts = player_url.split("/en/players/", 1)[1].split("/")
    if len(parts) < 2:
        return None
    return parts[1].replace("-", " ").strip() or None


def _normalize_nationality(s):
    """Keep only the 2–3 letter code (e.g. 'us USA' -> 'USA', 'mx MEX' -> 'MEX')."""
    if not s or not (s := (s or "").strip()):
        return None
    parts = s.split()
    for p in reversed(parts):
        if len(p) >= 2 and p.isupper():
            return p
    return parts[-1] if parts else None


def _team_name_from_table(table_locator, suffixes=TABLE_NAME_SUFFIXES):
    """Extract team name from one layer up at table level (caption or thead); trim known suffixes."""
    cap = table_locator.locator("caption").first
    if cap.count() > 0:
        name = (cap.inner_text() or "").strip()
    else:
        th = table_locator.locator("thead th").first
        if th.count() > 0:
            name = (th.inner_text() or "").strip()
        else:
            return None
    for suffix in suffixes:
        if name and name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    return name or None


def _scrape_player_match_data_for_urls(page, match_urls):
    """Scrape outfield + keeper rows for match_urls using an already-open page. Returns (outfield_rows, keeper_rows)."""
    outfield_rows = []
    keeper_rows = []
    for match_url in match_urls:
        last_error = None
        for attempt in range(1, MAX_ATTEMPTS_PER_MATCH + 1):
            try:
                sb.sleep(DELAY_BEFORE_GOTO_SEC)
                page.goto(match_url, wait_until="domcontentloaded", timeout=PAGE_NAV_TIMEOUT_MS)
                sb.sleep(DELAY_AFTER_GOTO_SEC)
                match_id = _match_id_from_url(match_url)

                # --- Outfield tables ---
                page.locator(OUTFIELD_PLAYER_TABLE_SELECTOR).first.wait_for(state="visible", timeout=TABLE_VISIBLE_TIMEOUT_MS)
                tables = page.locator(OUTFIELD_PLAYER_TABLE_SELECTOR).all()
                for table in tables:
                    team_name = _team_name_from_table(table)
                    tbody = table.locator("tbody")
                    rows = tbody.locator("tr").all()
                    for row in rows:
                        if "thead" in (row.get_attribute("class") or ""):
                            continue
                        row_data = {"team_name": team_name, "player_url": None, "player_id": None, "match_id": match_id}
                        for stat_key in OUTFIELD_PLAYER_MATCH_STAT_TYPES:
                            if stat_key == "lineup_status":
                                continue  # set from player cell below
                            cell = row.locator(f"[data-stat='{stat_key}']").first
                            if cell.count() == 0:
                                row_data[stat_key] = "0" if stat_key not in OUTFIELD_TEXT_KEYS else None
                                continue
                            if stat_key == "player":
                                link = cell.locator("a").first
                                if link.count() > 0:
                                    href = link.get_attribute("href")
                                    row_data["player_url"] = (FBREF_BASE_URL + href) if href else None
                                    row_data["player_id"] = _player_id_from_url(row_data["player_url"])
                                    row_data[stat_key] = _player_name_from_url(row_data["player_url"])
                                else:
                                    row_data["player_url"] = None
                                    row_data["player_id"] = None
                                    row_data[stat_key] = (cell.inner_text() or "").strip() or None
                                row_data["lineup_status"] = _lineup_status_from_player_cell(cell)
                            elif stat_key == "nationality":
                                raw = (cell.inner_text() or "").strip()
                                row_data[stat_key] = _normalize_nationality(raw)
                            else:
                                raw = (cell.inner_text() or "").strip()
                                if raw:
                                    row_data[stat_key] = raw
                                else:
                                    row_data[stat_key] = "0" if stat_key not in OUTFIELD_TEXT_KEYS else None
                        if "lineup_status" not in row_data:
                            row_data["lineup_status"] = "Starter"
                        pos = row_data.get("position")
                        if pos and "," in pos:
                            row_data["primary_position"] = pos.split(",", 1)[0].strip()
                        else:
                            row_data["primary_position"] = pos
                        outfield_rows.append(row_data)

                # --- Keeper tables (same page) ---
                page.locator(KEEPER_PLAYER_TABLE_SELECTOR).first.wait_for(state="visible", timeout=TABLE_VISIBLE_TIMEOUT_MS)
                tables = page.locator(KEEPER_PLAYER_TABLE_SELECTOR).all()
                for table in tables:
                    team_name = _team_name_from_table(table)
                    tbody = table.locator("tbody")
                    rows = tbody.locator("tr").all()
                    for row in rows:
                        if "thead" in (row.get_attribute("class") or ""):
                            continue
                        row_data = {"team_name": team_name, "player_url": None, "player_id": None, "match_id": match_id}
                        for stat_key in KEEPER_PLAYER_MATCH_STAT_TYPES:
                            if stat_key == "lineup_status":
                                continue  # set from player cell below
                            cell = row.locator(f"[data-stat='{stat_key}']").first
                            if cell.count() == 0:
                                row_data[stat_key] = "0" if stat_key not in KEEPER_TEXT_KEYS else None
                                continue
                            if stat_key == "player":
                                link = cell.locator("a").first
                                if link.count() > 0:
                                    href = link.get_attribute("href")
                                    row_data["player_url"] = (FBREF_BASE_URL + href) if href else None
                                    row_data["player_id"] = _player_id_from_url(row_data["player_url"])
                                    row_data[stat_key] = _player_name_from_url(row_data["player_url"])
                                else:
                                    row_data["player_url"] = None
                                    row_data["player_id"] = None
                                    row_data[stat_key] = (cell.inner_text() or "").strip() or None
                                row_data["lineup_status"] = _lineup_status_from_player_cell(cell)
                            elif stat_key == "nationality":
                                raw = (cell.inner_text() or "").strip()
                                row_data[stat_key] = _normalize_nationality(raw)
                            else:
                                raw = (cell.inner_text() or "").strip()
                                if raw:
                                    row_data[stat_key] = raw
                                else:
                                    row_data[stat_key] = "0" if stat_key not in KEEPER_TEXT_KEYS else None
                        if "lineup_status" not in row_data:
                            row_data["lineup_status"] = "Starter"
                        keeper_rows.append(row_data)
                last_error = None
                break
            except Exception as e:
                last_error = e
                if attempt < MAX_ATTEMPTS_PER_MATCH:
                    # Close page and open a new one (same connection) before retry, like we do after each chunk
                    try:
                        context = page.context
                        page.close()
                        page = context.new_page()
                    except Exception:
                        pass
                    sb.sleep(RETRY_DELAY_SEC)
                else:
                    print(f"Skipping match (no player stats or error): {match_url} — {e}")
    return outfield_rows, keeper_rows


def get_player_match_data(match_urls, date, season_id, page=None):
    """
    Load match page(s) and scrape both outfield and keeper player stats.
    Accepts match_urls (str or list), date, and season_id. Writes one CSV:
    player_match_data_{season_id}_{date}.csv (outfield + keeper joined).
    If page is provided, uses that page (one connection for many batches). Otherwise connects once per call.
    Returns the combined DataFrame.
    """
    if isinstance(match_urls, str):
        match_urls = [match_urls]
    if page is not None:
        outfield_rows, keeper_rows = _scrape_player_match_data_for_urls(page, match_urls)
    else:
        current_endpoint = sb.get_endpoint_url()
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(current_endpoint)
            context = browser.contexts[0]
            page = context.pages[0]
            outfield_rows, keeper_rows = _scrape_player_match_data_for_urls(page, match_urls)

    merged = merge_keeper_into_outfield(outfield_rows, keeper_rows)
    safe_season = str(season_id).replace("/", "_").replace(" ", "_")
    safe_date = str(date).replace("/", "-").replace(" ", "_").replace(":", "-")
    csv_path = player_match_stats_file_directory + f"player_match_data_{safe_season}_{safe_date}.csv"
    merged.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d", quoting=csv.QUOTE_NONNUMERIC)
    print(f"Exported {len(merged)} rows to {csv_path}")
    return merged


def merge_keeper_into_outfield(outfield_rows, keeper_rows):
    """
    Left-join keeper stats into outfield on player_id and match_id.
    Adds gk_shots_on_target_against, gk_goals_against, gk_saves, gk_save_pct.
    Outfield-only players get 0 for those columns. Returns a DataFrame.
    """
    df_out = pd.DataFrame(outfield_rows)
    df_kee = pd.DataFrame(keeper_rows)
    join_keys = ["player_id", "match_id"]
    gk_cols = [c for c in GK_JOIN_COLUMNS if c in df_kee.columns]
    if not gk_cols:
        df_out[GK_JOIN_COLUMNS] = 0
        return df_out
    df_merged = df_out.merge(
        df_kee[join_keys + gk_cols],
        on=join_keys,
        how="left",
    )
    for c in GK_JOIN_COLUMNS:
        if c not in df_merged.columns:
            df_merged[c] = 0
        else:
            df_merged[c] = pd.to_numeric(df_merged[c], errors="coerce").fillna(0)
    return df_merged


def get_outfield_player_match_data(match_url, date, season_id):
    """Return combined player stats (outfield + keeper) for one match; also writes CSV via get_player_match_data."""
    return get_player_match_data(match_url, date, season_id)


def get_keeper_player_match_data(match_url, date, season_id):
    """Return keeper player stats for one match (loads page once; use get_player_match_data if you need both)."""
    return get_player_match_data(match_url, date, season_id)


# Skip player-level scraping when match-level notes indicate cancelled/awarded (no player stats).
NOTES_SKIP_PHRASES = ("Match Cancelled", "Match Canceled", "Match awarded")

def _notes_skip_player_scrape(notes_val):
    if notes_val is None or (isinstance(notes_val, float) and pd.isna(notes_val)):
        return False
    s = str(notes_val).strip()
    return any(phrase.lower() in s.lower() for phrase in NOTES_SKIP_PHRASES)

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


def scrape_player_match_data_for_season_date_groups(match_level_data, endpoint_url):
    """
    For each (season_id, date) in match_level_data, scrape player stats and write
    player_match_data_{season_id}_{date}.csv. Uses endpoint_url for Playwright CDP.
    """
    for (season_id, date), group in match_level_data.groupby(["season_id", "date"]):
        if "notes" in group.columns:
            skip_mask = group["notes"].apply(_notes_skip_player_scrape)
            match_urls = group.loc[~skip_mask, "match_url"].dropna().tolist()
        else:
            match_urls = group["match_url"].dropna().tolist()
        if not match_urls:
            continue
        all_outfield = []
        all_keeper = []
        for chunk_start in range(0, len(match_urls), URL_CHUNK_SIZE):
            chunk = match_urls[chunk_start : chunk_start + URL_CHUNK_SIZE]
            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(endpoint_url)
                context = browser.contexts[0]
                page = context.pages[0]
                of, kp = _scrape_player_match_data_for_urls(page, chunk)
                all_outfield.extend(of)
                all_keeper.extend(kp)
        merged = merge_keeper_into_outfield(all_outfield, all_keeper)
        safe_season = str(season_id).replace("/", "_").replace(" ", "_")
        safe_date = str(date).replace("/", "-").replace(" ", "_").replace(":", "-")
        csv_path = player_match_stats_file_directory + f"player_match_data_{safe_season}_{safe_date}.csv"
        merged.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d", quoting=csv.QUOTE_NONNUMERIC)
        print(f"Exported {len(merged)} rows to {csv_path}")


if __name__ == "__main__":
    sb = sb_cdp.Chrome()
    endpoint_url = sb.get_endpoint_url()

    match_level_data = load_match_level_data_from_cleaned_seed(CLEANED_ALL_MATCH_STATS_PATH)
    print(f"Loaded {len(match_level_data)} match level data row(s) from {CLEANED_ALL_MATCH_STATS_PATH}")

    scrape_player_match_data_for_season_date_groups(match_level_data, endpoint_url)

    export_file_directory = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
    player_match_stats_export_name = "all_player_match_stats.csv"
    player_match_stats_files = glob.glob(os.path.join(player_match_stats_file_directory, "*.csv"))
    csv_appender(player_match_stats_file_directory, export_file_directory, player_match_stats_export_name)
    print(f"Exported {len(player_match_stats_files)} row(s) to {player_match_stats_export_name}")