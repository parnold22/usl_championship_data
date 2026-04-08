'''
Starting with setting all of the competitions and seasons we want to scrape
Adding some default variables like base URLs
'''

import pandas as pd

FBREF_BASE_URL = "https://fbref.com"
_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
file_directory = _scraped_data_base + "/"
player_match_stats_file_directory = _scraped_data_base + "/player_match_stats/"
match_stats_file_directory = _scraped_data_base + "/match_stats/"
season_dims_file_directory = _scraped_data_base + "/season_dims/"

season_year = [
        "2026",  # 2026 Season
        #"2025",  # 2025 Season
        #"2024",  # 2024 Season
        #"2023",  # 2023 Season
        #"2022",  # 2022 Season
        #"2021",  # 2021 Season
        #"2020",  # 2020 Season
]
leagues = {
        "73": "USL Championship",  # USL Championship
}

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
        "tackles_won": "Tackles Won",
        "interceptions": "Interceptions",
        "own_goals": "Own Goals",
        "pens_won": "Penalties Won",
        "penalties_conceded": "Penalties Conceded",

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

# Schedule scraping (get_match_data)
SCHED_PAGE_TIMEOUT_MS = 60_000
SCHED_TABLE_VISIBLE_TIMEOUT_MS = 30_000
SCHED_RETRY_DELAY_SEC = 10
SCHED_MAX_ATTEMPTS = 2


# DataFrame: league_id, league_name, season_id, season_name, url
rows = []
for season_year_id in season_year:
    for league_id, league_name in leagues.items():
        url = f"https://fbref.com/en/comps/{league_id}/{season_year_id}/schedule/"
        season_name = f"{season_year_id} Season"
        rows.append({
            "season_id": f"{league_id}_{season_year_id}",
            "league_id": league_id,
            "league_name": league_name,
            "season_year_id": season_year_id,
            "season_name": season_name,
            "url": url,
        })
        print(f"Scraping {league_name} - {season_year_id}: {url}")

league_fixture_urls = pd.DataFrame(rows)
print("Number of schedule URLs to scrape:", len(league_fixture_urls))

for season_id in league_fixture_urls["season_id"].unique():
    subset = league_fixture_urls[league_fixture_urls["season_id"] == season_id]
    safe_id = str(season_id).replace("/", "_").replace(" ", "_")
    csv_path = season_dims_file_directory + f"season_data_{safe_id}.csv"
    subset.to_csv(csv_path, index=False)
    print(f"Exported {len(subset)} row(s) to {csv_path}")

'''
starting to scrape below
first starting with the league fixture list, we need to extract the details for each match and their URLs
Target DOM: .fb -> #wrap -> #content -> #all_sched -> #switcher_sched -> .table_container -> table.stats_table -> tbody
Extract each row's data-stat cells per MATCH_STAT_TYPES; for match_report use the anchor href.
'''

from playwright.sync_api import sync_playwright
from seleniumbase import sb_cdp

sb = sb_cdp.Chrome()
endpoint_url = sb.get_endpoint_url()


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

# CSS path to schedule table tbody (table_container and stats_table may have extra classes)
SCHED_TBODY_SELECTOR = (
    ".fb #wrap #content #all_sched #switcher_sched "
    "div.table_container table.stats_table tbody"
)

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
                    page.goto(url, wait_until="domcontentloaded", timeout=SCHED_PAGE_TIMEOUT_MS)
                    sb.sleep(10)
                    page.locator(SCHED_TBODY_SELECTOR).first.wait_for(state="visible", timeout=SCHED_TABLE_VISIBLE_TIMEOUT_MS)
                    loaded = True
                except Exception as e:
                    if attempt < SCHED_MAX_ATTEMPTS:
                        sb.sleep(SCHED_RETRY_DELAY_SEC)
                    else:
                        print(f"Skipping schedule URL (timeout or error after {SCHED_MAX_ATTEMPTS} attempts): {url} — {e}")
                    # exit with block → connection closed; next attempt reopens browser
                if loaded:
                    all_tbodies = page.locator(SCHED_TBODY_SELECTOR).all()
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


all_matches = get_match_data(league_fixture_urls)


#Export match data to CSV


matches_df = pd.DataFrame(all_matches)

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
    subset.to_csv(csv_path_2, index=False)
    print(f"Exported {len(subset)} rows to {csv_path_2}")



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
                            elif stat_key == "nationality":
                                raw = (cell.inner_text() or "").strip()
                                row_data[stat_key] = _normalize_nationality(raw)
                            else:
                                raw = (cell.inner_text() or "").strip()
                                if raw:
                                    row_data[stat_key] = raw
                                else:
                                    row_data[stat_key] = "0" if stat_key not in OUTFIELD_TEXT_KEYS else None
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
                            elif stat_key == "nationality":
                                raw = (cell.inner_text() or "").strip()
                                row_data[stat_key] = _normalize_nationality(raw)
                            else:
                                raw = (cell.inner_text() or "").strip()
                                if raw:
                                    row_data[stat_key] = raw
                                else:
                                    row_data[stat_key] = "0" if stat_key not in KEEPER_TEXT_KEYS else None
                        keeper_rows.append(row_data)
                last_error = None
                break
            except Exception as e:
                last_error = e
                if attempt < MAX_ATTEMPTS_PER_MATCH:
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
    merged.to_csv(csv_path, index=False, encoding="utf-8")
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

# Scrape player-level match data per (season_id, date); one CSV per (season_id, date).
# Process match URLs in chunks of URL_CHUNK_SIZE; open a fresh browser connection per chunk and disconnect after each chunk so the browser resets between batches.
current_endpoint = sb.get_endpoint_url()
for (season_id, date), group in matches_df.groupby(["season_id", "date"]):
    if "notes" in group.columns:
        skip_mask = group["notes"].apply(_notes_skip_player_scrape)
        match_urls = group.loc[~skip_mask, "match_report"].dropna().tolist()
    else:
        match_urls = group["match_report"].dropna().tolist()
    if not match_urls:
        continue
    all_outfield = []
    all_keeper = []
    for chunk_start in range(0, len(match_urls), URL_CHUNK_SIZE):
        chunk = match_urls[chunk_start : chunk_start + URL_CHUNK_SIZE]
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(current_endpoint)
            context = browser.contexts[0]
            page = context.pages[0]
            of, kp = _scrape_player_match_data_for_urls(page, chunk)
            all_outfield.extend(of)
            all_keeper.extend(kp)
        # Connection closes here; next chunk will open a fresh connection
    merged = merge_keeper_into_outfield(all_outfield, all_keeper)
    safe_season = str(season_id).replace("/", "_").replace(" ", "_")
    safe_date = str(date).replace("/", "-").replace(" ", "_").replace(":", "-")
    csv_path = player_match_stats_file_directory + f"player_match_data_{safe_season}_{safe_date}.csv"
    merged.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Exported {len(merged)} rows to {csv_path}")