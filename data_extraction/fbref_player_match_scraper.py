'''
Starting with setting all of the competitions and seasons we want to scrape
Adding some default variables like base URLs
'''

import argparse
import csv
import glob
import os
import time
import pandas as pd
from playwright.sync_api import sync_playwright
from seleniumbase import sb_cdp



FBREF_BASE_URL = "https://fbref.com"
_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
_cleaned_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
file_directory = _scraped_data_base + "/"
player_match_stats_file_directory = _scraped_data_base + "/player_match_stats/"
CLEANED_ALL_MATCH_STATS_PATH = os.path.join(_cleaned_data_base, "all_match_stats.csv")

# ---------------------------------------------------------------------------
# Run filter — when executing this file as __main__, limit scraping to one season.
# Set to None to use every season in the seed CSV. Example: "73_2026"
# Command-line --season-id overrides this when you pass it.
# ---------------------------------------------------------------------------
RUN_FILTER_SEASON_ID = "73_2018"  # None for all seasons

# ---------------------------------------------------------------------------
# Start date — only scrape matches on or after this calendar date (within rows
# left after the season filter). None = from the first match date in that set.
# Use YYYY-MM-DD (e.g. "2026-03-15"). Command-line --start-date overrides when passed.
# ---------------------------------------------------------------------------
RUN_FILTER_START_DATE = "2018-10-07" #None for all dates


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
TABLE_VISIBLE_TIMEOUT_MS = PAGE_NAV_TIMEOUT_MS  # align with nav; 30s caused long stalls on slow FBref tables
CDP_CONNECT_TIMEOUT_MS = 180_000  # websocket discovery can exceed 120s after heavy scraping / full_break
CDP_CONNECT_RETRIES = 6
CDP_RETRY_DELAY_SEC = 15  # base delay; doubled each failed attempt up to CDP_RETRY_BACKOFF_MAX_SEC
CDP_RETRY_BACKOFF_MAX_SEC = 120
CDP_POST_DISCONNECT_SETTLE_SEC = 3  # after Playwright browser.close(), let Chrome CDP settle before long pause/reconnect
FULL_BREAK_EVERY_N_MATCHES = 6  # close Playwright CDP, pause, reconnect (lets Chrome / rate limits recover)
FULL_BREAK_SEC = 180
DELAY_BEFORE_GOTO_SEC = 3   # pause before each navigation to avoid back-to-back requests / ERR_ABORTED
DELAY_AFTER_GOTO_SEC = 10
RETRY_DELAY_SEC = 15        # base for exponential backoff before retry (rate limits / transient failures)
RETRY_BACKOFF_MAX_SEC = 120  # cap: min( this, RETRY_DELAY_SEC * 2**(attempt-1) )
PAUSE_AFTER_SKIP_SEC = 60   # cool down after all attempts fail (helps avoid cascading skips)
DELAY_BETWEEN_MATCHES_SEC = 6  # after a successful match scrape, before starting the next URL
PAUSE_BETWEEN_DATE_GROUPS_SEC = 45  # between each (season_id, date) output file
PAUSE_BETWEEN_CHUNKS_SEC = 20  # between URL_CHUNK_SIZE batches within the same date (many matches/day)
PAUSE_BEFORE_DEFERRED_RETRY_SEC = 30  # pause before second pass (URLs that failed first pass)
MAX_ATTEMPTS_PER_MATCH = 3
URL_CHUNK_SIZE = 25        # batch size for logging; each chunk opens its own Playwright CDP session (full breaks also reconnect inside a chunk)



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


def _configure_page_timeouts(page):
    """Align Playwright defaults with PAGE_NAV_TIMEOUT_MS (goto timeout alone does not cap locators)."""
    page.set_default_navigation_timeout(PAGE_NAV_TIMEOUT_MS)
    page.set_default_timeout(PAGE_NAV_TIMEOUT_MS)
    ctx = page.context
    ctx.set_default_navigation_timeout(PAGE_NAV_TIMEOUT_MS)
    ctx.set_default_timeout(PAGE_NAV_TIMEOUT_MS)


def _scrape_player_match_data_for_urls(endpoint_url, match_urls):
    """Scrape outfield + keeper rows for match_urls via Chrome CDP.

    Opens a Playwright CDP session and closes it when done. Every
    FULL_BREAK_EVERY_N_MATCHES URLs (and before the deferred retry pass), closes the
    Playwright browser connection, sleeps FULL_BREAK_SEC, reads a fresh CDP URL from
    ``sb``, and reconnects.

    URLs that fail all per-URL retries in the first pass are queued and tried again once
    after every other URL in this call completes (deferred retry pass).
    """
    playwright = sync_playwright().start()
    browser = None
    page = None
    ep = endpoint_url

    def connect(url):
        nonlocal browser, page, ep
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
            browser = None
            page = None

        attempt_url = url
        last_err = None
        for attempt in range(1, CDP_CONNECT_RETRIES + 1):
            try:
                browser = playwright.chromium.connect_over_cdp(
                    attempt_url, timeout=CDP_CONNECT_TIMEOUT_MS
                )
                page = browser.contexts[0].pages[0]
                _configure_page_timeouts(page)
                ep = attempt_url
                if attempt > 1:
                    print(
                        f"CDP connected on attempt {attempt}/{CDP_CONNECT_RETRIES} ({attempt_url!r})"
                    )
                return
            except Exception as e:
                last_err = e
                print(
                    f"CDP connect attempt {attempt}/{CDP_CONNECT_RETRIES} "
                    f"failed for {attempt_url!r}: {e}"
                )
                if attempt >= CDP_CONNECT_RETRIES:
                    break
                delay = min(
                    CDP_RETRY_BACKOFF_MAX_SEC,
                    CDP_RETRY_DELAY_SEC * (2 ** (attempt - 1)),
                )
                if sb is not None and hasattr(sb, "sleep"):
                    sb.sleep(delay)
                else:
                    time.sleep(delay)
                if sb is not None and hasattr(sb, "get_endpoint_url"):
                    fresh = sb.get_endpoint_url()
                    if fresh:
                        attempt_url = fresh
        raise last_err

    def full_break(reason=""):
        nonlocal ep, browser, page
        print(
            f"Full break ({reason}): closing Playwright CDP session, "
            f"pausing {FULL_BREAK_SEC}s, reconnecting to Chrome..."
        )
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        browser = None
        page = None
        sb.sleep(CDP_POST_DISCONNECT_SETTLE_SEC)
        sb.sleep(FULL_BREAK_SEC)
        ep = sb.get_endpoint_url() if sb else ep
        connect(ep)

    outfield_rows = []
    keeper_rows = []
    connect(ep)

    try:
        def _scrape_url_pass(urls, *, is_deferred_retry_pass):
            """Run one pass over urls; appends to outfield_rows / keeper_rows; returns skipped_urls."""
            nonlocal page
            skipped = []
            n = len(urls)
            urls_since_break = 0
            for i, match_url in enumerate(urls):
                scraped_ok = False
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
                        scraped_ok = True
                        break
                    except Exception as e:
                        if attempt < MAX_ATTEMPTS_PER_MATCH:
                            print(
                                f"Retry {attempt}/{MAX_ATTEMPTS_PER_MATCH} for {match_url!r}: {e}"
                            )
                            try:
                                context = page.context
                                page.close()
                                page = context.new_page()
                                _configure_page_timeouts(page)
                            except Exception:
                                pass
                            backoff = min(
                                RETRY_BACKOFF_MAX_SEC, RETRY_DELAY_SEC * (2 ** (attempt - 1))
                            )
                            sb.sleep(backoff)
                        else:
                            if is_deferred_retry_pass:
                                print(
                                    f"Skipping match (no player stats or error): {match_url} — {e}"
                                )
                            else:
                                print(
                                    f"Deferred until after remaining URLs: {match_url} — {e}"
                                )
                            sb.sleep(PAUSE_AFTER_SKIP_SEC)
                            skipped.append(match_url)
                if scraped_ok:
                    sb.sleep(DELAY_BETWEEN_MATCHES_SEC)
                urls_since_break += 1
                if urls_since_break >= FULL_BREAK_EVERY_N_MATCHES and i < n - 1:
                    full_break(f"every {FULL_BREAK_EVERY_N_MATCHES} match URLs")
                    urls_since_break = 0
            return skipped

        deferred = _scrape_url_pass(match_urls, is_deferred_retry_pass=False)
        deferred = list(dict.fromkeys(deferred))
        if deferred:
            print(
                f"Deferred retry pass: {len(deferred)} URL(s) after completing the rest of this batch"
            )
            sb.sleep(PAUSE_BEFORE_DEFERRED_RETRY_SEC)
            full_break("before deferred retry pass")
            _scrape_url_pass(deferred, is_deferred_retry_pass=True)
    finally:
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass
        try:
            playwright.stop()
        except Exception:
            pass

    return outfield_rows, keeper_rows


def get_player_match_data(match_urls, date, season_id):
    """
    Load match page(s) and scrape both outfield and keeper player stats.
    Accepts match_urls (str or list), date, and season_id. Writes one CSV:
    player_match_data_{season_id}_{date}.csv (outfield + keeper joined).
    Uses SeleniumBase CDP (``sb``) for Playwright connection; includes periodic full breaks.
    Returns the combined DataFrame.
    """
    if isinstance(match_urls, str):
        match_urls = [match_urls]
    ep = sb.get_endpoint_url() if sb else None
    if not ep:
        raise RuntimeError("SeleniumBase CDP not configured: set module-level sb before get_player_match_data")
    outfield_rows, keeper_rows = _scrape_player_match_data_for_urls(ep, match_urls)

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


def scrape_player_match_data_for_season_date_groups(
    match_level_data,
    endpoint_url,
    season_id_filter=None,
    start_date_filter=None,
):
    """
    For each (season_id, date) in match_level_data, scrape player stats and write
    player_match_data_{season_id}_{date}.csv. Uses ``sb.get_endpoint_url()`` each chunk
    (``endpoint_url`` kept for API compatibility if ``sb`` is unset).

    If ``season_id_filter`` is set (e.g. ``"73_2026"``), only rows with that ``season_id``
    are scraped; other seasons in the DataFrame are ignored.

    If ``start_date_filter`` is set (e.g. ``"2026-03-15"``), only rows whose ``date`` is
    on or after that calendar day are kept (applied after the season filter). ``None`` keeps
    all dates in the filtered set. Groups are processed in chronological order.
    """
    if season_id_filter is not None:
        sid = str(season_id_filter).strip()
        before = len(match_level_data)
        match_level_data = match_level_data[
            match_level_data["season_id"].astype(str).str.strip() == sid
        ].copy()
        print(
            f"Filtered to season_id={sid!r}: {len(match_level_data)} row(s) "
            f"(from {before} total in seed)."
        )
        if match_level_data.empty:
            print("No rows left after season filter; nothing to scrape.")
            return

    if start_date_filter is not None:
        raw = str(start_date_filter).strip()
        start_ts = pd.to_datetime(raw, errors="coerce")
        if pd.isna(start_ts):
            print(f"Invalid start_date_filter {start_date_filter!r}; ignoring start-date filter.")
        else:
            start_ts = pd.Timestamp(start_ts).normalize()
            before = len(match_level_data)
            dcol = pd.to_datetime(match_level_data["date"], errors="coerce")
            match_level_data = match_level_data[dcol.notna() & (dcol >= start_ts)].copy()
            print(
                f"Filtered to match date >= {start_ts.date()!s}: {len(match_level_data)} row(s) "
                f"(from {before} after season filter)."
            )
            if match_level_data.empty:
                print("No rows left after start-date filter; nothing to scrape.")
                return

    _dsort = pd.to_datetime(match_level_data["date"], errors="coerce")
    match_level_data = (
        match_level_data.assign(__dsort=_dsort)
        .sort_values(["season_id", "__dsort"], kind="mergesort")
        .drop(columns=["__dsort"])
    )

    for _date_group_idx, ((season_id, date), group) in enumerate(
        match_level_data.groupby(["season_id", "date"])
    ):
        if _date_group_idx > 0:
            sb.sleep(PAUSE_BETWEEN_DATE_GROUPS_SEC)
        if "notes" in group.columns:
            skip_mask = group["notes"].apply(_notes_skip_player_scrape)
            match_urls = group.loc[~skip_mask, "match_url"].dropna().tolist()
        else:
            match_urls = group["match_url"].dropna().tolist()
        if not match_urls:
            continue
        all_outfield = []
        all_keeper = []
        num_chunks = (len(match_urls) + URL_CHUNK_SIZE - 1) // URL_CHUNK_SIZE
        for chunk_idx, chunk_start in enumerate(
            range(0, len(match_urls), URL_CHUNK_SIZE), start=1
        ):
            chunk = match_urls[chunk_start : chunk_start + URL_CHUNK_SIZE]
            print(
                f"Beginning URL chunk {chunk_idx}/{num_chunks} "
                f"({len(chunk)} URLs) season_id={season_id!r} date={date!r}"
            )
            current_ep = sb.get_endpoint_url() if sb else endpoint_url
            of, kp = _scrape_player_match_data_for_urls(current_ep, chunk)
            all_outfield.extend(of)
            all_keeper.extend(kp)
            print(
                f"Finished URL chunk {chunk_idx}/{num_chunks} "
                f"season_id={season_id!r} date={date!r}"
            )
            if chunk_idx < num_chunks:
                sb.sleep(PAUSE_BETWEEN_CHUNKS_SEC)
        merged = merge_keeper_into_outfield(all_outfield, all_keeper)
        safe_season = str(season_id).replace("/", "_").replace(" ", "_")
        safe_date = str(date).replace("/", "-").replace(" ", "_").replace(":", "-")
        csv_path = player_match_stats_file_directory + f"player_match_data_{safe_season}_{safe_date}.csv"
        merged.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d", quoting=csv.QUOTE_NONNUMERIC)
        print(f"Exported {len(merged)} rows to {csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape FBref player match stats using rows from cleaned all_match_stats.csv."
    )
    parser.add_argument(
        "--season-id",
        dest="season_id",
        default=None,
        metavar="ID",
        help=(
            "Only scrape this season (e.g. 73_2026). If omitted, uses RUN_FILTER_SEASON_ID "
            "near the top of this file (None = all seasons in the seed CSV)."
        ),
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Only scrape matches on or after this date (within the season filter). "
            "If omitted, uses RUN_FILTER_START_DATE (None = all dates)."
        ),
    )
    args = parser.parse_args()

    if args.season_id is not None:
        season_filter = str(args.season_id).strip() or None
    elif RUN_FILTER_SEASON_ID is None:
        season_filter = None
    else:
        season_filter = str(RUN_FILTER_SEASON_ID).strip() or None

    if args.start_date is not None:
        start_date_filter = str(args.start_date).strip() or None
    elif RUN_FILTER_START_DATE is None:
        start_date_filter = None
    else:
        start_date_filter = str(RUN_FILTER_START_DATE).strip() or None

    sb = sb_cdp.Chrome()
    endpoint_url = sb.get_endpoint_url()

    match_level_data = load_match_level_data_from_cleaned_seed(CLEANED_ALL_MATCH_STATS_PATH)
    print(f"Loaded {len(match_level_data)} match level data row(s) from {CLEANED_ALL_MATCH_STATS_PATH}")
    if season_filter:
        print(f"Season filter active: {season_filter!r} (from CLI or RUN_FILTER_SEASON_ID)")
    if start_date_filter:
        print(f"Start-date filter active: {start_date_filter!r} (from CLI or RUN_FILTER_START_DATE)")

    scrape_player_match_data_for_season_date_groups(
        match_level_data,
        endpoint_url,
        season_id_filter=season_filter,
        start_date_filter=start_date_filter,
    )

    export_file_directory = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
    player_match_stats_export_name = "all_player_match_stats.csv"
    player_match_stats_files = glob.glob(os.path.join(player_match_stats_file_directory, "*.csv"))
    csv_appender(player_match_stats_file_directory, export_file_directory, player_match_stats_export_name)
    print(f"Exported {len(player_match_stats_files)} row(s) to {player_match_stats_export_name}")