"""
In-season updater: for the latest season in scraped_data/match_stats,
1) Re-scrape the schedule to get all completed matches and overwrite that season's match_level_data CSV.
2) Scrape player-level match stats only for (season_id, date) that are not already in scraped_data/player_match_stats.
Requires a browser started via SeleniumBase CDP (run with the same env as fbref_match_scraper / fbref_player_match_scraper).
"""

import glob
import os
import re
import pandas as pd
from seleniumbase import sb_cdp

# Local modules (must be importable; set their sb/endpoint_url before calling get_match_data)
import fbref_match_scraper as match_scraper
import fbref_player_match_scraper as player_scraper


_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
_cleaned_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"
MATCH_STATS_DIR = os.path.join(_scraped_data_base, "match_stats")
PLAYER_MATCH_STATS_DIR = os.path.join(_scraped_data_base, "player_match_stats")
SEASON_DIMS_DIR = os.path.join(_scraped_data_base, "season_dims")
MATCH_LEVEL_FILE_PATTERN = "match_level_data_*.csv"
PLAYER_MATCH_FILE_PREFIX = "player_match_data_"


def _season_key_from_path(path: str) -> tuple:
    """Extract (league_id, year) from path like .../match_level_data_73_2026.csv for sorting."""
    basename = os.path.basename(path)
    m = re.match(r"match_level_data_(\d+)_(\d+)\.csv", basename, re.IGNORECASE)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (0, 0)


def get_latest_season_match_stats_path(match_stats_dir: str = MATCH_STATS_DIR) -> str | None:
    """Return path to the match_level_data CSV for the latest season (by league_id, then year)."""
    pattern = os.path.join(match_stats_dir, MATCH_LEVEL_FILE_PATTERN)
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=_season_key_from_path)


def get_latest_season_id_from_path(latest_path: str) -> str:
    """e.g. match_level_data_73_2026.csv -> 73_2026"""
    basename = os.path.basename(latest_path)
    return basename.replace("match_level_data_", "").replace(".csv", "")


def build_schedule_row_for_season(season_id: str) -> pd.DataFrame:
    """One-row schedule DataFrame for get_match_data: league_id, league_name, season_id, season_name, url."""
    required = ["league_id", "league_name", "season_id", "season_name", "url"]
    # Try scraped_data/season_dims CSVs, then cleaned_data/all_season_dims.csv
    paths = sorted(glob.glob(os.path.join(SEASON_DIMS_DIR, "*.csv")))
    paths.append(os.path.join(_cleaned_data_base, "all_season_dims.csv"))
    for path in paths:
        if not os.path.isfile(path):
            continue
        try:
            df = pd.read_csv(path)
            if "season_id" not in df.columns or "url" not in df.columns:
                continue
            row = df[df["season_id"].astype(str) == str(season_id)]
            if not row.empty:
                out = row.iloc[[0]].copy()
                for c in required:
                    if c not in out.columns:
                        out[c] = None
                return out.reindex(columns=required)
        except Exception:
            continue
    # Fallback: build URL from season_id (e.g. 73_2026)
    parts = str(season_id).split("_")
    if len(parts) >= 2:
        league_id, year = parts[0], parts[1]
        url = f"https://fbref.com/en/comps/{league_id}/{year}/schedule/"
        return pd.DataFrame([{
            "league_id": league_id,
            "league_name": "USL Championship",
            "season_id": season_id,
            "season_name": f"{year} Season",
            "url": url,
        }])
    return pd.DataFrame(columns=required)


def get_stored_player_dates_for_season(season_id: str, player_match_stats_dir: str = PLAYER_MATCH_STATS_DIR) -> set[str]:
    """Parse filenames player_match_data_{season_id}_{date}.csv and return set of date strings (YYYY-MM-DD)."""
    safe_season = str(season_id).replace("/", "_").replace(" ", "_")
    prefix = f"{PLAYER_MATCH_FILE_PREFIX}{safe_season}_"
    pattern = os.path.join(player_match_stats_dir, "*.csv")
    stored = set()
    for path in glob.glob(pattern):
        basename = os.path.basename(path)
        if basename.startswith(prefix) and basename.endswith(".csv"):
            # player_match_data_73_2026_2026-03-07.csv -> 2026-03-07
            date_part = basename[len(prefix) : -4]
            if date_part:
                stored.add(date_part)
    return stored


def update_latest_season_match_stats(latest_path: str, endpoint_url: str) -> None:
    """Re-scrape completed matches for the latest season and overwrite latest_path."""
    season_id = get_latest_season_id_from_path(latest_path)
    schedule_df = build_schedule_row_for_season(season_id)
    if schedule_df.empty:
        print(f"No schedule row for season {season_id}; skipping match_stats update.")
        return
    schedule_url = schedule_df["url"].iloc[0]
    print(f"Step 1a: Re-scraping match schedule for season {season_id} from FBRef (completed matches only)...")
    print(f"         Schedule URL: {schedule_url}")
    match_scraper.endpoint_url = endpoint_url
    all_matches = match_scraper.get_match_data(schedule_df)
    if not all_matches:
        print("No matches returned from schedule scrape; not overwriting", latest_path)
        return
    matches_df = pd.DataFrame(all_matches)
    matches_df = matches_df[matches_df["season_id"].astype(str) == str(season_id)]
    if matches_df.empty:
        print("No matches for season", season_id, "; not overwriting", latest_path)
        return
    # Same score formatting as match scraper
    for col in ["score"]:
        if col in matches_df.columns:
            matches_df[col] = matches_df[col].fillna("").astype(str).replace("nan", "")
            matches_df[col] = matches_df[col].apply(lambda x: "'" + x if x else "")
    matches_df.to_csv(latest_path, index=False)
    print(f"Exported {len(matches_df)} rows to {latest_path}")


def scrape_missing_player_dates(
    latest_path: str,
    season_id: str,
    endpoint_url: str,
    player_match_stats_dir: str = PLAYER_MATCH_STATS_DIR,
) -> None:
    """Load match_level_data from latest_path; for (season_id, date) not in player_match_stats, scrape and write."""
    df = pd.read_csv(latest_path)
    if "match_report" in df.columns and "match_url" not in df.columns:
        df = df.rename(columns={"match_report": "match_url"})
    for c in ["match_url", "season_id", "date"]:
        if c not in df.columns:
            df[c] = None
    stored_dates = get_stored_player_dates_for_season(season_id, player_match_stats_dir)
    df["_date_str"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if not stored_dates:
        to_scrape = df
    else:
        to_scrape = df[~df["_date_str"].isin(stored_dates)].copy()
    to_scrape = to_scrape.drop(columns=["_date_str"], errors="ignore")
    if to_scrape.empty:
        print("Step 2: No new dates to scrape for player_match_stats (all dates already have CSVs).")
        return
    missing_dates = sorted(to_scrape["date"].dropna().unique().tolist()) if "date" in to_scrape.columns else []
    print(f"Step 2: Scraping player match stats for {len(missing_dates)} date(s) not yet in player_match_stats: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
    keep = [c for c in ["match_url", "season_id", "date", "notes"] if c in to_scrape.columns]
    match_level_data = to_scrape[keep].copy()
    player_scraper.scrape_player_match_data_for_season_date_groups(match_level_data, endpoint_url)


def get_latest_match_date_for_latest_season(match_stats_dir: str = MATCH_STATS_DIR) -> str | None:
    """Load match_stats for the latest season and return the most recent match date (YYYY-MM-DD)."""
    path = get_latest_season_match_stats_path(match_stats_dir)
    if not path:
        return None
    df = pd.read_csv(path)
    if "date" not in df.columns or df["date"].empty:
        return None
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return dates.max().strftime("%Y-%m-%d")


def run_in_season_update() -> None:
    """Run full in-season update: refresh latest season match_stats, then scrape missing player_match_stats dates."""
    latest_path = get_latest_season_match_stats_path(MATCH_STATS_DIR)
    if not latest_path:
        print("No match_level_data CSV files found in", MATCH_STATS_DIR)
        return
    season_id = get_latest_season_id_from_path(latest_path)
    print("=" * 60)
    print("In-season update: re-scrape match schedule + player stats for missing dates")
    print("=" * 60)
    print(f"Latest season: {season_id}")
    print(f"Match stats file: {latest_path}")

    print("\nStarting browser (SeleniumBase CDP)...")
    sb = sb_cdp.Chrome()
    endpoint_url = sb.get_endpoint_url()
    match_scraper.sb = sb
    player_scraper.sb = sb
    if not endpoint_url:
        print("ERROR: Could not get CDP endpoint_url. Is Chrome running with SeleniumBase?")
        return
    print("Browser ready.\n")

    # 1) Re-scrape match schedule for latest season (completed matches only) and overwrite latest_path
    update_latest_season_match_stats(latest_path, endpoint_url)
    print("Step 1b: Refreshing consolidated all_match_stats.csv...")
    match_scraper.csv_appender(MATCH_STATS_DIR, _cleaned_data_base, "all_match_stats.csv")
    print(f"         Wrote {_cleaned_data_base}/all_match_stats.csv\n")

    # 2) Scrape player stats only for (season_id, date) not already in player_match_stats
    scrape_missing_player_dates(latest_path, season_id, endpoint_url, PLAYER_MATCH_STATS_DIR)

    # 3) Rebuild consolidated all_player_match_stats.csv
    print("\nStep 3: Rebuilding consolidated all_player_match_stats.csv...")
    player_scraper.csv_appender(PLAYER_MATCH_STATS_DIR, _cleaned_data_base, "all_player_match_stats.csv")
    print(f"         Wrote {_cleaned_data_base}/all_player_match_stats.csv\n")

    latest_date = get_latest_match_date_for_latest_season(MATCH_STATS_DIR)
    print("=" * 60)
    print("Done. Latest match date in match_stats:", latest_date or "(none)")
    print("=" * 60)


if __name__ == "__main__":
    run_in_season_update()
