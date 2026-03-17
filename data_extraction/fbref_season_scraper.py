'''
Starting with setting all of the competitions and seasons we want to scrape
Adding some default variables like base URLs
'''


import csv
import glob
import os
import pandas as pd


FBREF_BASE_URL = "https://fbref.com"
_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
file_directory = _scraped_data_base + "/"
season_dims_file_directory = _scraped_data_base + "/season_dims/"

season_year = [
        "2026",  # 2026 Season
        "2025",  # 2025 Season
        "2024",  # 2024 Season
        "2023",  # 2023 Season
        "2022",  # 2022 Season
        "2021",  # 2021 Season
        "2020",  # 2020 Season
        "2019",  # 2019 Season
        "2018",  # 2018 Season
        "2017",  # 2017 Season
]
leagues = {
        "73": "USL Championship",  # USL Championship
}


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
    subset.to_csv(csv_path, index=False, date_format="%Y-%m-%d", quoting=csv.QUOTE_NONNUMERIC)
    print(f"Exported {len(subset)} row(s) to {csv_path}")


cleaned_export_file_directory = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"

season_dims_files = glob.glob(os.path.join(season_dims_file_directory, "*.csv"))

season_dims_export_name = "all_season_dims.csv"

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

csv_appender(season_dims_file_directory, cleaned_export_file_directory, season_dims_export_name)
print(f"Exported {len(season_dims_files)} row(s) to {season_dims_export_name}")