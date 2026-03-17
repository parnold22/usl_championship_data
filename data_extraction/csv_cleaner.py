import csv
import glob
import os
import pandas as pd


_scraped_data_base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/scraped_data"
file_directory = _scraped_data_base + "/"
player_match_stats_file_directory = _scraped_data_base + "/player_match_stats/"
match_stats_file_directory = _scraped_data_base + "/match_stats/"
season_dims_file_directory = _scraped_data_base + "/season_dims/"
export_file_directory = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"


season_dims_files = glob.glob(os.path.join(season_dims_file_directory, "*.csv"))
match_stats_files = glob.glob(os.path.join(match_stats_file_directory, "*.csv"))
player_match_stats_files = glob.glob(os.path.join(player_match_stats_file_directory, "*.csv"))

season_dims_export_name = "all_season_dims.csv"
match_stats_export_name = "all_match_stats.csv"
player_match_stats_export_name = "all_player_match_stats.csv"

def csv_appender(file_directory, export_directory, export_name):
    all_files = glob.glob(os.path.join(file_directory, "*.csv"))
    new_df = []

    for file in all_files:
        # Read every column as string to avoid:
        # - match_id/player_id (e.g. 88268e55) being parsed as float → 8.83E+59
        # - date (YYYY-MM-DD) being parsed as datetime → rewritten as M/DD/YY
        df = pd.read_csv(file, dtype=str, keep_default_na=False)
        new_df.append(df)

    final_df = pd.concat(new_df, ignore_index=True)
    final_df.to_csv(
        os.path.join(export_directory, export_name),
        index=False,
        date_format="%Y-%m-%d",
        quoting=csv.QUOTE_NONNUMERIC,  # quote IDs (e.g. 32428e51) so they aren't read as numbers
    )

csv_appender(season_dims_file_directory, export_file_directory, season_dims_export_name)
csv_appender(match_stats_file_directory, export_file_directory, match_stats_export_name)
csv_appender(player_match_stats_file_directory, export_file_directory, player_match_stats_export_name)
