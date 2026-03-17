# Run with project venv: .venv/bin/python model_building.py
import duckdb
import pandas as pd
from pathlib import Path

# Use DB in project root so we see the same views/tables as load.py and dbt
project_root = Path(__file__).resolve().parent
connection = duckdb.connect(project_root / "usl_championship_data.duckdb")

export_path = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data"


match_level_player_data_query = """
SELECT * FROM match_level_player_data
"""

match_level_player_data_result = connection.sql(match_level_player_data_query).df()
match_level_player_data_result.to_csv(export_path + "/match_level_player_data.csv", index=False)
print("Match level player data query result saved to match_level_player_data.csv")

match_level_team_data_query = """
SELECT * FROM matches_by_team
"""

match_level_team_data_result = connection.sql(match_level_team_data_query).df()
match_level_team_data_result.to_csv(export_path + "/match_level_team_data.csv", index=False)
print("Match level team data query result saved to match_level_team_data.csv")


dim_match_data_query = """
SELECT * FROM dim_match
"""

dim_match_data_result = connection.sql(dim_match_data_query).df()
dim_match_data_result.to_csv(export_path + "/dim_match_data.csv", index=False)
print("Dim match data query result saved to dim_match_data.csv")

