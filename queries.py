# Run with project venv: .venv/bin/python queries.py
import duckdb
import pandas as pd

connection = duckdb.connect('usl_championship_data.duckdb')

query = """
SELECT * FROM match_level_player_data
"""

result = connection.sql(query).df()
result.to_csv("match_level_player_data.csv", index=False)
print("Query result saved to match_level_player_data.csv")

