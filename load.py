import duckdb

def load_data():
    # Connect to a persistent DuckDB file
    # If the file doesn't exist, this creates it
    con = duckdb.connect('usl_championship_data.duckdb')

    print("🚀 Starting ELT Process...")

    # 1. create a schema for raw data
    con.sql("CREATE SCHEMA IF NOT EXISTS raw;")

    # Load Player Match Stats Data (CSV)
    print("... Loading Player Match Stats Data")
    con.sql("""
        CREATE OR REPLACE TABLE raw.source_player_match_stats AS 
        SELECT * FROM read_csv_auto('/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data/all_player_match_stats.csv');
    """)

    # Load Match Stats Data (CSV)
    print("... Loading Match Stats Data")
    con.sql("""
        CREATE OR REPLACE TABLE raw.source_match_stats AS 
        SELECT * FROM read_csv_auto('/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data/all_match_stats.csv');
    """)

    # Load Season Dims Data (CSV)
    print("... Loading Season Dims Data")
    con.sql("""
        CREATE OR REPLACE TABLE raw.source_season_dims AS 
        SELECT * FROM read_csv_auto('/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data/all_season_dims.csv');
    """)
    
    # Validation check - to see if data is loaded into database successfully
    count = con.sql("SELECT count(*) FROM raw.source_player_match_stats").fetchone()
    print(f"✅ Loaded {count[0]} player match stats records.")
    count = con.sql("SELECT count(*) FROM raw.source_match_stats").fetchone()
    print(f"✅ Loaded {count[0]} match stats records.")
    count = con.sql("SELECT count(*) FROM raw.source_season_dims").fetchone()
    print(f"✅ Loaded {count[0]} season dims records.")
    
    con.close()

if __name__ == "__main__":
    load_data()