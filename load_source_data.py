import duckdb

def load_data():
    # Connect to a persistent DuckDB file
    # If the file doesn't exist, this creates it
    con = duckdb.connect('usl_championship_data.duckdb')

    print("🚀 Starting ELT Process...")

    # 1. create a schema for raw data
    con.sql("CREATE SCHEMA IF NOT EXISTS raw;")

    base = "/Users/parnold/Personal/Personal Projects/github/usl_championship_data/dbt_usl_championship/seeds/cleaned_data"

    # Load Player Match Stats Data (CSV) – keep ID columns as VARCHAR so 32428e51 is not read as float
    print("... Loading Player Match Stats Data")
    con.sql(f"""
        CREATE OR REPLACE TABLE raw.source_player_match_stats AS
        SELECT * FROM read_csv_auto('{base}/all_player_match_stats.csv', header=true, types={{'player_id': 'VARCHAR', 'match_id': 'VARCHAR'}});
    """)

    # Load Match Stats Data (CSV) – keep ID columns as VARCHAR so match_id is not read as float
    print("... Loading Match Stats Data")
    con.sql(f"""
        CREATE OR REPLACE TABLE raw.source_match_stats AS
        SELECT * FROM read_csv_auto('{base}/all_match_stats.csv', header=true, types={{'match_id': 'VARCHAR', 'season_id': 'VARCHAR'}});
    """)

    # Load Season Dims Data (CSV) – keep season_id as VARCHAR
    print("... Loading Season Dims Data")
    con.sql(f"""
        CREATE OR REPLACE TABLE raw.source_season_dims AS
        SELECT * FROM read_csv_auto('{base}/all_season_dims.csv', header=true, types={{'season_id': 'VARCHAR'}});
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