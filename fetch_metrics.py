import os
import sys
import pandas as pd
from sqlalchemy import create_engine

def get_db_url():
    db_url = os.environ.get("RENDER_DB_URL")
    if not db_url:
        print("ERROR: Environment variable RENDER_DB_URL is not set.")
        sys.exit(1)
    return db_url

def fetch_data():
    db_url = get_db_url()
    engine = create_engine(db_url)

    query = """
        SELECT
            l.lead_id,
            l.created_at,
            r.rep_name,
            COUNT(i.interaction_id) AS interaction_count
        FROM leads l
        LEFT JOIN reps r ON l.rep_id = r.rep_id
        LEFT JOIN interactions i ON l.lead_id = i.lead_id
        GROUP BY 1,2,3
        ORDER BY l.lead_id;
    """

    df = pd.read_sql(query, engine)
    return df

def save_metrics(df):
    output_path = "metrics_daily.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved metrics to {output_path}")

def main():
    print("Running fetch_metrics.py...")
    df = fetch_data()
    print(df.head())   # quick console-check
    save_metrics(df)
    print("Done.")

if __name__ == "__main__":
    main()
