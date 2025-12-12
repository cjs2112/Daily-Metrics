import os
import sys
import psycopg2
from datetime import date, timedelta


def get_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.environ["PG_PORT"],
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def run_daily(run_date: date):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT metrics.run_daily_metrics(%s);",
                (run_date,)
            )


def backfill(days: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT metrics.backfill_last_n_days(%s);",
                (days,)
            )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Default behavior: run yesterday
        run_date = date.today() - timedelta(days=1)
        run_daily(run_date)
    else:
        # Manual backfill: python run_daily_metrics.py 30
        days = int(sys.argv[1])
        backfill(days)
