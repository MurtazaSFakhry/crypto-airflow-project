from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestQuoteRequest
import psycopg2
from datetime import datetime, timedelta, timezone
import time


def ingest_hourly_bars():
    """
    1. Fetch last completed hourly bars for BTC & ETH
    2. Insert into Postgres
    3. Commit
    """
    print("Starting hourly ingestion...", flush=True)

    client = CryptoHistoricalDataClient()

    # Creating request object for the last two hours to ensure last completed hourly bar is present
    request = CryptoBarsRequest(
        symbol_or_symbols=["BTC/USD", "ETH/USD"],
        timeframe=TimeFrame.Hour,
        start=datetime.now(timezone.utc) - timedelta(hours=2),
        end=datetime.now(timezone.utc),
    )

    print("request prepared", flush=True)

    # Retrieve bars
    bars = client.get_crypto_bars(request).df
    bars = bars.reset_index()
    print(f"bars rows: {len(bars)}", flush=True)

    # Metadata
    bars["timeframe"] = "1h"
    bars["ingestion_timestamp"] = datetime.now(timezone.utc)

    print("connecting to DB", flush=True)
    conn = psycopg2.connect(
        host="localhost",
        database="crypto_pipeline",
        user="postgres",
        password="Murtaza",
        port=5432,
    )
    cur = conn.cursor()

    # Ensure target table exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS crypto_hourly_bars_raw (
        symbol TEXT NOT NULL,
        bar_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        trade_count INTEGER,
        vwap DOUBLE PRECISION,
        timeframe TEXT,
        ingestion_timestamp TIMESTAMP WITH TIME ZONE,
        PRIMARY KEY (symbol, bar_timestamp)
    );
    """
    cur.execute(create_table_sql)

    insert_sql = """
    INSERT INTO crypto_hourly_bars_raw (
        symbol, bar_timestamp, open, high, low, close,
        volume, trade_count, vwap, timeframe, ingestion_timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, bar_timestamp) DO NOTHING;
    """

    inserted = 0
    for i, row in bars.iterrows():
        try:
            cur.execute(
                insert_sql,
                (
                    row.get("symbol"),
                    row.get("timestamp"),
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("volume"),
                    row.get("trade_count"),
                    row.get("vwap"),
                    row.get("timeframe"),
                    row.get("ingestion_timestamp"),
                ),
            )
            inserted += 1
        except Exception as e:
            print(f"Failed to insert row {i}: {e}", flush=True)

    print(f"Attempted inserts: {len(bars)}, successful inserts: {inserted}", flush=True)

    # Commit only after attempting all inserts
    try:
        conn.commit()
        print("Transaction committed.", flush=True)
    except Exception as e:
        print(f"Commit failed: {e}", flush=True)

    # Verify commit by querying count for this timeframe
    try:
        cur.execute("SELECT COUNT(*) FROM crypto_hourly_bars_raw WHERE timeframe=%s;", ("1h",))
        cnt = cur.fetchone()[0]
        print(f"Rows in table (timeframe=1h): {cnt}", flush=True)
    except Exception as e:
        print(f"Verification query failed: {e}", flush=True)

    cur.close()
    conn.close()

    print("Ingestion complete.", flush=True)


if __name__ == "__main__":
    # Infinite 1-hour scheduler
    client = CryptoHistoricalDataClient()

    r1 = CryptoLatestQuoteRequest(symbol_or_symbols=["ETH/USD", "BTC/USD"])
    lc = client.get_crypto_latest_quote(r1)
    print(r1)
    print(lc)
    while True:
        try:
            print(f"Run started at {datetime.utcnow()} UTC", flush=True)
            ingest_hourly_bars()
            print("Sleeping for 1 hour...\n", flush=True)

        except Exception as e:
            print("ERROR during ingestion:", e, flush=True)

        time.sleep(60 * 60)  # 1 hour


#additional
client = CryptoHistoricalDataClient()

r1 = CryptoLatestQuoteRequest(symbol_or_symbols=["ETH/USD", "BTC/USD"])
lc = client.get_crypto_latest_quote(r1)
