import polars as pl
from datetime import datetime

# Path to our Lakehouse
PARQUET_PATH = "/Users/nidhi/Developer/spotify_lakehouse/spotify_history.parquet"

def run_health_check():
    print(">>> Starting Data Observability Health Check...")
    
    # Load the data
    df = pl.read_parquet(PARQUET_PATH)
    total_rows = len(df)
    
    # 1. Check for Nulls in Critical Columns
    null_counts = df.select([
        pl.col("track_name").null_count().alias("missing_tracks"),
        pl.col("artist_name").null_count().alias("missing_artists"),
        pl.col("ts").null_count().alias("missing_timestamps")
    ])
    
    # 2. Check for "Impossible" Data (Data Quality)
    # A song usually isn't longer than 20 minutes (1,200,000 ms)
    outliers = df.filter(pl.col("ms_played") > 1_200_000).height
    
    # 3. Check for "Data Gaps" (Consistency)
    # Calculate the max gap between listening days
    days = df.select(pl.col("ts").dt.date().unique().sort())
    max_gap = days.select(
        (pl.col("ts") - pl.col("ts").shift(1)).dt.total_days().max()
    ).item()
    
    # 4. Freshness Check
    last_listen = df.select(pl.col("ts").max()).item()
    days_since_last = (datetime.now() - last_listen.replace(tzinfo=None)).days

    # --- REPORTING ---
    print("\n--- 🏥 DATA HEALTH REPORT ---")
    print(f"✅ Total Records: {total_rows:,}")
    print(f"📅 Data Range: {df['ts'].min().date()} to {df['ts'].max().date()}")
    
    print("\n[QUALITY CHECK]")
    if null_counts["missing_tracks"][0] == 0:
        print("  🟢 No missing track/artist names.")
    else:
        print(f"  🔴 WARNING: {null_counts['missing_tracks'][0]} records have missing names.")
        
    if outliers == 0:
        print("  🟢 No duration outliers detected.")
    else:
        print(f"  🟡 NOTE: {outliers} tracks were longer than 20 mins (likely podcasts or long mixes).")

    print("\n[PIPELINE CONSISTENCY]")
    if max_gap <= 7:
        print(f"  🟢 Consistency looks good. Largest gap was only {max_gap} days.")
    else:
        print(f"  🟡 WARNING: Found a gap of {max_gap} days in your history.")

    print("\n[FRESHNESS]")
    if days_since_last < 30:
        print(f"  🟢 Data is fresh (Last listen: {days_since_last} days ago).")
    else:
        print(f"  🔴 STALE DATA: Your last recorded listen was {days_since_last} days ago.")

if __name__ == "__main__":
    run_health_check()
