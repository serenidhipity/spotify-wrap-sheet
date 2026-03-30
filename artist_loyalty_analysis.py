import polars as pl
import os

# Define path
PARQUET_PATH = "/Users/nidhi/Developer/spotify_lakehouse/spotify_history.parquet"

print(">>> Initializing Polars (High-Speed Analytics Engine)...")

# Step 1: Use the LAZY API
# scan_parquet() doesn't load the data yet; it just creates a "plan".
lazy_df = pl.scan_parquet(PARQUET_PATH)

# Step 2: Define the Transformation Plan
# We'll calculate:
# - Total Minutes
# - Consistency (Unique Days Listened)
# - Patience (Inverse of Skip Rate)
analysis_plan = (
    lazy_df
    .with_columns([
        (pl.col("ms_played") / 60000).alias("minutes"),
        pl.col("ts").dt.date().alias("date")
    ])
    .group_by("artist_name")
    .agg([
        pl.col("minutes").sum().alias("total_minutes"),
        pl.col("date").n_unique().alias("unique_days"),
        pl.col("skipped").mean().alias("skip_rate")
    ])
    # Calculate a custom Loyalty Score: 
    # (Consistency * Minutes) / (1 + Skip Rate)
    .with_columns(
        ((pl.col("unique_days") * pl.col("total_minutes")) / (1 + pl.col("skip_rate")))
        .alias("loyalty_score")
    )
    .sort("loyalty_score", descending=True)
    .limit(10)
)

print(">>> Optimizing and Executing the Query Plan...")
# collect() tells Polars to execute the plan in the most efficient way possible.
top_loyal_artists = analysis_plan.collect()

print("\n🏆 Top 10 Artists by Loyalty Score:")
print(top_loyal_artists)

# Show off the "Query Plan" (What the robot actually decided to do)
print("\n🔍 The 'Optimized' Brain of Polars (Execution Plan):")
print(analysis_plan.explain())
