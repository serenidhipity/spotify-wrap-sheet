import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Define path
PARQUET_PATH = "/Users/nidhi/Developer/spotify_lakehouse/spotify_history.parquet"
OUTPUT_DIR = "/Users/nidhi/Developer/spotify_visuals"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print(">>> Loading Data from Lakehouse...")
df = pl.read_parquet(PARQUET_PATH)

# Set visual style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)

# --- 1. Top 10 Loyal Artists (From our Phase 2 calculation) ---
print(">>> Generating Top Artists Plot...")
top_loyal = (
    df
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
    .with_columns(
        ((pl.col("unique_days") * pl.col("total_minutes")) / (1 + pl.col("skip_rate")))
        .alias("loyalty_score")
    )
    .sort("loyalty_score", descending=True)
    .head(10)
    .to_pandas()
)

plt.figure(figsize=(12, 6))
sns.barplot(data=top_loyal, x="loyalty_score", y="artist_name", palette="viridis")
plt.title("Top 10 Most 'Loyal' Artists (Consistency x Minutes / Patience)")
plt.savefig(os.path.join(OUTPUT_DIR, "top_loyal_artists.png"))
plt.close()

# --- 2. Monthly Listening Volume (Timeline) ---
print(">>> Generating Timeline Plot...")
monthly_trend = (
    df
    .with_columns(pl.col("ts").dt.truncate("1mo").alias("month"))
    .group_by("month")
    .agg((pl.col("ms_played").sum() / 3600000).alias("hours_played"))
    .sort("month")
    .to_pandas()
)

plt.figure(figsize=(14, 6))
plt.plot(monthly_trend["month"], monthly_trend["hours_played"], color='royalblue', linewidth=2.5)
plt.fill_between(monthly_trend["month"], monthly_trend["hours_played"], alpha=0.3, color='royalblue')
plt.title("Listening Hours per Month (2018-2025)")
plt.ylabel("Hours Played")
plt.savefig(os.path.join(OUTPUT_DIR, "listening_timeline.png"))
plt.close()

# --- 3. The "Listening Clock" (Hour of Day Heatmap) ---
print(">>> Generating Activity Heatmap...")
activity_matrix = (
    df
    .with_columns([
        pl.col("ts").dt.hour().alias("hour"),
        pl.col("ts").dt.weekday().alias("weekday")
    ])
    .group_by(["hour", "weekday"])
    .agg(pl.count().alias("play_count"))
    .to_pandas()
    .pivot(index="weekday", columns="hour", values="play_count")
    .fillna(0)
)

# Replace weekday numbers with names
day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
activity_matrix.index = [day_names[i-1] for i in activity_matrix.index]

plt.figure(figsize=(15, 6))
sns.heatmap(activity_matrix, cmap="YlGnBu", annot=False)
plt.title("Listening Intensity: Day of Week vs. Hour of Day")
plt.xlabel("Hour (24h)")
plt.savefig(os.path.join(OUTPUT_DIR, "activity_heatmap.png"))
plt.close()

print(f"\n✅ Visualizations Complete! Files saved in: {OUTPUT_DIR}")
