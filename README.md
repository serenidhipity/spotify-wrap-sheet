# Spotify Data Synthesis & Analysis (2018-2025)

This repository documents the technical process of transforming raw Spotify Extended Streaming History into a comprehensive analytical report.

## 🛠️ Data Synthesis Process

The analysis was performed using **Python (Pandas, Matplotlib, Seaborn)** and **Jupyter Notebooks**. The following steps were taken to synthesize the raw data:

### 1. Data Cleaning & Integration
- **Merging:** Combined 14+ large JSON files (`Streaming_History_Audio_*.json`) into a single unified dataset of over 214,000 streams.
- **Normalization:** Converted UTC timestamps to local datetime objects and normalized `ms_played` into minutes for easier calculation.
- **Filtering:** Removed podcast and non-musical entries to focus strictly on artist and track metadata.

### 2. Feature Engineering
- **Genre & Language Mapping:** Manually mapped the top 500+ artists to their primary genres and 15+ regional/global languages (Hindi, Korean, Urdu, Punjabi, Tamil, etc.).
- **Session Identification:** Implemented a session-tracking algorithm that identifies continuous "binge" listening blocks by calculating gaps between streams (threshold: 15 minutes).
- **Start-Reason Classification:** Categorized streams into "Manual Choice" vs. "Algorithmic Flow" basedon the `reason_start` metadata (e.g., `clickrow` vs. `trackdone`).

### 3. Statistical Modeling
- **Skip Rate Analysis:** Calculated the probability of a song being skipped based on the `skipped` field and `reason_end` flags.
- **Yearly Trend Mapping:** Analyzed the evolution of artist diversity (unique artists per year) and loyalty (dominance of top artists) from 2018 to 2025.
- **Temporal Analysis:** Created "Genre Clocks" and "Genre Seasons" by aggregating listening minutes by hour-of-day and month-of-year.

### 4. Visualization & Reporting
 - Generated specialized charts including **Streamgraphs** (to show artist evolution over time) and **Heatmaps** (to visualize daily and seasonal routines).
 - Compiled all individual analysis modules into a single **Master Jupyter Notebook** for interactive exploration.

## 📈 Key Findings (Summary)
 - **Persona:** Identified as an **'Eclectic Super-Fan'** with high loyalty to core artists but extreme exploration habits (470+ artists/month).
 - **Scale:** ~6,854 total hours of music analyzed.
 - **Behavior:** 84% of listening is "hands-off" (algorithmic flow), often occurring in massive 8-11 hour sessions.

---
