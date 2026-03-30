import polars as pl
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os

# Define path
PARQUET_PATH = "/Users/nidhi/Developer/spotify_lakehouse/spotify_history.parquet"

print(">>> Initializing the Manual Vibe Search (TF-IDF)...")

# 1. Load the unique track names
# (We only need unique tracks to build the search engine)
df = pl.read_parquet(PARQUET_PATH)
unique_tracks = (
    df
    .select(["track_name", "artist_name"])
    .unique()
    .with_columns(
        (pl.col("track_name") + " " + pl.col("artist_name")).alias("search_text")
    )
)

print(f">>> Processing {len(unique_tracks):,} unique tracks into a Semantic Matrix...")

# 2. Build the TF-IDF Vectorizer
# This turns text into a large "Matrix" where each number represents a word's importance.
vectorizer = TfidfVectorizer(stop_words='english')
tfidf_matrix = vectorizer.fit_transform(unique_tracks["search_text"])

def search_vibe(query_text, top_n=5):
    # Turn the user's query into the same vector format
    query_vector = vectorizer.transform([query_text])
    
    # Calculate "Cosine Similarity" (How close is the query to each track in the matrix?)
    similarities = cosine_similarity(query_vector, tfidf_matrix).flatten()
    
    # Get the top indices
    top_indices = similarities.argsort()[-top_n:][::-1]
    
    # Return the results
    results = unique_tracks.select(["track_name", "artist_name"])[top_indices]
    return results

# 3. Example Search
query = "late night romantic bts"
print(f"\n🔍 Searching for: '{query}'")
results = search_vibe(query)
print(results)

query2 = "punjabi hit remix"
print(f"\n🔍 Searching for: '{query2}'")
results2 = search_vibe(query2)
print(results2)

query3 = "bollywood movie themes"
print(f"\n🔍 Searching for: '{query3}'")
results3 = search_vibe(query3)
print(results3)
