# processor.py
import pandas as pd

# Load datasets
df_metadata = pd.read_csv('movies_metadata.csv', low_memory=False)
df_credits = pd.read_csv('credits.csv')
df_keywords = pd.read_csv('keywords.csv')
df_links = pd.read_csv('links.csv')

# Load ratings in chunks to reduce memory usage
ratings_chunks = pd.read_csv('ratings.csv', chunksize=500000, low_memory=False)
df_ratings = pd.concat(ratings_chunks, ignore_index=True)

# Convert IDs to numeric for consistency across dataframes
df_metadata['id'] = pd.to_numeric(df_metadata['id'], errors='coerce')
df_links['tmdbId'] = pd.to_numeric(df_links['tmdbId'], errors='coerce')
df_ratings['movieId'] = pd.to_numeric(df_ratings['movieId'], errors='coerce')
df_keywords['id'] = pd.to_numeric(df_keywords['id'], errors='coerce')
df_credits['id'] = pd.to_numeric(df_credits['id'], errors='coerce')

# Merge dataframes in sequence
merged_df = pd.merge(df_metadata, df_credits, left_on='id', right_on='id', how='inner')
merged_df = pd.merge(merged_df, df_keywords, left_on='id', right_on='id', how='inner')
merged_df = pd.merge(merged_df, df_links, left_on='id', right_on='tmdbId', how='inner')
final_df = pd.merge(merged_df, df_ratings, left_on='movieId', right_on='movieId', how='inner')

# Save the processed dataframe to a CSV to use in Streamlit
final_df.to_csv('merged_data.csv', index=False)

