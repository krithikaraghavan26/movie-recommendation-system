import pandas as pd
import cx_Oracle
from datetime import datetime
import os
from werkzeug.security import generate_password_hash # Required for secure passwords
# --- ORACLE CONNECTION CONFIGURATION ---
# IMPORTANT: These should match your setup
ORACLE_USER = "system"
ORACLE_PASS = "root"
ORACLE_HOST = "localhost" 
ORACLE_PORT = "1521"       
ORACLE_SERVICE = "xepdb1" 
DSN = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
def connect_to_db():
    """Establishes and returns an Oracle database connection."""
    try:
        conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=DSN)
        return conn
    except Exception as e:
        print(f"ERROR: Database connection failed. Check your credentials and Oracle setup.")
        print(f"Details: {e}")
        return None
def preprocess_and_save_data(df):
    """ 
    Splits and normalizes the single dataframe:
    1. Generates clean, sequential user_id (to fix PK violation).
    2. Hashes a default password for the new column.
    3. Maps reviews to the new clean user_id and movie_id (to fix FK violation).
    """
    print("Preprocessing data for insertion (Normalizing keys and hashing passwords)...")
    # --- 1. USERS Table Data (Creating a new, guaranteed unique PK: clean_user_id) ---
    users_df_raw = df[['user_id', 'name', 'email']].drop_duplicates().reset_index(drop=True)
    users_df_raw['clean_user_id'] = users_df_raw.index + 1
    # Generate and Hash a default password
    DEFAULT_PASSWORD = "password" 
    hashed_password = generate_password_hash(DEFAULT_PASSWORD, method='pbkdf2:sha256')
    users_df_raw['password_hash'] = hashed_password
    users_to_load = users_df_raw[['clean_user_id', 'name', 'email', 'password_hash']].copy()
    # --- 2. MOVIES Table Data ---
    movies_df = df[['title', 'genre', 'release_year', 'director']].drop_duplicates().reset_index(drop=True)
    movies_df.insert(0, 'movie_id', movies_df.index + 1)
    # --- 3. REVIEWS Table Data (Mapping to new FKs) ---
    # Merge with USERS mapping to link reviews to the new clean_user_id (PK of USERS)
    reviews_df_temp = pd.merge(df, users_df_raw[['user_id', 'name', 'email', 'clean_user_id']], on=['user_id', 'name', 'email'], how='left')
    # Merge with MOVIES to link reviews to movie_id (PK of MOVIES)
    reviews_df_temp = pd.merge(reviews_df_temp, movies_df[['movie_id', 'title']], on='title', how='left')
    # Select final columns for REVIEWS table
    reviews_to_load = reviews_df_temp[[
        'clean_user_id', 'movie_id', 'rating', 'review_text', 'review_date'
    ]].reset_index()
    reviews_to_load.insert(0, 'review_id', reviews_to_load.index + 1)
    reviews_to_load = reviews_to_load.drop(columns=['index']).rename(columns={'clean_user_id': 'user_id'})
    print(f"- Cleaned data sizes: USERS={len(users_to_load)}, MOVIES={len(movies_df)}, REVIEWS={len(reviews_to_load)}")
    return users_to_load, movies_df, reviews_to_load
def load_data_to_oracle():
    """Reads transformed data and loads into Oracle."""
    try:
        # Load the single source file using the corrected encoding
        df_source = pd.read_csv('dataset_dbms.csv', encoding='latin-1') 
    except Exception as e:
        print(f"ERROR: Could not read dataset_dbms.csv. Details: {e}")
        return
    users_df, movies_df, reviews_df = preprocess_and_save_data(df_source)
    conn = connect_to_db()
    if not conn:
        return
    cursor = conn.cursor()
    print("\nStarting data load into Oracle tables...")
    # --- 1. Load USERS data ---
    try:
        # Columns: user_id (new PK), name, email, password_hash
        user_data = [tuple(row) for row in users_df.itertuples(index=False)]
        print(f"Inserting {len(user_data)} rows into USERS...")
        # Add :4 for the password_hash
        cursor.executemany("INSERT INTO USERS (user_id, name, email, password_hash) VALUES (:1, :2, :3, :4)", user_data)
        print("USERS table loaded successfully.")
    except Exception as e:
        print(f"Error loading USERS: {e}")
        conn.rollback()
        return
    # --- 2. Load MOVIES data ---
    try:
        # Columns: movie_id (PK), title, genre, release_year, director
        movie_data = [tuple(row) for row in movies_df.itertuples(index=False)]
        print(f"Inserting {len(movie_data)} rows into MOVIES...")
        cursor.executemany("INSERT INTO MOVIES (movie_id, title, genre, release_year, director) VALUES (:1, :2, :3, :4, :5)", movie_data)
        print("MOVIES table loaded successfully.")
    except Exception as e:
        print(f"Error loading MOVIES: {e}")
        conn.rollback()
        return
    # --- 3. Load REVIEWS data ---
    try:
        review_data = []
        for index, row in reviews_df.iterrows():
            # Convert date string ('DD-MM-YYYY') to Python datetime object 
            date_obj = datetime.strptime(row['review_date'], '%d-%m-%Y')
            review_data.append((
                row['review_id'],
                row['user_id'], # This is the NEW clean_user_id FK
                row['movie_id'],
                row['rating'],
                row['review_text'],
                date_obj
            ))
        print(f"Inserting {len(review_data)} rows into REVIEWS...")
        # Columns: review_id, user_id (FK), movie_id (FK), rating, review_text, review_date
        cursor.executemany("""
            INSERT INTO REVIEWS (review_id, user_id, movie_id, rating, review_text, review_date) 
            VALUES (:1, :2, :3, :4, :5, :6)
        """, review_data)
        print("REVIEWS table loaded successfully.")
    except Exception as e:
        print(f"Error loading REVIEWS: {e}")
        conn.rollback()
        return
    # Final Commit
    try:
        conn.commit()
        print("\nAll data committed successfully to the database. You can now run 'app.py'.")
    except Exception as e:
        print(f"Final commit failed: {e}")
        conn.rollback()
    cursor.close()
    conn.close()
if __name__ == '__main__':
    # Ensure you've re-run the DDL to include the 'password_hash' column!
    load_data_to_oracle()