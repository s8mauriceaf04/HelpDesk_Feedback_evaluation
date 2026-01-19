import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kobo credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/awarJdfo35KbdNTePdZUyR/export-settings/esfyKiQv2LZjmuTHymN2TDw/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")

# Schema and table details
schema_name = "project"
table_name = "helpdesk_feedback_evaluation"

# Step 1: Fetch data from Kobo Toolbox
print("Fetching data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("✅ Data fetched successfully")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')

    # Clean column names
    print("Processing data...")
    df.columns = (
        df.columns.str.strip()
                  .str.replace(" ", "_")
                  .str.replace("&", "and")
                  .str.replace("-", "_")
                  .str.lower()
    )

    # Fix missing or malformed values
    numeric_columns = [
        "contact_frequency", "response_speed", "staff_knowledge",
        "explanation_clarity", "solution_effectiveness", "resolution_rating",
        "service_reliability", "staff_professionalism", "recommendation_likelihood"
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    print(df.head())

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    # Create schema
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Drop existing table
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

    # Create table
    create_table_query = f"""
    CREATE TABLE {schema_name}.{table_name} (
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        full_name TEXT,
        user_role TEXT,
        user_gender TEXT,
        contact_frequency INT,
        response_speed INT,
        resolution_timing TEXT,
        staff_knowledge INT,
        explanation_clarity INT,
        solution_effectiveness INT,
        resolution_rating INT,
        staff_professionalism INT,
        recommendation_likelihood INT
    );
    """
    cur.execute(create_table_query)

    # Insert query
    insert_query = f"""
    INSERT INTO {schema_name}.{table_name} (
        start_time,
        end_time,
        full_name,
        user_role,
        user_gender,
        contact_frequency,
        response_speed,
        resolution_timing,
        staff_knowledge,
        explanation_clarity,
        solution_effectiveness,
        resolution_rating,
        staff_professionalism,
        recommendation_likelihood
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Insert data
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("start_time"),
            row.get("end_time"),
            row.get("full_name"),
            row.get("user_role"),
            row.get("user_gender"),
            row.get("contact_frequency"),
            row.get("response_speed"),
            row.get("resolution_timing"),
            row.get("staff_knowledge"),
            row.get("explanation_clarity"),
            row.get("solution_effectiveness"),
            row.get("resolution_rating"),
            row.get("staff_professionalism"),
            row.get("recommendation_likelihood"),
        ))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data successfully loaded into PostgreSQL!")

else:
    print("❌ Failed to fetch data:", response.status_code)


