import pandas as pd
from sqlalchemy import create_engine, text

MYSQL_USER = "root"
MYSQL_PASSWORD = "venkat"
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "cdta_db"

TABLE_NAME = "traffic_stops"

def clean_data(df):
    df_cleaned = df.dropna(axis=1, how='all')

    if 'driver_age_raw' in df_cleaned.columns:
        df_cleaned['driver_age_raw'] = df_cleaned['driver_age_raw'].fillna(-1).astype(int)
    if 'driver_age' in df_cleaned.columns:
        df_cleaned['driver_age'] = df_cleaned['driver_age'].fillna(-1).astype(int)

    categorical_cols = [
        'country_name', 'driver_gender', 'driver_race',
        'violation_raw', 'violation', 'search_type', 'stop_outcome',
        'stop_duration', 'vehicle_number'
    ]
    for col in categorical_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].fillna('Unknown')

    boolean_cols = ['search_conducted', 'is_arrested', 'drugs_related_stop']
    for col in boolean_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].fillna(False).astype(bool)

    if 'stop_date' in df_cleaned.columns:
        df_cleaned['stop_date'] = pd.to_datetime(df_cleaned['stop_date'], errors='coerce')
        df_cleaned.dropna(subset=['stop_date'], inplace=True)
    if 'stop_time' in df_cleaned.columns:
        df_cleaned['stop_time'] = df_cleaned['stop_time'].astype(str)

    return df_cleaned

def create_and_populate_db(df):
    db_connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
    )
    engine = create_engine(db_connection_str)

    temp_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}")
    with temp_engine.connect() as temp_conn:
        temp_conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE};"))
        temp_conn.commit()

    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME};"))
        connection.commit()

        create_table_with_indexes_query = f"""
        CREATE TABLE {TABLE_NAME} (
            stop_date DATE,
            stop_time VARCHAR(8),
            country_name VARCHAR(255),
            driver_gender VARCHAR(50),
            driver_age_raw INT,
            driver_age INT,
            driver_race VARCHAR(50),
            violation_raw VARCHAR(255),
            violation VARCHAR(255),
            search_conducted BOOLEAN,
            search_type VARCHAR(255),
            stop_outcome VARCHAR(50),
            is_arrested BOOLEAN,
            stop_duration VARCHAR(50),
            drugs_related_stop BOOLEAN,
            vehicle_number VARCHAR(255),
            INDEX idx_stop_date (stop_date),
            INDEX idx_violation (violation),
            INDEX idx_vehicle_number (vehicle_number),
            INDEX idx_drugs_related_stop (drugs_related_stop),
            INDEX idx_driver_age (driver_age),
            INDEX idx_is_arrested (is_arrested),
            INDEX idx_country_name (country_name),
            INDEX idx_driver_gender (driver_gender),
            INDEX idx_driver_race (driver_race)
        );
        """
        connection.execute(text(create_table_with_indexes_query))
        connection.commit()

        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists='append',
            index=False,
        )

        result = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};"))
        count = result.scalar()
        
        result = connection.execute(text(f"SELECT * FROM {TABLE_NAME} LIMIT 5;"))
        for i, row in enumerate(result):
            pass

if __name__ == "__main__":
    df = pd.read_csv("traffic_stops.csv")
    cleaned_df = clean_data(df.copy())
    create_and_populate_db(cleaned_df)