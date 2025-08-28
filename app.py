import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text


MYSQL_USER = "root"
MYSQL_PASSWORD = "venkat"
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "cdta_db"

TRAFFIC_STOPS_TABLE = "traffic_stops"

@st.cache_resource
def get_db_connection():
    db_connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
    )
    engine = create_engine(db_connection_str)
    return engine

def fetch_data(query, params=None):
    engine = get_db_connection()
    with engine.connect() as connection:
        if params is not None and not isinstance(params, (tuple, dict)):
            params = tuple(params)
        df = pd.read_sql(query, connection, params=params)
    return df

INSIGHTS = {
    "Top 10 Drug-Related Vehicles": f"""
        SELECT vehicle_number, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE drugs_related_stop = TRUE AND vehicle_number != 'Unknown'
        GROUP BY vehicle_number
        ORDER BY stop_count DESC
        LIMIT 10;
    """,
    "Most Frequently Searched Vehicles": f"""
        SELECT vehicle_number, COUNT(*) as search_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE search_conducted = TRUE AND vehicle_number != 'Unknown'
        GROUP BY vehicle_number
        ORDER BY search_count DESC
        LIMIT 10;
    """,
    "Driver Age Group with Highest Arrest Rate": f"""
        SELECT
            CASE
                WHEN driver_age BETWEEN 15 AND 24 THEN '15-24'
                WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
                WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
                WHEN driver_age BETWEEN 45 AND 54 THEN '45-54'
                WHEN driver_age BETWEEN 55 AND 64 THEN '55-64'
                WHEN driver_age > 64 THEN '65+'
                ELSE 'Unknown'
            END as age_group,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_age > 0
        GROUP BY age_group
        ORDER BY arrest_rate DESC;
    """,
    "Gender Distribution of Drivers Stopped by Country": f"""
        SELECT country_name, driver_gender, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND driver_gender != 'Unknown'
        GROUP BY country_name, driver_gender
        ORDER BY country_name, driver_gender;
    """,
    "Race and Gender Combination with Highest Search Rate": f"""
        SELECT
            driver_race,
            driver_gender,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_race != 'Unknown' AND driver_gender != 'Unknown'
        GROUP BY driver_race, driver_gender
        ORDER BY search_rate DESC
        LIMIT 10;
    """,
    "Time of Day with Most Traffic Stops": f"""
        SELECT
            HOUR(stop_time) AS hour_of_day,
            COUNT(*) AS stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        GROUP BY hour_of_day
        ORDER BY stop_count DESC;
    """,
    "Average Stop Duration for Different Violations": f"""
        SELECT violation, AVG(
            CASE stop_duration
                WHEN '0-15 Min' THEN 7.5
                WHEN '16-30 Min' THEN 23
                WHEN '30+ Min' THEN 45
                ELSE 0
            END
        ) as average_duration_minutes
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY average_duration_minutes DESC;
    """,
    "Night Stops More Likely to Lead to Arrests?": f"""
        SELECT
            CASE
                WHEN HOUR(stop_time) >= 20 OR HOUR(stop_time) < 6
                THEN 'Night'
                ELSE 'Day'
            END as time_of_day_category,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        GROUP BY time_of_day_category
        ORDER BY arrest_rate DESC;
    """,
    "Violations Most Associated with Searches or Arrests": f"""
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY search_rate DESC, arrest_rate DESC;
    """,
    "Violations Most Common Among Younger Drivers (<25)": f"""
        SELECT violation, COUNT(*) as stop_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE driver_age > 0 AND driver_age < 25 AND violation != 'Unknown'
        GROUP BY violation
        ORDER BY stop_count DESC
        LIMIT 10;
    """,
    "Violation That Rarely Results in Search or Arrest": f"""
        SELECT
            violation,
            (SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as search_rate,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        HAVING search_rate < 5 AND arrest_rate < 5
        ORDER BY search_rate ASC, arrest_rate ASC
        LIMIT 5;
    """,
    "Countries with Highest Rate of Drug-Related Stops": f"""
        SELECT country_name,
               (SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as drug_related_stop_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown'
        GROUP BY country_name
        ORDER BY drug_related_stop_rate DESC
        LIMIT 10;
    """,
    "Arrest Rate by Country and Violation": f"""
        SELECT country_name, violation,
               (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND violation != 'Unknown'
        GROUP BY country_name, violation
        HAVING COUNT(*) > 10
        ORDER BY country_name, arrest_rate DESC;
    """,
    "Country with Most Stops with Search Conducted": f"""
        SELECT country_name, COUNT(*) as search_conducted_stops_count
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE search_conducted = TRUE AND country_name != 'Unknown'
        GROUP BY country_name
        ORDER BY search_conducted_stops_count DESC
        LIMIT 5;
    """,
    "Yearly Breakdown of Stops and Arrests by Country": f"""
        SELECT
            YEAR(stop_date) AS stop_year,
            country_name,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS arrest_rate_percentage
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND stop_date IS NOT NULL
        GROUP BY stop_year, country_name
        ORDER BY stop_year, country_name;
    """,
    "Driver Violation Trends Based on Age and Race": f"""
        SELECT
            ts.driver_race,
            ts.driver_age,
            ts.violation,
            COUNT(*) AS violation_count
        FROM {TRAFFIC_STOPS_TABLE} AS ts
        WHERE ts.driver_race != 'Unknown' AND ts.violation != 'Unknown' AND ts.driver_age > 0
        GROUP BY ts.driver_race, ts.driver_age, ts.violation
        ORDER BY ts.driver_race, ts.driver_age, violation_count DESC
        LIMIT 20;
    """,
    "Time Period Analysis of Stops (Year, Month, Hour)": f"""
        SELECT
            YEAR(stop_date) AS stop_year,
            MONTH(stop_date) AS stop_month,
            HOUR(stop_time) AS stop_hour,
            COUNT(*) AS number_of_stops
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE stop_date IS NOT NULL AND stop_time IS NOT NULL
        GROUP BY stop_year, stop_month, stop_hour
        ORDER BY stop_year, stop_month, stop_hour;
    """,
    "Violations with High Search and Arrest Rates": f"""
        WITH ViolationStats AS (
            SELECT
                violation,
                COUNT(*) AS total_stops,
                SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
                SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests
            FROM {TRAFFIC_STOPS_TABLE}
            WHERE violation != 'Unknown'
            GROUP BY violation
        )
        SELECT
            violation,
            total_stops,
            total_searches,
            total_arrests,
            (total_searches * 100.0 / total_stops) AS search_rate_percentage,
            (total_arrests * 100.0 / total_stops) AS arrest_rate_percentage
        FROM ViolationStats
        WHERE total_stops > 50
        ORDER BY search_rate_percentage DESC, arrest_rate_percentage DESC
        LIMIT 10;
    """,
    "Driver Demographics by Country (Age, Gender, and Race)": f"""
        SELECT
            country_name,
            driver_gender,
            driver_race,
            COUNT(*) AS total_stops,
            AVG(driver_age) AS average_driver_age
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE country_name != 'Unknown' AND driver_gender != 'Unknown' AND driver_race != 'Unknown' AND driver_age > 0
        GROUP BY country_name, driver_gender, driver_race
        ORDER BY country_name, total_stops DESC
        LIMIT 20;
    """,
    "Top 5 Violations with Highest Arrest Rates": f"""
        SELECT
            violation,
            (SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as arrest_rate
        FROM {TRAFFIC_STOPS_TABLE}
        WHERE violation != 'Unknown'
        GROUP BY violation
        ORDER BY arrest_rate DESC
        LIMIT 5;
    """
}

st.set_page_config(layout="wide", page_title="SecureCheck Police Post Logs")

st.title("🚓Police checkPost Logs Dashboard")
st.markdown("---")

st.sidebar.header("Access Panel")
page = st.sidebar.radio("Go to", ["Dashboard Overview", "Analytics & Reports"])
st.sidebar.markdown("---")
st.sidebar.info("This dashboard provides basic insights into traffic stop data.")

if page == "Dashboard Overview":
    st.header("Dashboard Overview: Recent Activity")
    recent_logs_query = f"SELECT * FROM {TRAFFIC_STOPS_TABLE} ORDER BY stop_date DESC, stop_time DESC LIMIT 20;"
    recent_df = fetch_data(recent_logs_query)

    if not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("No recent traffic stop data available. Check database connection and data.")

    st.markdown("---")

    st.header("Key Statistics")
    col1, col2, col3 = st.columns(3)

    total_stops_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE};")
    total_stops = total_stops_df.iloc[0,0] if not total_stops_df.empty else 0
    col1.metric("Total Stops Recorded", total_stops)

    total_arrests_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE} WHERE is_arrested = TRUE;")
    total_arrests = total_arrests_df.iloc[0,0] if not total_arrests_df.empty else 0
    col2.metric("Total Arrests", total_arrests)

    total_searches_df = fetch_data(f"SELECT COUNT(*) FROM {TRAFFIC_STOPS_TABLE} WHERE search_conducted = TRUE;")
    total_searches = total_searches_df.iloc[0,0] if not total_searches_df.empty else 0
    col3.metric("Total Searches Conducted", total_searches)

    st.markdown("---")

    st.header("Interactive Data Visualization")

    violation_counts = fetch_data(INSIGHTS["Top 10 Drug-Related Vehicles"])
    if not violation_counts.empty:
        st.subheader("Top 10 Drug-Related Vehicles")
        st.bar_chart(violation_counts.set_index('vehicle_number')['stop_count'])
    else:
        st.info("No data for top drug-related vehicles. Check database data.")

    country_counts = fetch_data(INSIGHTS["Countries with Highest Rate of Drug-Related Stops"])
    if not country_counts.empty:
        st.subheader("Countries with Highest Rate of Drug-Related Stops")
        st.bar_chart(country_counts.set_index('country_name')['drug_related_stop_rate'])
    else:
        st.info("No country data to display. Check database data.")

elif page == "Analytics & Reports":
    st.header("📈 Analytics & Reports")
    st.write("STATISTICAL INSIGHTS")

    selected_query_name = st.selectbox("Data selector", list(INSIGHTS.keys()))
    query_to_run = INSIGHTS[selected_query_name]
    
    if st.button(f"Run {selected_query_name} Report"):
        st.markdown("---")
        st.subheader(f"Results for: {selected_query_name}")
        report_df = fetch_data(query_to_run)
        if not report_df.empty:
            st.dataframe(report_df, use_container_width=True)
            
        else:
            st.info("No data found for this report.")