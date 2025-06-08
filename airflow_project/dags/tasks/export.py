import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
import mysql.connector


def export_kpis_to_postgres():
    logging.info("Starting export of KPIs to PostgreSQL")

    try:
        # Connect to MySQL to fetch data again (or pass it from previous step in more advanced DAG)
        mysql_conn = mysql.connector.connect(
            host='mysql', user='flight', password='flight', database='flightdb'
        )
        df = pd.read_sql("SELECT * FROM flight_prices", con=mysql_conn)
        mysql_conn.close()

        # Recompute summary KPIs (you can optionally pass them from transform step)
        avg_fare = df.groupby('Airline')['Total Fare'].mean().reset_index()
        avg_fare.columns = ['airline', 'avg_total_fare']

        # Connect to PostgreSQL using SQLAlchemy
        postgres_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        engine = create_engine(postgres_url)

        # Write to PostgreSQL (replace if table exists)
        avg_fare.to_sql('flight_prices_kpis', engine, if_exists='replace', index=False)
        logging.info("✅ Export to PostgreSQL successful")

    except Exception as e:
        logging.error(f"❌ Failed to export KPIs to PostgreSQL: {e}", exc_info=True)
        raise
