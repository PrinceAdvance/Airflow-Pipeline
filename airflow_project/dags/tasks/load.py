import pandas as pd
import mysql.connector
import logging

def load_csv_to_mysql():
    logging.info("Starting CSV load task")
    conn = None
    cursor = None
    try:
        # Load and clean the CSV
        df = pd.read_csv('/opt/airflow/Flight_Price_Dataset_of_Bangladesh.csv')
        df.columns = df.columns.str.strip()  # Remove leading/trailing spaces

        # Rename columns to match DB schema
        df.rename(columns={
            'Base Fare (BDT)': 'Base Fare',
            'Tax & Surcharge (BDT)': 'Tax & Surcharge',
            'Total Fare (BDT)': 'Total Fare'
        }, inplace=True)

        logging.info(f"Loaded {len(df)} rows from CSV with columns: {df.columns.tolist()}")

        # Connect to MySQL
        conn = mysql.connector.connect(
            host='mysql', user='flight', password='flight', database='flightdb'
        )
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS flight_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Airline VARCHAR(100),
                Source VARCHAR(100),
                Destination VARCHAR(100),
                `Base Fare` FLOAT,
                `Tax & Surcharge` FLOAT,
                `Total Fare` FLOAT
            )
        """)

        # cursor.execute("DELETE FROM flight_prices")

        # Insert data row by row
        for _, row in df.iterrows():
            values = (
                row['Airline'],
                row['Source'],
                row['Destination'],
                row['Base Fare'],
                row['Tax & Surcharge'],
                row['Total Fare']
            )
            cursor.execute("""
                INSERT INTO flight_prices 
                (Airline, Source, Destination, `Base Fare`, `Tax & Surcharge`, `Total Fare`)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, values)

        conn.commit()
        logging.info("Data inserted successfully into flight_prices table.")

    except Exception as e:
        logging.error(f"Error loading CSV to MySQL: {e}", exc_info=True)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
