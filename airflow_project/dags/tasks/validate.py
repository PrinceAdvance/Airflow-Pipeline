import mysql.connector
import logging

def validate_flight_data():
    logging.info("Starting data validation task")
    try:
        conn = mysql.connector.connect(
            host='mysql', user='flight', password='flight', database='flightdb'
        )
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM flight_prices")
        rows = cursor.fetchall()
        logging.info(f"Fetched {len(rows)} rows for validation")

        required_columns = ['Airline', 'Source', 'Destination', 'Base Fare', 'Tax & Surcharge', 'Total Fare']

        for row in rows:
            for col in required_columns:
                if row[col] is None or (isinstance(row[col], str) and row[col].strip() == ''):
                    raise ValueError(f"Missing or empty value in column '{col}'")

            if float(row['Base Fare']) < 0 or float(row['Tax & Surcharge']) < 0 or float(row['Total Fare']) < 0:
                raise ValueError(f"Negative fare found in row: {row}")

        logging.info("Data validation passed successfully")
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
