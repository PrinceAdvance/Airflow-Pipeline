import pandas as pd
import mysql.connector
import logging

def transform_and_compute_kpis():
    logging.info("Starting KPI transformation task")
    conn = None
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='mysql', user='flight', password='flight', database='flightdb'
        )
        
        # Load data into a pandas DataFrame
        query = "SELECT * FROM flight_prices"
        df = pd.read_sql(query, conn)

        logging.info(f"Loaded {len(df)} rows for KPI computation")

        # Ensure date column exists and is datetime
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        else:
            logging.warning("No 'Date' column found; skipping seasonal KPI")
            df['Date'] = pd.NaT  # just to avoid crashing

        # 1. Average fare per airline
        avg_fare_per_airline = df.groupby('Airline')['Total Fare'].mean().reset_index()
        logging.info("📊 Average Total Fare per Airline:\n%s", avg_fare_per_airline.to_string(index=False))

        # 2. Most frequent routes
        most_common_routes = df.groupby(['Source', 'Destination']).size().reset_index(name='Flight Count')
        logging.info("✈️ Most Frequent Routes:\n%s", most_common_routes.sort_values(by='Flight Count', ascending=False).head(5).to_string(index=False))

        # 3. Top destinations
        top_destinations = df['Destination'].value_counts().head(5).reset_index()
        top_destinations.columns = ['Destination', 'Flight Count']
        logging.info("🏆 Top 5 Destinations:\n%s", top_destinations.to_string(index=False))

        # 4. Booking count by airline
        booking_count = df['Airline'].value_counts().reset_index()
        booking_count.columns = ['Airline', 'Booking Count']
        logging.info("🛫 Booking Count by Airline:\n%s", booking_count.to_string(index=False))

        # 5. Seasonal fare variation
        # Eid & Winter holiday peak windows 
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day

        def is_peak(row):
            return (
                (row['Month'] == 4 and 15 <= row['Day'] <= 30) or  # Eid season 
                (row['Month'] == 12) or                            # Winter holidays
                (row['Month'] == 1)                                # Early January holiday traffic
            )

        df['is_peak_season'] = df.apply(is_peak, axis=1)
        seasonal_kpis = df.groupby('is_peak_season')['Total Fare'].mean().reset_index()
        seasonal_kpis.columns = ['Peak Season', 'Avg Total Fare']
        seasonal_kpis['Peak Season'] = seasonal_kpis['Peak Season'].map({True: 'Peak', False: 'Off-Peak'})

        logging.info("🌤 Seasonal Fare Variation:\n%s", seasonal_kpis.to_string(index=False))

        logging.info("✅ KPI computation completed successfully")

    except Exception as e:
        logging.error(f"❌ KPI transformation failed: {e}", exc_info=True)
        raise
    finally:
        if conn: conn.close()

