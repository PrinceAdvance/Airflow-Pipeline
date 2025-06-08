# 🛫 Flight Price Analysis Pipeline - Documentation

## 📦 1. Pipeline Architecture & Execution Flow

### 🔧 Tools Used:

* **Apache Airflow**: DAG scheduling and orchestration
* **MySQL**: Raw data storage
* **PostgreSQL**: Final KPI storage
* **Pandas**: Data transformation & KPI computation
* **Docker**: Containerization and environment management

### ⚙️ Docker-Based Architecture:

```text
[CSV File] → [Airflow (Docker)]
                ├── load_csv_to_mysql (Task)
                │       ↓
                ├── validate_data
                │       ↓
                ├── transform_kpis
                │       ↓
                └── export_to_postgres

Containers:
- airflow (Scheduler + Webserver)
- mysql (Stores raw flight data)
- postgres (Stores computed KPIs)
```

---

## 📋 2. Airflow DAG & Task Descriptions

| Task ID              | Description                                                     |
| -------------------- | --------------------------------------------------------------- |
| `load_csv`           | Loads the CSV file into MySQL (`flight_prices` table).          |
| `validate_data`      | Validates rows for missing/invalid fare values.                 |
| `transform_kpis`     | Computes KPIs: fare averages, booking counts, seasonal metrics. |
| `export_to_postgres` | Exports KPI results to PostgreSQL (`flight_prices_kpis` table). |

---

## 📊 3. KPI Definitions & Computation Logic

| KPI                                | Description                                | Logic                                          |
| ---------------------------------- | ------------------------------------------ | ---------------------------------------------- |
| **Average Total Fare per Airline** | Average `Total Fare` grouped by `Airline`. | `df.groupby('Airline')['Total Fare'].mean()`   |
| **Most Frequent Routes**           | Top (Source → Destination) pairs.          | `df.groupby(['Source', 'Destination']).size()` |
| **Top 5 Destinations**             | Most common destinations.                  | `df['Destination'].value_counts().head(5)`     |
| **Booking Count by Airline**       | Count of records per airline.              | `df['Airline'].value_counts()`                 |
| **Seasonal Fare Variation**        | Avg fare during Eid/winter vs. off-peak.   | Filter on `Date` column by month/day           |

### 🗓 Peak Season Logic:

```python
(row['Month'] == 4 and 15 <= row['Day'] <= 30)  # Eid
(row['Month'] == 12 or row['Month'] == 1)       # Winter
```

---

## ⚠️ 4. Challenges & Resolutions

| Challenge                           | Resolution                                                           |
| ----------------------------------- | -------------------------------------------------------------------- |
| **CSV file not found in container** | Used Docker volume mapping in `docker-compose.yaml`.                 |
| **DAG import errors**               | Set proper `PYTHONPATH` and `tasks/` structure.                      |
| **MySQL insert error**              | Used named tuple values instead of `tuple(row)`.                     |
| **KeyError on columns**             | Cleaned with `df.columns.str.strip()` and renamed columns.           |
| **Airflow scheduler idle**          | Ensured `scheduler & webserver` run in one command inside container. |
| **PostgreSQL export failure**       | Added missing `mysql.connector` import and used SQLAlchemy.          |

---

## 🚀 Future Enhancements

* Add email/Slack alerts on task failures
* Use `XCom` to pass data between tasks
* Store time-stamped KPI snapshots in PostgreSQL
* Add a Superset/Metabase dashboard for KPI visualization
* Archive raw and cleaned datasets with version control

---

