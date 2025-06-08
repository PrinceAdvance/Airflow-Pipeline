
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import the task functions
from tasks.load import load_csv_to_mysql
from tasks.validate import validate_flight_data
from tasks.transform import transform_and_compute_kpis
from tasks.export import export_kpis_to_postgres

# Define the DAG and task pipeline
with DAG(
    dag_id='flight_price_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["flight", "mysql"]
) as dag:

# The load task loads data from a CSV file into a MySQL database.
    task_load = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_mysql
    )

# The validate task checks the data quality and integrity of the loaded data.
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_flight_data
    )

# The transform task processes the data and computes key performance indicators (KPIs).
    task_transform = PythonOperator(
        task_id='transform_kpis',
        python_callable=transform_and_compute_kpis
    )

# The export task exports the computed KPIs to a PostgreSQL database for analysis.S   
    task_export = PythonOperator(
    task_id='export_to_postgres',
    python_callable=export_kpis_to_postgres
)

    # Define task dependencies
    task_load >> task_validate >> task_transform >> task_export

