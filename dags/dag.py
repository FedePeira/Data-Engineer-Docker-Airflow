from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from modules.anonymize import DataRetriever
from modules.data_con import load_data

default_args = {
    'owner': 'federico_peirano',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': days_ago(1),
}

dag_id = "postgres_operator_dag"
description = "Dag conecction to postgres"
schedule_interval = "@once"

dag = DAG(
    dag_id=dag_id,
    description=description,
    schedule_interval=schedule_interval,
    default_args=default_args,
)

# Task: Retrieve data from the API
retrieve_data = DataRetriever()
data = retrieve_data.retrieve_data()

# Task: Anonymize the retrieved data
anonymized_data = retrieve_data.anonymize_data()

load_data_task = PythonOperator(
    task_id="load_data_to_redshift",
    python_callable=load_data,
    op_kwargs={"df": anonymized_data},
    dag=dag,
)

retrieve_data >> load_data_task