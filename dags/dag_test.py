from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def test_dag():
    assert dag.dag_id == "dag_test"
    assert dag.schedule_interval == None
    assert len(dag.tasks) == 3
    assert init_project.task_id == "init_project_data_creation"
    assert airflow.__name__ == "airflow"
    assert finish_dag.__name__ == "finish_dag"

with DAG(
    dag_id="dag_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,  
    doc_md="prueba dag"
    ) as dag:
    
    init_project = BashOperator(
        task_id="init_project_data_creation",
        bash_command="echo 'start project'")

    @task()
    def airflow():
        print("airflow")

    @task()
    def finish_dag():
        print("dag finish")

    test_task = PythonOperator(
        task_id="test_dag",
        python_callable=test_dag)

    init_project >> airflow() >> finish_dag() >> test_task
