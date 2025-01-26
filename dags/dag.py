from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from download import download_dataset, check_and_remove_duplicates, split_dataset


with DAG(
    "data_split_dag",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    # start_date=datetime(2023, 1, 1),    
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset
    )

    split_task = PythonOperator(
        task_id="check_and_remove_duplicates",
        python_callable=check_and_remove_duplicates
    )

    split_task = PythonOperator(
        task_id="split_dataset",
        python_callable=split_dataset
    )

    download_task >> split_task 
