from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import requests
import pandas as pd
from sklearn.model_selection import train_test_split


DATASET_URL = "https://vincentarelbundock.github.io/Rdatasets/csv/AER/CollegeDistance.csv"
CSV_FILE_PATH = "/Users/andrzej/airflow/dags/files/CollegeDistance.csv"
TRAIN_FILE_PATH = "/Users/andrzej/airflow/dags/files/train.csv"
TEST_FILE_PATH = "/Users/andrzej/airflow/dags/files/test.csv"


def download_dataset():
    response = requests.get(DATASET_URL)
    with open(CSV_FILE_PATH, "wb") as file:
        file.write(response.content)
    print(f"Dataset downloaded to {CSV_FILE_PATH}")

def check_and_remove_duplicates():
    df = pd.read_csv(CSV_FILE_PATH)

    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        print(f"Found {num_duplicates} duplicates. Removing them.")
        df = df.drop_duplicates()
        df.to_csv(CSV_FILE_PATH, index=False)
    else:
        print("No duplicates found.")

    print(f"Data processed and saved to {CSV_FILE_PATH}")

def split_dataset():
    df = pd.read_csv(CSV_FILE_PATH)
    train, test = train_test_split(df, test_size=0.3, random_state=42)
    train.to_csv(TRAIN_FILE_PATH, index=False)
    test.to_csv(TEST_FILE_PATH, index=False)
    print(f"Train dataset saved to {TRAIN_FILE_PATH}")
    print(f"Test dataset saved to {TEST_FILE_PATH}")



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

    check_and_remove_duplicates_task = PythonOperator(
        task_id="check_and_remove_duplicates",
        python_callable=check_and_remove_duplicates
    )

    split_task = PythonOperator(
        task_id="split_dataset",
        python_callable=split_dataset
    )

    download_task >> check_and_remove_duplicates_task >> split_task 
