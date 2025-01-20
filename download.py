import requests
import pandas as pd
from sklearn.model_selection import train_test_split


DATASET_URL = "https://vincentarelbundock.github.io/Rdatasets/csv/AER/CollegeDistance.csv"
CSV_FILE_PATH = "/Users/andrzej/Informatyka/PRO/Tutor5_s26442/airflow/dags/files/CollegeDistance.csv"
TRAIN_FILE_PATH = "/Users/andrzej/Informatyka/PRO/Tutor5_s26442/airflow/dags/files/train.csv"
TEST_FILE_PATH = "/Users/andrzej/Informatyka/PRO/Tutor5_s26442/airflow/dags/files/test.csv"


def download_dataset():
    response = requests.get(DATASET_URL)
    with open(CSV_FILE_PATH, "wb") as file:
        file.write(response.content)
    print(f"Dataset downloaded to {CSV_FILE_PATH}")

def split_dataset():
    df = pd.read_csv(CSV_FILE_PATH)
    train, test = train_test_split(df, test_size=0.3, random_state=42)
    train.to_csv(TRAIN_FILE_PATH, index=False)
    test.to_csv(TEST_FILE_PATH, index=False)
    print(f"Train dataset saved to {TRAIN_FILE_PATH}")
    print(f"Test dataset saved to {TEST_FILE_PATH}")
