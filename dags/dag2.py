from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler

# Google Sheets URL i zakres
SHEET_URL = "https://docs.google.com/spreadsheets/d/your-sheet-id"
SHEET_NAME = "Sheet1"
CLEANED_FILE_PATH = "/Users/andrzej/airflow/dags/files/cleaned_data.csv"
STANDARDIZED_FILE_PATH = "/Users/andrzej/airflow/dags/files/standardized_data.csv"
NORMALIZED_FILE_PATH = "/Users/andrzej/airflow/dags/files/normalized_data.csv"


def authenticate_google_sheets():
    creds = Credentials.from_service_account_file(
        "path/to/credentials.json",  
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    client = gspread.authorize(creds)
    return client

def download_google_sheet():
    client = authenticate_google_sheets()
    sheet = client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df.to_csv(CLEANED_FILE_PATH, index=False)
    print(f"Data downloaded from Google Sheets and saved to {CLEANED_FILE_PATH}")


def clean_data():
    df = pd.read_csv(CLEANED_FILE_PATH)
    
    
    df = df.dropna()

    df.to_csv(CLEANED_FILE_PATH, index=False)
    print(f"Data cleaned and saved to {CLEANED_FILE_PATH}")


def standardize_data():
    df = pd.read_csv(CLEANED_FILE_PATH)
    scaler = StandardScaler()
    df_standardized = scaler.fit_transform(df.select_dtypes(include=["float64", "int64"]))
    
    df_standardized = pd.DataFrame(df_standardized, columns=df.select_dtypes(include=["float64", "int64"]).columns)
    df[df_standardized.columns] = df_standardized
    df.to_csv(STANDARDIZED_FILE_PATH, index=False)
    print(f"Data standardized and saved to {STANDARDIZED_FILE_PATH}")


def normalize_data():
    df = pd.read_csv(CLEANED_FILE_PATH)
    scaler = MinMaxScaler()
    df_normalized = scaler.fit_transform(df.select_dtypes(include=["float64", "int64"]))
    
    df_normalized = pd.DataFrame(df_normalized, columns=df.select_dtypes(include=["float64", "int64"]).columns)
    df[df_normalized.columns] = df_normalized
    df.to_csv(NORMALIZED_FILE_PATH, index=False)
    print(f"Data normalized and saved to {NORMALIZED_FILE_PATH}")


with DAG(
    "google_sheets_data_processing",
    default_args={"owner": "airflow"},
    schedule_interval=None,
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_google_sheet",
        python_callable=download_google_sheet
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    standardize_task = PythonOperator(
        task_id="standardize_data",
        python_callable=standardize_data
    )

    normalize_task = PythonOperator(
        task_id="normalize_data",
        python_callable=normalize_data
    )

    download_task >> clean_task >> [standardize_task, normalize_task]
