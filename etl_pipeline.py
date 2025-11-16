import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaErrors
from datetime import datetime
import os
import json

PATHS = {
    "csv": "data/input/users_raw.csv",
    "trusted_dir" : "data/trusted",
    "quarantine_dir": "data/quarantine",
    "alert_dir": "data/alerts"
}

def extract_Dataframe_From_CSV(path):
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Error loading data: {e}")

def lowercase_Dataframe_Column(dataframe, column):
    if column not in dataframe.columns:
        raise ValueError(f"lowercase_Dataframe_Column: {column} is not in dataframe columns")
    dataframe[column] = dataframe[column].astype('string').str.lower()
    dataframe[column] = dataframe[column].replace("", pd.NA)
    return dataframe
def replace_Dataframe_Column_Not_Numeric_To_NULL(dataframe, column):
    if column not in dataframe.columns:
        raise ValueError(f"lowercase_Dataframe_Column: {column} is not in dataframe columns")
    dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce').round().astype("Int64")
    return dataframe
def replace_Dataframe_Column_Not_Datetime_To_NULL(dataframe, column):
    if column not in dataframe.columns:
        raise ValueError(f"lowercase_Dataframe_Column: {column} is not in dataframe columns")
    dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce', format='mixed')
    return dataframe

def transform_data(dataframe) :
    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError("transform_data: input is not a pandas DataFrame")

    dataframe = replace_Dataframe_Column_Not_Numeric_To_NULL(dataframe, "user_id")
    dataframe = lowercase_Dataframe_Column(dataframe, "username")
    dataframe = lowercase_Dataframe_Column(dataframe, "email")
    dataframe = replace_Dataframe_Column_Not_Numeric_To_NULL(dataframe, "age")
    dataframe = replace_Dataframe_Column_Not_Datetime_To_NULL(dataframe, "signup_date")

    return dataframe
def validate_data(dataframe):
    schema = pa.DataFrameSchema(
        {
            "user_id": Column(
                int,
                unique=True,
                nullable=False
            ),
            "username": Column(
                str,
                nullable=False
            ),
            "age": Column(
                int,
                checks=Check.between(-20, 100),
                nullable=False
            ),
            "email": Column(
                str,
                checks=Check.str_matches(
                    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                ),
                nullable=False
            ),
            "signup_date": Column(
                datetime,
                nullable=False
            )
        },
        strict=True
    )
    try:
        schema.validate(dataframe, lazy=True)
        return dataframe, pd.DataFrame(), None
    except SchemaErrors as e:
        failure_cases = e.failure_cases
        failed_indices = failure_cases['index'].unique()
        failed_dataframe = dataframe.loc[failed_indices].copy()
        valid_dataframe = dataframe.drop(failed_indices).copy()
        return valid_dataframe, failed_dataframe, e

def send_Alert_JSON(message, path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"alert_{timestamp}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(message, f, indent=2) 
def send_Alert_TXT(message, path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"alert_{timestamp}.txt")
    with open(filename, "a", encoding="utf-8") as f:
        f.write(message + "\n")
def Dataframe_To_Parquet_Write(dataframe, path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(path, exist_ok=True)
    filename = os.path.join(path, f"dylan_{timestamp}.parquet")
    dataframe.to_parquet(filename, engine='pyarrow', index=False)

def run_pipeline():
    dataframe = extract_Dataframe_From_CSV(PATHS["csv"])
    print("Data extraction successful.")

    dataframe = transform_data(dataframe)
    print("Data transformation successful.")

    valid_df, failed_df, exception_error = validate_data(dataframe)
    if exception_error is not None:
        if not valid_df.empty:
            Dataframe_To_Parquet_Write(valid_df, PATHS["trusted_dir"])
        if not failed_df.empty:
            Dataframe_To_Parquet_Write(failed_df, PATHS["quarantine_dir"])
        send_Alert_TXT("CRITICAL: ETL Pipeline failed for users_raw.csv. See json for details.", PATHS["alert_dir"])
        send_Alert_JSON(exception_error.message, PATHS["alert_dir"])
        print("ON VIOLATION")
    else:
        Dataframe_To_Parquet_Write(valid_df, PATHS["trusted_dir"])
        print("Data Quality Check Passed. Pipeline finished successfully.")

run_pipeline()