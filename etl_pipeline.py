import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from datetime import datetime
import os
import json

PATHS = {
    "csv": "data/input/users_raw.csv"
}

def extract_Dataframe_From_CSV(path):
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Error loading data: {e}")

def transform_data(dataframe) :
    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError("transform_data: input is not a pandas DataFrame")

    #No transformation for user_id
    if 'signup_date' in dataframe.columns:
        dataframe['signup_date'] = dataframe['signup_date'].replace(r'^\s*$', pd.NA, regex=True)
        dataframe['signup_date'] = pd.to_datetime(
            dataframe['signup_date'], errors='coerce'
        )
    else:
        print("transform_data: 'signup_date' column not found in DataFrame")
    
    if 'email' in dataframe.columns:
        dataframe['email'] = dataframe['email'].astype(str).str.lower()
    else:
        print("transform_data: 'email' column not found in DataFrame")
        
    dataframe = dataframe.replace(r'^\s*$', pd.NA, regex=True)  # vide -> NaN
    dataframe = dataframe.fillna("NULL")

    return dataframe

def validate_data(dataframe):
    state = False
    schema = pa.DataFrameSchema(
        {
            "user_id": Column(
                checks=Check.not_equal_to("NULL"),
                nullable=False
            ),
            "username": Column(
                checks=Check.not_equal_to("NULL"),
                nullable=True
            ),
            "age": Column(
                checks=Check.between(18, 100),
                nullable=True
            ),
            "email": Column(
                checks=Check.str_matches(
                    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                ),
                nullable=False
            ),
            "signup_date": Column(
                checks=Check.not_equal_to("NULL"),
                nullable=True
            )
        },
        strict=False
    )
    
    dataframetemp = dataframe.copy()
    dataframetemp['validate'] = True

    errors_json_list = []
    for idx, row in dataframetemp.iterrows():
        try:
            schema.validate(pd.DataFrame([row]), lazy=False)
        except pa.errors.SchemaError as e:
            errors_json_list.append({idx: e})
            dataframetemp.at[idx, 'validate'] = False
            state = True


    duplicated_ids = dataframetemp['user_id'][dataframetemp['user_id'].duplicated(keep=False)]
    duplicated_indices = dataframetemp['user_id'][dataframetemp['user_id'].duplicated(keep=False)].index
    dataframetemp.loc[dataframetemp['user_id'].isin(duplicated_ids), 'validate'] = False
    for index in duplicated_indices.to_list():
        errors_json_list.append({index: "is duplicated"})

    df_valid = dataframetemp[dataframetemp['validate'] == True].reset_index(drop=True)
    df_invalid = dataframetemp[dataframetemp['validate'] == False].reset_index(drop=True)

    return df_valid, df_invalid, state, errors_json_list

def send_alert(message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "data/alerts"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"alert_{timestamp}.txt")
    with open(filename, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def run_pipeline():
    dataframeRaw = extract_Dataframe_From_CSV(PATHS["csv"])
    print("Data extraction successful.")

    Tdataframe = transform_data(dataframeRaw)

    VVdataframe, VIdataframe, state, errors_json_list = validate_data(Tdataframe)
    if state is False:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "data/trusted"
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, "dylan_{timestamp}.parquet")
        VVdataframe.to_parquet(filename, engine='pyarrow', index=False)
        print("Data Quality Check Passed. Pipeline finished successfully.")
    else :
        output_dir = "data/quarantine"
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, "sales.parquet")
        VIdataframe.to_parquet(filename, engine='pyarrow', index=False)
        send_alert("CRITICAL: ETL Pipeline failed for users_raw.csv. See quarantine for details.")
        print("ON VIOLATION")

run_pipeline()