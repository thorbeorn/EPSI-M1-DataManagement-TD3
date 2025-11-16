import pytest
import pandas as pd
import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from etl_pipeline import lowercase_Dataframe_Column, replace_Dataframe_Column_Not_Numeric_To_NULL, replace_Dataframe_Column_Not_Datetime_To_NULL, transform_data

def test_lowercase_Dataframe_Column():
    df = pd.DataFrame({
        "data": [12, "NoN", "", "OUI", "peut-etre"]
    })
    transformed_df = lowercase_Dataframe_Column(df, "data")
    assert pd.api.types.is_string_dtype(transformed_df['data'])
    assert pd.isna(transformed_df.loc[2, "data"])
    assert transformed_df.loc[1, "data"] == "non"
    assert transformed_df.loc[3, "data"] == "oui"

def test_replace_Dataframe_Column_Not_Numeric_To_NULL():
    df = pd.DataFrame({
        "age": [25, "30", "invalid", 45, "", None, "50.5"]
    })
    transformed_df = replace_Dataframe_Column_Not_Numeric_To_NULL(df, "age") 
    assert transformed_df['age'].dtype == 'Int64'
    assert transformed_df.loc[0, "age"] == 25
    assert transformed_df.loc[1, "age"] == 30
    assert pd.isna(transformed_df.loc[2, "age"])
    assert transformed_df.loc[3, "age"] == 45
    assert pd.isna(transformed_df.loc[4, "age"])
    assert pd.isna(transformed_df.loc[5, "age"])
    assert transformed_df.loc[6, "age"] == 50
def test_replace_Dataframe_Column_Not_Numeric_To_NULL_column_not_exists():
    df = pd.DataFrame({
        "age": [25, 30]
    })
    with pytest.raises(ValueError, match="lowercase_Dataframe_Column: invalid_column is not in dataframe columns"):
        replace_Dataframe_Column_Not_Numeric_To_NULL(df, "invalid_column")

def test_replace_Dataframe_Column_Not_Datetime_To_NULL():
    df = pd.DataFrame({
        "signup_date": [
            "2023-01-15",
            "2023-02-20",
            "invalid_date",
            "2023-03-25 14:30:00",
            "",
            None,
            "2023-04-15"
        ]
    })
    transformed_df = replace_Dataframe_Column_Not_Datetime_To_NULL(df, "signup_date")
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['signup_date'])
    assert transformed_df.loc[0, "signup_date"] == pd.Timestamp("2023-01-15")
    assert transformed_df.loc[1, "signup_date"] == pd.Timestamp("2023-02-20")
    assert pd.isna(transformed_df.loc[2, "signup_date"])  # "invalid_date" devient NaT
    assert transformed_df.loc[3, "signup_date"] == pd.Timestamp("2023-03-25 14:30:00")
    assert pd.isna(transformed_df.loc[4, "signup_date"])  # "" devient NaT
    assert pd.isna(transformed_df.loc[5, "signup_date"])  # None devient NaT
    assert transformed_df.loc[6, "signup_date"] == pd.Timestamp("2023-04-15")
def test_replace_Dataframe_Column_Not_Datetime_To_NULL_column_not_exists():
    df = pd.DataFrame({
        "signup_date": ["2023-01-15"]
    })
    with pytest.raises(ValueError, match="lowercase_Dataframe_Column: invalid_column is not in dataframe columns"):
        replace_Dataframe_Column_Not_Datetime_To_NULL(df, "invalid_column")
def test_replace_Dataframe_Column_Not_Datetime_To_NULL_with_actual_datetime():
    df = pd.DataFrame({
        "signup_date": [
            datetime(2023, 1, 15),
            pd.Timestamp("2023-02-20"),
            "2023-03-25"
        ]
    })
    transformed_df = replace_Dataframe_Column_Not_Datetime_To_NULL(df, "signup_date")
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['signup_date'])
    assert transformed_df.loc[0, "signup_date"] == pd.Timestamp("2023-01-15")
    assert transformed_df.loc[1, "signup_date"] == pd.Timestamp("2023-02-20")
    assert transformed_df.loc[2, "signup_date"] == pd.Timestamp("2023-03-25")

def test_transform_data_valid_input():
    df = pd.DataFrame({
        "user_id": [1, "2", 3],
        "username": ["Alice", "BOB", "Charlie"],
        "email": ["Alice@Example.COM", "bob@TEST.com", "CHARLIE@mail.COM"],
        "age": [25, "30", 35],
        "signup_date": ["2023-01-15", "2023-02-20", "2023-03-25"]
    })
    transformed_df = transform_data(df)
    assert transformed_df['user_id'].dtype == 'Int64'
    assert transformed_df['username'].dtype == 'string'
    assert transformed_df['email'].dtype == 'string'
    assert transformed_df['age'].dtype == 'Int64'
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['signup_date'])
    assert transformed_df.loc[0, "user_id"] == 1
    assert transformed_df.loc[1, "user_id"] == 2
    assert transformed_df.loc[0, "username"] == "alice"
    assert transformed_df.loc[1, "username"] == "bob"
    assert transformed_df.loc[0, "email"] == "alice@example.com"
    assert transformed_df.loc[1, "email"] == "bob@test.com"
    assert transformed_df.loc[0, "age"] == 25
    assert transformed_df.loc[1, "signup_date"] == pd.Timestamp("2023-02-20")
def test_transform_data_with_invalid_values():
    df = pd.DataFrame({
        "user_id": [1, "invalid", 3, ""],
        "username": ["Alice", "Bob", "", "David"],
        "email": ["alice@test.com", "BOB@TEST.COM", "", "david@mail.com"],
        "age": [25, "thirty", 35, "40.7"],
        "signup_date": ["2023-01-15", "invalid_date", "2023-03-25", ""]
    })
    transformed_df = transform_data(df)
    assert transformed_df.loc[0, "user_id"] == 1
    assert pd.isna(transformed_df.loc[1, "user_id"])
    assert pd.isna(transformed_df.loc[3, "user_id"])
    assert transformed_df.loc[0, "username"] == "alice"
    assert pd.isna(transformed_df.loc[2, "username"])
    assert transformed_df.loc[0, "email"] == "alice@test.com"
    assert transformed_df.loc[1, "email"] == "bob@test.com"
    assert pd.isna(transformed_df.loc[2, "email"])
    assert transformed_df.loc[0, "age"] == 25
    assert pd.isna(transformed_df.loc[1, "age"])
    assert transformed_df.loc[3, "age"] == 41
    assert transformed_df.loc[0, "signup_date"] == pd.Timestamp("2023-01-15")
    assert pd.isna(transformed_df.loc[1, "signup_date"])
    assert pd.isna(transformed_df.loc[3, "signup_date"])
def test_transform_data_with_none_values():
    df = pd.DataFrame({
        "user_id": [1, None, 3],
        "username": ["Alice", None, "Charlie"],
        "email": ["alice@test.com", None, "charlie@mail.com"],
        "age": [25, None, 35],
        "signup_date": ["2023-01-15", None, "2023-03-25"]
    })
    transformed_df = transform_data(df)
    assert transformed_df.loc[0, "user_id"] == 1
    assert pd.isna(transformed_df.loc[1, "user_id"])
    assert pd.isna(transformed_df.loc[1, "username"])
    assert pd.isna(transformed_df.loc[1, "email"])
    assert pd.isna(transformed_df.loc[1, "age"])
    assert pd.isna(transformed_df.loc[1, "signup_date"])
def test_transform_data_invalid_input():
    with pytest.raises(ValueError, match="transform_data: input is not a pandas DataFrame"):
        transform_data("not a dataframe")
    with pytest.raises(ValueError, match="transform_data: input is not a pandas DataFrame"):
        transform_data([1, 2, 3])
    with pytest.raises(ValueError, match="transform_data: input is not a pandas DataFrame"):
        transform_data({"key": "value"})
def test_transform_data_preserves_dataframe_structure():
    df = pd.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "username": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com", "diana@test.com", "eve@test.com"],
        "age": [25, 30, 35, 40, 45],
        "signup_date": ["2023-01-15", "2023-02-20", "2023-03-25", "2023-04-10", "2023-05-05"]
    })
    original_shape = df.shape
    transformed_df = transform_data(df)
    assert transformed_df.shape == original_shape
    assert len(transformed_df.columns) == len(df.columns)
    assert list(transformed_df.columns) == list(df.columns)
def test_transform_data_case_sensitivity():
    df = pd.DataFrame({
        "user_id": [1, 2],
        "username": ["UPPERCASE", "MiXeD_CaSe"],
        "email": ["UPPER@TEST.COM", "MiXeD@TeSt.CoM"],
        "age": [25, 30],
        "signup_date": ["2023-01-15", "2023-02-20"]
    })
    transformed_df = transform_data(df)
    assert transformed_df.loc[0, "username"] == "uppercase"
    assert transformed_df.loc[1, "username"] == "mixed_case"
    assert transformed_df.loc[0, "email"] == "upper@test.com"
    assert transformed_df.loc[1, "email"] == "mixed@test.com"