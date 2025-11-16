import pytest
import pandas as pd
from etl_pipeline import transform_data

# ---------------------------
# Test 1 : Conversion de signup_date en datetime
# ---------------------------
def test_signup_date_conversion():
    df = pd.DataFrame({
        "signup_date": ["2023-10-01", "2023-10-02", ""]
    })
    transformed_df = transform_data(df)
    
    # Vérifie que la colonne est en datetime
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['signup_date'])
    
    # Vérifie que les valeurs vides sont converties en NaT
    assert pd.isna(transformed_df.loc[2, "signup_date"])

# ---------------------------
# Test 2 : Normalisation des emails en minuscules
# ---------------------------
def test_email_normalization():
    df = pd.DataFrame({
        "email": ["ALICE@EXAMPLE.COM", "BoB@domain.com"]
    })
    transformed_df = transform_data(df)
    
    # Vérifie que tous les emails sont en minuscules
    assert transformed_df.loc[0, "email"] == "alice@example.com"
    assert transformed_df.loc[1, "email"] == "bob@domain.com"

# ---------------------------
# Test 3 : Gestion des valeurs nulles ou inattendues dans age
# ---------------------------
def test_age_handling():
    df = pd.DataFrame({
        "age": [25, None, -5, 200]
    })
    transformed_df = transform_data(df)
    
    # Vérifie que les valeurs None sont remplacées par "NULL"
    assert transformed_df.loc[1, "age"] == "NULL"
    
    # Optionnel : si tu as une logique de nettoyage pour les âges invalides
    # par exemple remplacer les âges négatifs ou >100 par "NULL"
    assert transformed_df.loc[2, "age"] == -5  # si pas de nettoyage, sinon "NULL"
    assert transformed_df.loc[3, "age"] == 200  # idem
