from io import BytesIO
from pathlib import Path
import sys
import os

# Configurer Prefect pour fonctionner sans serveur (mode ephemeral)
os.environ["PREFECT_API_URL"] = ""

import pandas as pd
from prefect import flow, task

# Ajouter le dossier parent au path pour importer config
sys.path.insert(0, str(Path(__file__).parent.parent))
from flows.config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="read_from_bronze", retries=2)
def read_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from bronze bucket into a DataFrame.

    Args:
        object_name: Name of object in bronze bucket

    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_csv(BytesIO(data))
    print(f"Read {len(df)} rows from {BUCKET_BRONZE}/{object_name}")
    return df


@task(name="transform_clients")
def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean clients data.

    Transformations:
        - Remove duplicates based on id_client
        - Standardize email to lowercase
        - Convert date_inscription to datetime
        - Standardize country names
        - Add data quality columns

    Args:
        df: Raw clients DataFrame

    Returns:
        Cleaned and transformed DataFrame
    """
    df_clean = df.copy()
    
    # Remove duplicates
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["id_client"], keep="first")
    duplicates_removed = initial_count - len(df_clean)
    print(f"Removed {duplicates_removed} duplicate clients")
    
    # Standardize email to lowercase
    df_clean["email"] = df_clean["email"].str.lower().str.strip()
    
    # Convert date_inscription to datetime
    df_clean["date_inscription"] = pd.to_datetime(
        df_clean["date_inscription"], 
        errors="coerce"
    )
    
    # Standardize country names (trim whitespace, title case)
    df_clean["pays"] = df_clean["pays"].str.strip().str.title()
    
    # Standardize name
    df_clean["nom"] = df_clean["nom"].str.strip().str.title()
    
    # Add data quality columns
    df_clean["is_email_valid"] = df_clean["email"].str.contains(
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", 
        regex=True, 
        na=False
    )
    
    # Add processing timestamp
    df_clean["processed_at"] = pd.Timestamp.now()
    
    print(f"Transformed clients: {len(df_clean)} rows")
    return df_clean


@task(name="transform_achats")
def transform_achats(df: pd.DataFrame, df_clients: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and clean purchases (achats) data.

    Transformations:
        - Remove duplicates based on id_achat
        - Convert date_achat to datetime
        - Validate montant (amount) is positive
        - Standardize product names
        - Validate foreign key relationship with clients
        - Add derived columns (year, month, day)

    Args:
        df: Raw achats DataFrame
        df_clients: Cleaned clients DataFrame for FK validation

    Returns:
        Cleaned and transformed DataFrame
    """
    df_clean = df.copy()
    
    # Remove duplicates
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["id_achat"], keep="first")
    duplicates_removed = initial_count - len(df_clean)
    print(f"Removed {duplicates_removed} duplicate purchases")
    
    # Convert date_achat to datetime
    df_clean["date_achat"] = pd.to_datetime(
        df_clean["date_achat"], 
        errors="coerce"
    )
    
    # Validate and clean montant (must be positive)
    df_clean["montant"] = pd.to_numeric(df_clean["montant"], errors="coerce")
    invalid_amounts = df_clean["montant"].isna() | (df_clean["montant"] <= 0)
    if invalid_amounts.any():
        print(f"Warning: {invalid_amounts.sum()} rows with invalid amounts")
    df_clean = df_clean[~invalid_amounts]
    
    # Standardize product names
    df_clean["produit"] = df_clean["produit"].str.strip().str.title()
    
    # Validate foreign key - keep only purchases with valid clients
    valid_client_ids = set(df_clients["id_client"].unique())
    initial_count = len(df_clean)
    df_clean = df_clean[df_clean["id_client"].isin(valid_client_ids)]
    orphan_purchases = initial_count - len(df_clean)
    if orphan_purchases > 0:
        print(f"Removed {orphan_purchases} purchases with invalid client IDs")
    
    # Add derived date columns for easier analysis
    df_clean["annee_achat"] = df_clean["date_achat"].dt.year
    df_clean["mois_achat"] = df_clean["date_achat"].dt.month
    df_clean["jour_achat"] = df_clean["date_achat"].dt.day
    df_clean["jour_semaine"] = df_clean["date_achat"].dt.day_name()
    
    # Add processing timestamp
    df_clean["processed_at"] = pd.Timestamp.now()
    
    print(f"Transformed purchases: {len(df_clean)} rows")
    return df_clean


@task(name="write_to_silver", retries=2)
def write_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Write transformed DataFrame to silver bucket as Parquet.

    Args:
        df: Transformed DataFrame
        object_name: Name for the object in silver bucket (without extension)

    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    # Create silver bucket if it doesn't exist
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convert to Parquet format for better performance
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    parquet_name = f"{object_name}.parquet"
    
    client.put_object(
        BUCKET_SILVER,
        parquet_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    
    print(f"Written {len(df)} rows to {BUCKET_SILVER}/{parquet_name}")
    return parquet_name


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """
    Main flow: Read from bronze, transform, and write to silver layer.

    Returns:
        Dictionary with processed file names and statistics
    """
    # Read raw data from bronze
    df_clients_raw = read_from_bronze("clients.csv")
    df_achats_raw = read_from_bronze("achats.csv")
    
    # Transform data
    df_clients_clean = transform_clients(df_clients_raw)
    df_achats_clean = transform_achats(df_achats_raw, df_clients_clean)
    
    # Write to silver layer (Parquet format)
    silver_clients = write_to_silver(df_clients_clean, "clients")
    silver_achats = write_to_silver(df_achats_clean, "achats")
    
    # Return summary statistics
    return {
        "clients": {
            "file": silver_clients,
            "rows_raw": len(df_clients_raw),
            "rows_clean": len(df_clients_clean)
        },
        "achats": {
            "file": silver_achats,
            "rows_raw": len(df_achats_raw),
            "rows_clean": len(df_achats_clean)
        }
    }


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"\nSilver transformation complete!")
    print(f"Clients: {result['clients']['rows_raw']} -> {result['clients']['rows_clean']} rows")
    print(f"Achats: {result['achats']['rows_raw']} -> {result['achats']['rows_clean']} rows")
