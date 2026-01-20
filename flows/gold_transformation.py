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
from flows.config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="read_from_silver", retries=2)
def read_from_silver(object_name: str) -> pd.DataFrame:
    """
    Read Parquet data from silver bucket into a DataFrame.

    Args:
        object_name: Name of object in silver bucket

    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_parquet(BytesIO(data))
    print(f"Read {len(df)} rows from {BUCKET_SILVER}/{object_name}")
    return df


@task(name="create_client_summary")
def create_client_summary(df_clients: pd.DataFrame, df_achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create aggregated client summary with purchase statistics.

    Metrics per client:
        - Total purchases count
        - Total amount spent
        - Average purchase amount
        - First and last purchase dates
        - Favorite product (most purchased)
        - Days since registration

    Args:
        df_clients: Cleaned clients DataFrame
        df_achats: Cleaned purchases DataFrame

    Returns:
        Client summary DataFrame
    """
    # Aggregate purchase statistics per client
    achats_agg = df_achats.groupby("id_client").agg(
        nb_achats=("id_achat", "count"),
        montant_total=("montant", "sum"),
        montant_moyen=("montant", "mean"),
        montant_min=("montant", "min"),
        montant_max=("montant", "max"),
        premier_achat=("date_achat", "min"),
        dernier_achat=("date_achat", "max")
    ).reset_index()
    
    # Find favorite product per client (most frequently purchased)
    produit_favori = (
        df_achats.groupby(["id_client", "produit"])
        .size()
        .reset_index(name="count")
        .sort_values(["id_client", "count"], ascending=[True, False])
        .drop_duplicates(subset=["id_client"], keep="first")
        [["id_client", "produit"]]
        .rename(columns={"produit": "produit_favori"})
    )
    
    # Merge with clients data
    df_summary = df_clients.merge(achats_agg, on="id_client", how="left")
    df_summary = df_summary.merge(produit_favori, on="id_client", how="left")
    
    # Fill NaN for clients without purchases
    df_summary["nb_achats"] = df_summary["nb_achats"].fillna(0).astype(int)
    df_summary["montant_total"] = df_summary["montant_total"].fillna(0)
    df_summary["montant_moyen"] = df_summary["montant_moyen"].fillna(0)
    
    # Calculate days since registration
    df_summary["jours_depuis_inscription"] = (
        pd.Timestamp.now() - df_summary["date_inscription"]
    ).dt.days
    
    # Calculate customer lifetime value indicator
    df_summary["valeur_client"] = df_summary["montant_total"] / (
        df_summary["jours_depuis_inscription"].replace(0, 1)
    ) * 365  # Annualized value
    
    # Round numeric columns
    df_summary["montant_total"] = df_summary["montant_total"].round(2)
    df_summary["montant_moyen"] = df_summary["montant_moyen"].round(2)
    df_summary["valeur_client"] = df_summary["valeur_client"].round(2)
    
    # Add processing timestamp
    df_summary["processed_at"] = pd.Timestamp.now()
    
    print(f"Created client summary: {len(df_summary)} clients")
    return df_summary


@task(name="create_product_analytics")
def create_product_analytics(df_achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create product performance analytics.

    Metrics per product:
        - Total sales count
        - Total revenue
        - Average price
        - Unique customers
        - Sales by month

    Args:
        df_achats: Cleaned purchases DataFrame

    Returns:
        Product analytics DataFrame
    """
    # Overall product metrics
    product_stats = df_achats.groupby("produit").agg(
        nb_ventes=("id_achat", "count"),
        revenu_total=("montant", "sum"),
        prix_moyen=("montant", "mean"),
        prix_min=("montant", "min"),
        prix_max=("montant", "max"),
        nb_clients_uniques=("id_client", "nunique"),
        premiere_vente=("date_achat", "min"),
        derniere_vente=("date_achat", "max")
    ).reset_index()
    
    # Calculate market share
    total_revenue = product_stats["revenu_total"].sum()
    product_stats["part_marche_pct"] = (
        product_stats["revenu_total"] / total_revenue * 100
    ).round(2)
    
    # Round numeric columns
    product_stats["revenu_total"] = product_stats["revenu_total"].round(2)
    product_stats["prix_moyen"] = product_stats["prix_moyen"].round(2)
    
    # Sort by revenue
    product_stats = product_stats.sort_values("revenu_total", ascending=False)
    
    # Add processing timestamp
    product_stats["processed_at"] = pd.Timestamp.now()
    
    print(f"Created product analytics: {len(product_stats)} products")
    return product_stats


@task(name="create_monthly_sales")
def create_monthly_sales(df_achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create monthly sales aggregation for time-series analysis.

    Metrics per month:
        - Total sales count
        - Total revenue
        - Average basket size
        - Unique customers
        - Top product of the month

    Args:
        df_achats: Cleaned purchases DataFrame

    Returns:
        Monthly sales DataFrame
    """
    # Create year-month column
    df_achats = df_achats.copy()
    df_achats["annee_mois"] = df_achats["date_achat"].dt.to_period("M")
    
    # Monthly aggregations
    monthly_stats = df_achats.groupby("annee_mois").agg(
        nb_ventes=("id_achat", "count"),
        revenu_total=("montant", "sum"),
        panier_moyen=("montant", "mean"),
        nb_clients_uniques=("id_client", "nunique")
    ).reset_index()
    
    # Find top product per month
    top_product_monthly = (
        df_achats.groupby(["annee_mois", "produit"])["montant"]
        .sum()
        .reset_index()
        .sort_values(["annee_mois", "montant"], ascending=[True, False])
        .drop_duplicates(subset=["annee_mois"], keep="first")
        [["annee_mois", "produit"]]
        .rename(columns={"produit": "top_produit"})
    )
    
    monthly_stats = monthly_stats.merge(top_product_monthly, on="annee_mois", how="left")
    
    # Convert period to string for Parquet compatibility
    monthly_stats["annee_mois"] = monthly_stats["annee_mois"].astype(str)
    
    # Round numeric columns
    monthly_stats["revenu_total"] = monthly_stats["revenu_total"].round(2)
    monthly_stats["panier_moyen"] = monthly_stats["panier_moyen"].round(2)
    
    # Sort by month
    monthly_stats = monthly_stats.sort_values("annee_mois")
    
    # Add processing timestamp
    monthly_stats["processed_at"] = pd.Timestamp.now()
    
    print(f"Created monthly sales: {len(monthly_stats)} months")
    return monthly_stats


@task(name="create_country_analytics")
def create_country_analytics(
    df_clients: pd.DataFrame, 
    df_achats: pd.DataFrame
) -> pd.DataFrame:
    """
    Create country-level analytics.

    Metrics per country:
        - Total customers
        - Total purchases
        - Total revenue
        - Average revenue per customer

    Args:
        df_clients: Cleaned clients DataFrame
        df_achats: Cleaned purchases DataFrame

    Returns:
        Country analytics DataFrame
    """
    # Merge clients with purchases to get country info
    df_merged = df_achats.merge(
        df_clients[["id_client", "pays"]], 
        on="id_client", 
        how="left"
    )
    
    # Country-level aggregations
    country_stats = df_merged.groupby("pays").agg(
        nb_achats=("id_achat", "count"),
        revenu_total=("montant", "sum"),
        panier_moyen=("montant", "mean"),
        nb_clients_actifs=("id_client", "nunique")
    ).reset_index()
    
    # Add total clients per country
    clients_per_country = df_clients.groupby("pays").size().reset_index(name="nb_clients_total")
    country_stats = country_stats.merge(clients_per_country, on="pays", how="right")
    
    # Fill NaN for countries without purchases
    country_stats["nb_achats"] = country_stats["nb_achats"].fillna(0).astype(int)
    country_stats["revenu_total"] = country_stats["revenu_total"].fillna(0)
    country_stats["nb_clients_actifs"] = country_stats["nb_clients_actifs"].fillna(0).astype(int)
    
    # Calculate revenue per customer
    country_stats["revenu_par_client"] = (
        country_stats["revenu_total"] / country_stats["nb_clients_actifs"].replace(0, 1)
    ).round(2)
    
    # Calculate conversion rate (active/total clients)
    country_stats["taux_conversion_pct"] = (
        country_stats["nb_clients_actifs"] / country_stats["nb_clients_total"] * 100
    ).round(2)
    
    # Round numeric columns
    country_stats["revenu_total"] = country_stats["revenu_total"].round(2)
    country_stats["panier_moyen"] = country_stats["panier_moyen"].round(2)
    
    # Sort by revenue
    country_stats = country_stats.sort_values("revenu_total", ascending=False)
    
    # Add processing timestamp
    country_stats["processed_at"] = pd.Timestamp.now()
    
    print(f"Created country analytics: {len(country_stats)} countries")
    return country_stats


@task(name="write_to_gold", retries=2)
def write_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Write aggregated DataFrame to gold bucket as Parquet.

    Args:
        df: Aggregated DataFrame
        object_name: Name for the object in gold bucket

    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    # Create gold bucket if it doesn't exist
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Convert to Parquet format
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    parquet_name = f"{object_name}.parquet"
    
    client.put_object(
        BUCKET_GOLD,
        parquet_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    
    print(f"Written {len(df)} rows to {BUCKET_GOLD}/{parquet_name}")
    return parquet_name


@flow(name="Gold Transformation Flow")
def gold_transformation_flow() -> dict:
    """
    Main flow: Read from silver, create aggregations, and write to gold layer.

    Returns:
        Dictionary with created gold tables and their statistics
    """
    # Read cleaned data from silver
    df_clients = read_from_silver("clients.parquet")
    df_achats = read_from_silver("achats.parquet")
    
    # Create gold aggregations
    df_client_summary = create_client_summary(df_clients, df_achats)
    df_product_analytics = create_product_analytics(df_achats)
    df_monthly_sales = create_monthly_sales(df_achats)
    df_country_analytics = create_country_analytics(df_clients, df_achats)
    
    # Write to gold layer
    gold_clients = write_to_gold(df_client_summary, "client_summary")
    gold_products = write_to_gold(df_product_analytics, "product_analytics")
    gold_monthly = write_to_gold(df_monthly_sales, "monthly_sales")
    gold_country = write_to_gold(df_country_analytics, "country_analytics")
    
    return {
        "client_summary": {
            "file": gold_clients,
            "rows": len(df_client_summary)
        },
        "product_analytics": {
            "file": gold_products,
            "rows": len(df_product_analytics)
        },
        "monthly_sales": {
            "file": gold_monthly,
            "rows": len(df_monthly_sales)
        },
        "country_analytics": {
            "file": gold_country,
            "rows": len(df_country_analytics)
        }
    }


if __name__ == "__main__":
    result = gold_transformation_flow()
    print(f"\n{'='*50}")
    print("Gold transformation complete!")
    print(f"{'='*50}")
    for table_name, info in result.items():
        print(f"  - {table_name}: {info['rows']} rows -> {info['file']}")
