"""
Load Gold layer Parquet data from MinIO to MongoDB.

This module reads aggregated data from the Gold bucket and loads it
into MongoDB collections for operational use by the API and dashboard.
"""

from io import BytesIO
from pathlib import Path
import sys
import os
import time
from datetime import datetime

os.environ["PREFECT_API_URL"] = ""

import pandas as pd
from prefect import flow, task
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

sys.path.insert(0, str(Path(__file__).parent.parent))
from flows.config import BUCKET_GOLD, get_minio_client


MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")


def get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client."""
    return MongoClient(MONGO_URI)


def get_mongo_database():
    """Get the MongoDB database instance."""
    client = get_mongo_client()
    return client[MONGO_DATABASE]


@task(name="read_gold_parquet", retries=2)
def read_gold_parquet(object_name: str) -> pd.DataFrame:
    """
    Read Parquet data from Gold bucket into a DataFrame.

    Args:
        object_name: Name of object in gold bucket

    Returns:
        DataFrame with gold data
    """
    client = get_minio_client()
    
    try:
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_parquet(BytesIO(data))
        print(f"✅ Read {len(df)} rows from {BUCKET_GOLD}/{object_name}")
        return df
    except Exception as e:
        print(f"❌ Error reading {object_name}: {e}")
        raise


@task(name="prepare_documents")
def prepare_documents(df: pd.DataFrame) -> list[dict]:
    """
    Convert DataFrame to list of MongoDB documents.
    
    Handles datetime conversion and NaN values.

    Args:
        df: DataFrame to convert

    Returns:
        List of dictionaries ready for MongoDB insertion
    """
    df = df.copy()
    
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.to_pydatetime()
    
    records = df.to_dict(orient="records")
    
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
    
    return records


@task(name="load_to_mongodb")
def load_to_mongodb(
    documents: list[dict], 
    collection_name: str,
    drop_existing: bool = True
) -> dict:
    """
    Load documents into MongoDB collection.

    Args:
        documents: List of documents to insert
        collection_name: Target collection name
        drop_existing: Whether to drop existing collection first

    Returns:
        Dict with insertion stats
    """
    start_time = time.time()
    
    db = get_mongo_database()
    collection = db[collection_name]
    
    if drop_existing:
        collection.drop()
        print(f"🗑️  Dropped existing collection: {collection_name}")
    
    if documents:
        try:
            result = collection.insert_many(documents)
            inserted_count = len(result.inserted_ids)
        except BulkWriteError as e:
            inserted_count = e.details.get("nInserted", 0)
            print(f"⚠️  Bulk write error: {e.details}")
    else:
        inserted_count = 0
    
    elapsed_time = time.time() - start_time
    
    stats = {
        "collection": collection_name,
        "documents_inserted": inserted_count,
        "elapsed_time_seconds": round(elapsed_time, 3)
    }
    
    print(f"✅ Loaded {inserted_count} documents into '{collection_name}' in {elapsed_time:.3f}s")
    return stats


@task(name="create_indexes")
def create_indexes() -> None:
    """
    Create MongoDB indexes for better query performance.
    """
    db = get_mongo_database()
    
    db.clients_summary.create_index("id_client", unique=True)
    db.clients_summary.create_index("pays")
    db.clients_summary.create_index("montant_total")
    print("✅ Created indexes on clients_summary")
    
    db.products_analytics.create_index("produit", unique=True)
    db.products_analytics.create_index("revenu_total")
    print("✅ Created indexes on products_analytics")
    
    db.monthly_sales.create_index("annee_mois", unique=True)
    print("✅ Created indexes on monthly_sales")
    
    db.country_analytics.create_index("pays", unique=True)
    print("✅ Created indexes on country_analytics")


@task(name="verify_load")
def verify_load() -> dict:
    """
    Verify data was loaded correctly by counting documents.

    Returns:
        Dict with collection counts
    """
    db = get_mongo_database()
    
    collections = ["clients_summary", "products_analytics", "monthly_sales", "country_analytics"]
    counts = {}
    
    for col_name in collections:
        count = db[col_name].count_documents({})
        counts[col_name] = count
    
    print("\n📊 MongoDB Collection Counts:")
    for col, count in counts.items():
        print(f"   - {col}: {count} documents")
    
    return counts


@flow(name="gold_to_mongodb_flow")
def gold_to_mongodb_flow() -> dict:
    """
    Main flow to load all Gold data into MongoDB.

    Reads Parquet files from MinIO Gold bucket and loads them
    into corresponding MongoDB collections.

    Returns:
        Dict with overall load statistics
    """
    start_time = time.time()
    print("\n" + "="*60)
    print("🚀 Starting Gold → MongoDB Load")
    print("="*60)
    
    gold_to_mongo_mapping = {
        "client_summary.parquet": "clients_summary",
        "product_analytics.parquet": "products_analytics",
        "monthly_sales.parquet": "monthly_sales",
        "country_analytics.parquet": "country_analytics"
    }
    
    all_stats = []
    
    for parquet_file, collection_name in gold_to_mongo_mapping.items():
        print(f"\n📦 Processing {parquet_file} → {collection_name}")
        
        try:
            df = read_gold_parquet(parquet_file)
            
            documents = prepare_documents(df)
            
            stats = load_to_mongodb(documents, collection_name)
            all_stats.append(stats)
            
        except Exception as e:
            print(f"❌ Failed to process {parquet_file}: {e}")
            all_stats.append({
                "collection": collection_name,
                "documents_inserted": 0,
                "error": str(e)
            })
    
    print("\n🔧 Creating indexes...")
    create_indexes()
    
    collection_counts = verify_load()
    
    total_time = time.time() - start_time
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_time_seconds": round(total_time, 3),
        "collections_loaded": len([s for s in all_stats if s.get("documents_inserted", 0) > 0]),
        "total_documents": sum(s.get("documents_inserted", 0) for s in all_stats),
        "details": all_stats,
        "collection_counts": collection_counts
    }
    
    print("\n" + "="*60)
    print("✅ Gold → MongoDB Load Complete!")
    print(f"   Total time: {total_time:.2f}s")
    print(f"   Collections: {report['collections_loaded']}")
    print(f"   Documents: {report['total_documents']}")
    print("="*60 + "\n")
    
    return report


if __name__ == "__main__":
    result = gold_to_mongodb_flow()
    print("\n📋 Final Report:")
    for key, value in result.items():
        if key != "details":
            print(f"   {key}: {value}")