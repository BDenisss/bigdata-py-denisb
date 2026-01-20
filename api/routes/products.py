"""
Product routes for the API.
"""

from fastapi import APIRouter, Query
from api.database import get_collection
from api.models import ProductAnalytics, ProductListResponse

router = APIRouter(prefix="/products", tags=["Products"])


@router.get("", response_model=ProductListResponse)
def get_products(
    sort_by: str = Query("revenu_total", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc or desc")
):
    """
    Get all products with their analytics.
    """
    collection = get_collection("products_analytics")
    
    sort_direction = -1 if sort_order == "desc" else 1
    cursor = collection.find().sort(sort_by, sort_direction)
    
    products = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        products.append(ProductAnalytics(**doc))
    
    return ProductListResponse(
        total=len(products),
        data=products
    )


@router.get("/top", response_model=list[ProductAnalytics])
def get_top_products(
    limit: int = Query(5, ge=1, le=20, description="Number of top products"),
    by: str = Query("revenu_total", description="Rank by: revenu_total, nb_ventes, or nb_clients_uniques")
):
    """
    Get top performing products.
    """
    collection = get_collection("products_analytics")
    
    cursor = collection.find().sort(by, -1).limit(limit)
    
    products = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        products.append(ProductAnalytics(**doc))
    
    return products
