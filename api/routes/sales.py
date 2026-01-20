"""
Sales routes for the API.
"""

from fastapi import APIRouter, Query
from api.database import get_collection
from api.models import MonthlySales, MonthlySalesResponse

router = APIRouter(prefix="/sales", tags=["Sales"])


@router.get("/monthly", response_model=MonthlySalesResponse)
def get_monthly_sales(
    sort_order: str = Query("asc", description="Sort order by month: asc or desc")
):
    """
    Get monthly sales aggregations.
    """
    collection = get_collection("monthly_sales")
    
    sort_direction = 1 if sort_order == "asc" else -1
    cursor = collection.find().sort("annee_mois", sort_direction)
    
    sales = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        sales.append(MonthlySales(**doc))
    
    return MonthlySalesResponse(
        total=len(sales),
        data=sales
    )


@router.get("/by-country", response_model=list[dict])
def get_sales_by_country(
    sort_by: str = Query("revenu_total", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order")
):
    """
    Get sales aggregated by country.
    """
    collection = get_collection("country_analytics")
    
    sort_direction = -1 if sort_order == "desc" else 1
    cursor = collection.find().sort(sort_by, sort_direction)
    
    countries = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        countries.append(doc)
    
    return countries
