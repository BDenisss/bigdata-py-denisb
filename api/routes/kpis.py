"""
KPI routes for the API - Global metrics and dashboard data.
"""

import time
from fastapi import APIRouter
from api.database import get_collection
from api.models import GlobalKPIs, TimingMetrics

router = APIRouter(prefix="/kpis", tags=["KPIs"])


@router.get("", response_model=GlobalKPIs)
def get_global_kpis():
    """
    Get global KPIs for dashboard.
    """
    clients_col = get_collection("clients_summary")
    total_clients = clients_col.count_documents({})
    
    client_agg = list(clients_col.aggregate([
        {
            "$group": {
                "_id": None,
                "total_achats": {"$sum": "$nb_achats"},
                "chiffre_affaires": {"$sum": "$montant_total"}
            }
        }
    ]))
    
    total_achats = client_agg[0]["total_achats"] if client_agg else 0
    chiffre_affaires = client_agg[0]["chiffre_affaires"] if client_agg else 0
    panier_moyen = round(chiffre_affaires / total_achats, 2) if total_achats > 0 else 0
    
    country_col = get_collection("country_analytics")
    top_country_doc = country_col.find_one(sort=[("revenu_total", -1)])
    top_pays = top_country_doc["pays"] if top_country_doc else "N/A"
    
    products_col = get_collection("products_analytics")
    top_product_doc = products_col.find_one(sort=[("revenu_total", -1)])
    top_produit = top_product_doc["produit"] if top_product_doc else "N/A"
    
    return GlobalKPIs(
        total_clients=total_clients,
        total_achats=total_achats,
        chiffre_affaires=round(chiffre_affaires, 2),
        panier_moyen=panier_moyen,
        top_pays=top_pays,
        top_produit=top_produit
    )


@router.get("/refresh-metrics", response_model=TimingMetrics)
def get_refresh_metrics():
    """
    Measure and return API response time metrics.
    """
    start_time = time.time()
    
    query_start = time.time()
    
    clients_col = get_collection("clients_summary")
    _ = clients_col.count_documents({})
    _ = list(clients_col.find().limit(10))
    
    products_col = get_collection("products_analytics")
    _ = list(products_col.find())
    
    monthly_col = get_collection("monthly_sales")
    _ = list(monthly_col.find())
    
    query_time = (time.time() - query_start) * 1000
    total_time = (time.time() - start_time) * 1000
    
    return TimingMetrics(
        endpoint="/kpis/refresh-metrics",
        query_time_ms=round(query_time, 2),
        total_time_ms=round(total_time, 2)
    )
