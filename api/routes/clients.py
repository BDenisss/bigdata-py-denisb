"""
Client routes for the API.
"""

from fastapi import APIRouter, Query, HTTPException
from api.database import get_collection
from api.models import ClientSummary, ClientListResponse

router = APIRouter(prefix="/clients", tags=["Clients"])


@router.get("", response_model=ClientListResponse)
def get_clients(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    pays: str = Query(None, description="Filter by country"),
    sort_by: str = Query("montant_total", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc or desc")
):
    """
    Get paginated list of clients with their statistics.
    """
    collection = get_collection("clients_summary")
    
    filter_query = {}
    if pays:
        filter_query["pays"] = pays
    
    sort_direction = -1 if sort_order == "desc" else 1
    
    total = collection.count_documents(filter_query)
    
    skip = (page - 1) * page_size
    cursor = collection.find(filter_query).sort(sort_by, sort_direction).skip(skip).limit(page_size)
    
    clients = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        clients.append(ClientSummary(**doc))
    
    return ClientListResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=clients
    )


@router.get("/top", response_model=list[ClientSummary])
def get_top_clients(
    limit: int = Query(10, ge=1, le=50, description="Number of top clients"),
    by: str = Query("montant_total", description="Rank by: montant_total or nb_achats")
):
    """
    Get top clients by total amount or purchase count.
    """
    collection = get_collection("clients_summary")
    
    cursor = collection.find().sort(by, -1).limit(limit)
    
    clients = []
    for doc in cursor:
        doc.pop("_id", None)
        doc.pop("processed_at", None)
        clients.append(ClientSummary(**doc))
    
    return clients


@router.get("/{client_id}", response_model=ClientSummary)
def get_client(client_id: int):
    """
    Get a specific client by ID.
    """
    collection = get_collection("clients_summary")
    
    doc = collection.find_one({"id_client": client_id})
    
    if not doc:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found")
    
    doc.pop("_id", None)
    doc.pop("processed_at", None)
    
    return ClientSummary(**doc)
