"""
FastAPI application for E-commerce Analytics API.

This API exposes MongoDB data for the Streamlit dashboard.
"""

import time
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from api.database import get_mongo_client, MONGO_DATABASE
from api.routes import clients, products, sales, kpis
from api.models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    print("🚀 Starting E-commerce Analytics API...")
    yield
    print("👋 Shutting down API...")


app = FastAPI(
    title="E-commerce Analytics API",
    description="API exposant les données MongoDB pour le dashboard Streamlit",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_timing_header(request: Request, call_next):
    """Add response timing header."""
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    response.headers["X-Process-Time-Ms"] = str(round(process_time, 2))
    return response


app.include_router(clients.router)
app.include_router(products.router)
app.include_router(sales.router)
app.include_router(kpis.router)


@app.get("/", tags=["Root"])
def root():
    """API root endpoint."""
    return {
        "message": "E-commerce Analytics API",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check():
    """
    Health check endpoint.
    
    Verifies MongoDB connectivity.
    """
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        mongo_status = "connected"
    except Exception as e:
        mongo_status = f"error: {str(e)}"
    
    return HealthResponse(
        status="healthy" if mongo_status == "connected" else "unhealthy",
        mongodb=mongo_status,
        timestamp=datetime.now()
    )


@app.get("/collections", tags=["Debug"])
def list_collections():
    """
    List all collections and their document counts.
    """
    client = get_mongo_client()
    db = client[MONGO_DATABASE]
    
    collections = {}
    for name in db.list_collection_names():
        collections[name] = db[name].count_documents({})
    
    return {
        "database": MONGO_DATABASE,
        "collections": collections
    }
