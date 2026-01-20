"""
Pydantic models for API request/response schemas.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ClientSummary(BaseModel):
    id_client: int
    nom: str
    email: str
    pays: str
    date_inscription: Optional[datetime] = None
    nb_achats: int
    montant_total: float
    montant_moyen: float
    produit_favori: Optional[str] = None
    valeur_client: float
    
    class Config:
        from_attributes = True


class ClientListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: list[ClientSummary]


class ProductAnalytics(BaseModel):
    produit: str
    nb_ventes: int
    revenu_total: float
    prix_moyen: float
    nb_clients_uniques: int
    part_marche_pct: float
    
    class Config:
        from_attributes = True


class ProductListResponse(BaseModel):
    total: int
    data: list[ProductAnalytics]


class MonthlySales(BaseModel):
    annee_mois: str
    nb_ventes: int
    revenu_total: float
    panier_moyen: float
    nb_clients_uniques: int
    top_produit: Optional[str] = None
    
    class Config:
        from_attributes = True


class MonthlySalesResponse(BaseModel):
    total: int
    data: list[MonthlySales]


class CountryAnalytics(BaseModel):
    pays: str
    nb_clients: int
    nb_achats: int
    revenu_total: float
    revenu_moyen_par_client: float
    
    class Config:
        from_attributes = True


class CountryListResponse(BaseModel):
    total: int
    data: list[CountryAnalytics]


class GlobalKPIs(BaseModel):
    total_clients: int
    total_achats: int
    chiffre_affaires: float
    panier_moyen: float
    top_pays: str
    top_produit: str


class HealthResponse(BaseModel):
    status: str
    mongodb: str
    timestamp: datetime


class TimingMetrics(BaseModel):
    endpoint: str
    query_time_ms: float
    total_time_ms: float
