"""
E-commerce Analytics Dashboard - Streamlit Application

Dashboard connecté à l'API FastAPI pour visualiser les données MongoDB.
"""

import time
from datetime import datetime

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

API_BASE_URL = "http://localhost:8000"

st.set_page_config(
    page_title="E-commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data(ttl=60)
def fetch_data(endpoint: str) -> dict | list | None:
    """Fetch data from API with caching."""
    try:
        start_time = time.time()
        response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
        response.raise_for_status()
        elapsed = (time.time() - start_time) * 1000
        return response.json(), elapsed
    except requests.RequestException as e:
        st.error(f"Erreur API: {e}")
        return None, 0


def check_api_health() -> bool:
    """Check if API is available."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False


with st.sidebar:
    st.title("⚙️ Configuration")
    
    api_status = check_api_health()
    if api_status:
        st.success("✅ API connectée")
    else:
        st.error("❌ API non disponible")
        st.info("Lancez l'API avec:\n`uvicorn api.app:app --port 8000`")
    
    st.divider()
    
    auto_refresh = st.checkbox("🔄 Auto-refresh", value=False)
    refresh_interval = st.selectbox(
        "Intervalle (secondes)",
        options=[10, 30, 60, 120],
        index=1,
        disabled=not auto_refresh
    )
    
    if auto_refresh:
        st.info(f"Refresh toutes les {refresh_interval}s")
        time.sleep(0.1)
        st.rerun()
    
    st.divider()
    
    if st.button("🔄 Rafraîchir maintenant", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.divider()
    
    st.subheader("⏱️ Métriques de performance")
    metrics_data, _ = fetch_data("/kpis/refresh-metrics")
    if metrics_data:
        st.metric("Temps requête MongoDB", f"{metrics_data['query_time_ms']:.1f} ms")
        st.metric("Temps total API", f"{metrics_data['total_time_ms']:.1f} ms")


st.title("📊 E-commerce Analytics Dashboard")
st.caption(f"Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if not api_status:
    st.warning("⚠️ Dashboard en mode dégradé - API non disponible")
    st.stop()

st.header("📈 KPIs Globaux")

kpis_data, kpi_time = fetch_data("/kpis")

if kpis_data:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="👥 Total Clients",
            value=f"{kpis_data['total_clients']:,}"
        )
    
    with col2:
        st.metric(
            label="🛒 Total Achats",
            value=f"{kpis_data['total_achats']:,}"
        )
    
    with col3:
        st.metric(
            label="💰 Chiffre d'Affaires",
            value=f"{kpis_data['chiffre_affaires']:,.2f} €"
        )
    
    with col4:
        st.metric(
            label="🧺 Panier Moyen",
            value=f"{kpis_data['panier_moyen']:.2f} €"
        )
    
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            label="🏆 Top Pays",
            value=kpis_data['top_pays']
        )
    
    with col6:
        st.metric(
            label="⭐ Top Produit",
            value=kpis_data['top_produit']
        )
    
    with col7:
        st.metric(
            label="⏱️ Temps API",
            value=f"{kpi_time:.0f} ms"
        )

st.divider()

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📅 Évolution des Ventes Mensuelles")
    
    monthly_data, _ = fetch_data("/sales/monthly")
    
    if monthly_data and monthly_data.get("data"):
        df_monthly = pd.DataFrame(monthly_data["data"])
        
        fig_monthly = px.line(
            df_monthly,
            x="annee_mois",
            y="revenu_total",
            markers=True,
            title="Revenu mensuel"
        )
        fig_monthly.update_layout(
            xaxis_title="Mois",
            yaxis_title="Revenu (€)",
            hovermode="x unified"
        )
        st.plotly_chart(fig_monthly, use_container_width=True)
        
        with st.expander("📊 Détails mensuels"):
            st.dataframe(
                df_monthly[["annee_mois", "nb_ventes", "revenu_total", "panier_moyen", "top_produit"]],
                use_container_width=True,
                hide_index=True
            )

with col_right:
    st.subheader("🏆 Top Produits")
    
    products_data, _ = fetch_data("/products/top?limit=10")
    
    if products_data:
        df_products = pd.DataFrame(products_data)
        
        fig_products = px.bar(
            df_products,
            x="revenu_total",
            y="produit",
            orientation="h",
            title="Top 10 produits par revenu",
            color="revenu_total",
            color_continuous_scale="Blues"
        )
        fig_products.update_layout(
            yaxis={"categoryorder": "total ascending"},
            xaxis_title="Revenu (€)",
            yaxis_title="Produit",
            showlegend=False
        )
        st.plotly_chart(fig_products, use_container_width=True)

st.divider()

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("🌍 Répartition par Pays")
    
    country_data, _ = fetch_data("/sales/by-country")
    
    if country_data:
        df_country = pd.DataFrame(country_data)
        
        fig_country = px.pie(
            df_country,
            values="revenu_total",
            names="pays",
            title="Répartition du CA par pays",
            hole=0.4
        )
        fig_country.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_country, use_container_width=True)

with col_right2:
    st.subheader("👥 Top Clients")
    
    clients_data, _ = fetch_data("/clients/top?limit=10&by=montant_total")
    
    if clients_data:
        df_clients = pd.DataFrame(clients_data)
        
        fig_clients = px.bar(
            df_clients,
            x="nom",
            y="montant_total",
            title="Top 10 clients par montant total",
            color="nb_achats",
            color_continuous_scale="Viridis"
        )
        fig_clients.update_layout(
            xaxis_title="Client",
            yaxis_title="Montant Total (€)",
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig_clients, use_container_width=True)

st.divider()

st.header("📋 Données Détaillées")

tab1, tab2, tab3 = st.tabs(["👥 Clients", "📦 Produits", "🌍 Pays"])

with tab1:
    st.subheader("Liste des Clients")
    
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        page = st.number_input("Page", min_value=1, value=1)
    with col_filter2:
        page_size = st.selectbox("Clients par page", [10, 20, 50], index=1)
    
    clients_list, _ = fetch_data(f"/clients?page={page}&page_size={page_size}")
    
    if clients_list and clients_list.get("data"):
        st.caption(f"Total: {clients_list['total']} clients | Page {page}")
        df_clients_list = pd.DataFrame(clients_list["data"])
        st.dataframe(
            df_clients_list[["id_client", "nom", "email", "pays", "nb_achats", "montant_total", "produit_favori"]],
            use_container_width=True,
            hide_index=True
        )

with tab2:
    st.subheader("Tous les Produits")
    
    all_products, _ = fetch_data("/products")
    
    if all_products and all_products.get("data"):
        df_all_products = pd.DataFrame(all_products["data"])
        st.dataframe(
            df_all_products,
            use_container_width=True,
            hide_index=True
        )

with tab3:
    st.subheader("Statistiques par Pays")
    
    all_countries, _ = fetch_data("/sales/by-country")
    
    if all_countries:
        df_all_countries = pd.DataFrame(all_countries)
        st.dataframe(
            df_all_countries,
            use_container_width=True,
            hide_index=True
        )

st.divider()
st.caption("📊 E-commerce Analytics Dashboard | Données depuis MongoDB via FastAPI")
