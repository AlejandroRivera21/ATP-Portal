import streamlit as st
from elasticsearch import Elasticsearch
import os

# ----------------------------
# Configuración de la página
# ----------------------------
st.set_page_config(
    page_title="Portal de Monitoreo ATP",
    layout="wide"
)

# ----------------------------
# Conexión a Elasticsearch
# ----------------------------
def conectar_elasticsearch():
    url = os.getenv("ES_URL", "http://localhost:9200")
    return Elasticsearch(url, verify_certs=False)

# ----------------------------
# Obtener KPIs desde Elasticsearch
# ----------------------------
def obtener_kpis_desde_es():
    es = conectar_elasticsearch()

    query = {
        "size": 0,
        "aggs": {
            "por_resultado": {
                "terms": {
                    "field": "resultado.keyword",
                    "size": 10
                }
            }
        }
    }

    response = es.search(index="*", body=query)

    buckets = response["aggregations"]["por_resultado"]["buckets"]

    kpis = {
        "total": 0,
        "ok": 0,
        "error": 0,
        "timeout": 0
    }

    for b in buckets:
        estado = b["key"]
        cantidad = b["doc_count"]
        kpis["total"] += cantidad

        if estado == "OK":
            kpis["ok"] = cantidad
        elif estado == "ERROR":
            kpis["error"] = cantidad
        elif estado == "TIMEOUT":
            kpis["timeout"] = cantidad

    return kpis

# ----------------------------
# Fallback si Elasticsearch falla
# ----------------------------
def obtener_kpis_fallback():
    return {
        "total": 5,
        "ok": 3,
        "error": 1,
        "timeout": 1
    }

# ----------------------------
# Cargar KPIs (ES o Fallback)
# ----------------------------
try:
    kpis = obtener_kpis_desde_es()
    fuente_datos = "Elasticsearch"
except Exception:
    kpis = obtener_kpis_fallback()
    fuente_datos = "Fallback (simulado)"

# ----------------------------
# Calcular SLA
# ----------------------------
if kpis["total"] > 0:
    sla = (kpis["ok"] / kpis["total"]) * 100
else:
    sla = 100

# ----------------------------
# UI Streamlit
# ----------------------------
st.title("Portal de Monitoreo ATP")
st.subheader("KPIs calculados en Elasticsearch")
st.caption(f"Fuente de datos: {fuente_datos}")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Transacciones", kpis["total"])
col2.metric("SLA %", f"{sla:.2f}%")
col3.metric("Errores Técnicos", kpis["error"])
col4.metric("Timeouts", kpis["timeout"])
