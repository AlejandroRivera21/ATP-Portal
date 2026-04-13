import streamlit as st
from elasticsearch import Elasticsearch
import os
from datetime import date
import pandas as pd

# ----------------------------
# Configuración de la página
# ----------------------------
st.set_page_config(
    page_title="Portal de Monitoreo ATP",
    layout="wide"
)

# ----------------------------
# Título principal
# ----------------------------
st.title("Portal de Monitoreo ATP")
st.subheader("KPIs calculados en Elasticsearch")

# ----------------------------
# Conexión a Elasticsearch
# ----------------------------
def conectar_elasticsearch():
    url = os.getenv("ES_URL", "http://localhost:9200")
    return Elasticsearch(url, verify_certs=False)

# ----------------------------
# Sidebar: filtros
# ----------------------------
st.sidebar.header("Filtros")

operador = st.sidebar.selectbox(
    "Operador",
    ["Todos", "CLARO", "ETB"]
)

fecha_inicio = st.sidebar.date_input(
    "Fecha inicio",
    value=date.today()
)

fecha_fin = st.sidebar.date_input(
    "Fecha fin",
    value=date.today()
)

# ----------------------------
# KPIs + Percentiles
# ----------------------------
def obtener_kpis_y_percentiles(operador, fecha_inicio, fecha_fin):
    es = conectar_elasticsearch()
    filtros = []

    if operador != "Todos":
        filtros.append({"term": {"operador.keyword": operador}})

    filtros.append({
        "range": {
            "@timestamp": {
                "gte": str(fecha_inicio),
                "lte": str(fecha_fin)
            }
        }
    })

    query = {
        "size": 0,
        "query": {"bool": {"filter": filtros}},
        "aggs": {
            "por_resultado": {
                "terms": {"field": "resultado.keyword"}
            },
            "latencia": {
                "percentiles": {
                    "field": "duracion_ms",
                    "percents": [50, 95, 99]
                }
            }
        }
    }

    r = es.search(index="*", body=query)
    buckets = r["aggregations"]["por_resultado"]["buckets"]

    kpis = {"total": 0, "ok": 0, "error": 0, "timeout": 0}
    for b in buckets:
        kpis["total"] += b["doc_count"]
        if b["key"] == "OK":
            kpis["ok"] = b["doc_count"]
        elif b["key"] == "ERROR":
            kpis["error"] = b["doc_count"]
        elif b["key"] == "TIMEOUT":
            kpis["timeout"] = b["doc_count"]

    p = r["aggregations"]["latencia"]["values"]
    percentiles = {
        "p50": p.get("50.0"),
        "p95": p.get("95.0"),
        "p99": p.get("99.0")
    }

    return kpis, percentiles

# ----------------------------
# Fallback
# ----------------------------
def obtener_fallback():
    kpis = {"total": 5, "ok": 3, "error": 1, "timeout": 1}
    percentiles = {"p50": 120, "p95": 450, "p99": 900}
    return kpis, percentiles

# ----------------------------
# Datos principales
# ----------------------------
try:
    kpis, percentiles = obtener_kpis_y_percentiles(
        operador, fecha_inicio, fecha_fin
    )
    fuente_datos = "Elasticsearch"
except Exception:
    kpis, percentiles = obtener_fallback()
    fuente_datos = "Fallback (simulado)"

st.caption(f"Fuente de datos: {fuente_datos}")

# ----------------------------
# Calcular SLA
# ----------------------------
sla = (kpis["ok"] / kpis["total"] * 100) if kpis["total"] else 100

# ----------------------------
# Alertas (T‑06)
# ----------------------------
if sla < 95:
    st.error("🔴 SLA por debajo del 95%. Riesgo operativo.")
elif sla < 98:
    st.warning("🟡 SLA en riesgo.")
else:
    st.success("🟢 SLA OK.")

if kpis["timeout"] > 0:
    st.warning(f"⚠️ Se detectaron {kpis['timeout']} timeouts.")

# ----------------------------
# KPIs
# ----------------------------
st.markdown("### KPIs actuales")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Transacciones", kpis["total"])
c2.metric("SLA %", f"{sla:.2f}")
c3.metric("Errores Técnicos", kpis["error"])
c4.metric("Timeouts", kpis["timeout"])

st.markdown("### Latencia (ms)")
p1, p2, p3 = st.columns(3)
p1.metric("p50", percentiles["p50"])
p2.metric("p95", percentiles["p95"])
p3.metric("p99", percentiles["p99"])

# ----------------------------
# T‑08: Histórico SLA (fallback)
# ----------------------------
st.markdown("### Histórico de SLA")

# Datos simulados para histórico
hist_df = pd.DataFrame({
    "Fecha": pd.date_range("2026-04-10", periods=4),
    "SLA": [96, 93, 97, sla]
})

st.line_chart(hist_df.set_index("Fecha"))
