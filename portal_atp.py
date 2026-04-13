import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta

# --------------------------------------------------
# CONFIGURACIÓN DE PÁGINA
# --------------------------------------------------
st.set_page_config(
    page_title="ATP — Portal Orquestador",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------
# LOGO ATP (sidebar)
# --------------------------------------------------
for ruta in ["assets/logo_ATP.png", "logo_ATP.png"]:
    if os.path.exists(ruta):
        st.sidebar.image(ruta, width=150)
        break

# --------------------------------------------------
# TÍTULO
# --------------------------------------------------
st.title("ATP — Portal Orquestador")
st.caption("Monitoreo operativo — CLARO / ETB")

# --------------------------------------------------
# DATOS FALLBACK (SIMULADOS)
# --------------------------------------------------
def get_fallback_data():
    now = datetime.now()
    rows = []
    for i in range(120):
        rows.append({
            "timestamp": now - timedelta(minutes=i * 5),
            "resultado": "OK" if i % 10 != 0 else "ERROR",
            "duracion_ms": 120 + (i * 7) % 350,
            "operador": "CLARO" if i % 2 == 0 else "ETB"
        })
    return pd.DataFrame(rows)

df = get_fallback_data()

# --------------------------------------------------
# SIDEBAR — FILTROS
# --------------------------------------------------
st.sidebar.subheader("Filtros")

operador = st.sidebar.selectbox(
    "Operador",
    ["ATP", "CLARO", "ETB"]
)

horas = st.sidebar.slider(
    "Últimas horas",
    min_value=1,
    max_value=48,
    value=12
)

st.sidebar.markdown("---")
st.sidebar.warning("🟡 Usando datos simulados")

# --------------------------------------------------
# APLICAR FILTROS
# --------------------------------------------------
df["timestamp"] = pd.to_datetime(df["timestamp"])

limite = datetime.now() - timedelta(hours=horas)
df_f = df[df["timestamp"] >= limite]

if operador != "ATP":
    df_f = df_f[df_f["operador"] == operador]

# --------------------------------------------------
# KPIs
# --------------------------------------------------
total = len(df_f)
ok = len(df_f[df_f["resultado"] == "OK"])
errores = len(df_f[df_f["resultado"] == "ERROR"])
sla = round((ok / total) * 100, 2) if total else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Transacciones", total)
c2.metric("SLA %", sla)
c3.metric("Errores", errores)
c4.metric("OK", ok)

# --------------------------------------------------
# ALERTAS SLA
# --------------------------------------------------
if sla < 95:
    st.error("🔴 SLA CRÍTICO")
elif sla < 98:
    st.warning("🟡 SLA EN RIESGO")
else:
    st.success("🟢 SLA OK")

# --------------------------------------------------
# GRÁFICA — DISTRIBUCIÓN DE RESULTADOS
# --------------------------------------------------
st.subheader("Distribución de Resultados")

fig1 = px.histogram(
    df_f,
    x="resultado",
    color="resultado",
    title="Distribución de Resultados"
)

st.plotly_chart(fig1, use_container_width=True)

# --------------------------------------------------
# GRÁFICA — LATENCIA
# --------------------------------------------------
st.subheader("Latencia (ms)")

fig2 = px.box(
    df_f,
    y="duracion_ms",
    title="Distribución de Latencia"
)

st.plotly_chart(fig2, use_container_width=True)

# --------------------------------------------------
# HISTÓRICO SLA (CORREGIDO ✅)
# --------------------------------------------------
st.subheader("Histórico SLA")

sla_hist = (
    df_f
    .assign(hora=df_f["timestamp"].dt.floor("h"))  # ✅ CORRECCIÓN AQUÍ
    .groupby("hora")
    .apply(lambda x: round((x["resultado"] == "OK").mean() * 100, 2))
    .reset_index(name="SLA")
)

fig3 = px.line(
    sla_hist,
    x="hora",
    y="SLA",
    title="SLA por hora"
)

st.plotly_chart(fig3, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------
st.markdown("---")
st.caption("ATP Fiber Colombia S.A.S — Portal Orquestador")