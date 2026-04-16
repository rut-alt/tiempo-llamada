import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Primera llamada por lead (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta la primera llamada saliente (desde creación del negocio)")

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"

WORK_START_HOUR = 9

def adjust_creation_time(ts: pd.Timestamp) -> pd.Timestamp:
    if pd.isna(ts):
        return ts
    if ts.hour < WORK_START_HOUR:
        return ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    return ts

def format_duration_exact(seconds: float) -> str:
    if pd.isna(seconds):
        return ""
    sign = "-" if seconds < 0 else ""
    seconds = abs(int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    if days > 0:
        return f"{sign}{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"

def compute_first_outbound_call(df: pd.DataFrame):
    df = df.copy()

    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()

    # Solo filas válidas
    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()

    # Solo llamadas salientes
    df = df[df[COL_SUBJECT].str.contains(r"llamada saliente", case=False, na=False)].copy()

    # Ajuste horario creación
    df["created_adjusted"] = df[COL_CREATED].apply(adjust_creation_time)

    # Solo llamadas posteriores a la creación
    df["delta_sec"] = (df[COL_DUE_DATE] - df["created_adjusted"]).dt.total_seconds()
    df = df[df["delta_sec"] >= 0].copy()

    # ORDENAR por negocio + fecha llamada ascendente
    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).copy()

    # QUEDARSE CON LA PRIMERA llamada saliente de cada negocio
    first_calls = df.drop_duplicates(subset=[COL_DEAL_ID], keep="first").copy()

    first_calls = first_calls.rename(columns={
        COL_DUE_DATE: "first_call_time",
        COL_SUBJECT: "first_call_subject"
    })

    keep_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        "created_adjusted",
        "first_call_time",
        "first_call_subject",
        "delta_sec"
    ]

    if COL_OWNER in first_calls.columns:
        keep_cols.append(COL_OWNER)

    res = first_calls[keep_cols].copy()
    res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    res = res.sort_values(COL_CREATED).reset_index(drop=True)

    if COL_OWNER in res.columns:
        agent_stats = (
            res.groupby(COL_OWNER)
            .agg(
                leads=(COL_DEAL_ID, "count"),
                media_seg=("delta_sec", "mean"),
                mediana_seg=("delta_sec", "median"),
            )
            .reset_index()
        )
        agent_stats["media"] = agent_stats["media_seg"].apply(format_duration_exact)
        agent_stats["mediana"] = agent_stats["mediana_seg"].apply(format_duration_exact)
        agent_stats = agent_stats.sort_values("media_seg")
    else:
        agent_stats = pd.DataFrame()

    media_total = format_duration_exact(res["delta_sec"].mean()) if len(res) else ""
    mediana_total = format_duration_exact(res["delta_sec"].median()) if len(res) else ""

    return res, agent_stats, media_total, mediana_total, df

def to_excel_bytes(res: pd.DataFrame, agent_stats: pd.DataFrame, debug_calls: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="primera_llamada_por_negocio")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        debug_calls.to_excel(writer, index=False, sheet_name="debug_llamadas_filtradas")
    return output.getvalue()

if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    missing = [c for c in [COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT] if c not in df.columns]
    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    res, agent_stats, media_total, mediana_total, debug_calls = compute_first_outbound_call(df)

    col1, col2 = st.columns(2)
    col1.metric("Media total", media_total)
    col2.metric("Mediana total", mediana_total)

    st.subheader("✅ Primera llamada saliente por negocio")
    st.dataframe(res, use_container_width=True)

    st.subheader("🔎 Debug: llamadas salientes filtradas y ordenadas")
    st.dataframe(
        debug_calls[[COL_DEAL_ID, COL_CREATED, "created_adjusted", COL_DUE_DATE, COL_SUBJECT, "delta_sec"]],
        use_container_width=True
    )

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente")
        st.dataframe(agent_stats[[COL_OWNER, "leads", "media", "mediana"]], use_container_width=True)

    xlsx_bytes = to_excel_bytes(res, agent_stats, debug_calls)
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name="primera_llamada_saliente_por_negocio_y_agente.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel para calcular la primera llamada saliente por negocio.")
