import io
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Primera llamada por lead (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta la primera llamada (desde creación del negocio)")
st.write(
    "Sube un Excel exportado de Pipedrive (Actividades) filtrado a **tipo = llamada**. "
    "La app calcula la **primera llamada por ID de negocio** (la más cercana y posterior a la creación) "
    "y el **tiempo medio por agente**. "
    "Incluye ajuste para leads creados antes de la hora de inicio (por defecto, 09:00)."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])

# Ajusta estos nombres si en tu Excel son distintos
COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_CALL_DONE = "Actividad - Hora en que se marcó como completada"
COL_OWNER = "Negocio - Propietario"

# ✅ Ajuste de horario: si el lead se crea antes de esta hora, el reloj empieza a contar a esta hora
WORK_START_HOUR = 9

def adjust_creation_time(ts: pd.Timestamp) -> pd.Timestamp:
    """Si se crea antes de WORK_START_HOUR, ajusta a las WORK_START_HOUR:00:00 del mismo día."""
    if pd.isna(ts):
        return ts
    if ts.hour < WORK_START_HOUR:
        return ts.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    return ts

def format_duration_exact(seconds: float) -> str:
    """
    Formatea sin redondear:
      - si hay días:   '2d 03:04:05'
      - si no:         '03:04:05'
    """
    if pd.isna(seconds):
        return ""
    sign = "-" if seconds < 0 else ""
    seconds = abs(seconds)

    total_seconds = int(seconds)  # NO redondeamos

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    if days > 0:
        return f"{sign}{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"

def compute_first_call(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    df = df.copy()

    # Normalizar tipos
    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_CALL_DONE] = pd.to_datetime(df[COL_CALL_DONE], errors="coerce")

    # 1) Filtro: ID, creación y hora completada NO vacíos
    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_CALL_DONE])

    # 2) Ajuste horario para leads de madrugada (creación ajustada)
    df["created_adjusted"] = df[COL_CREATED].apply(adjust_creation_time)

    # 3) Delta segundos (llamada - creación ajustada)
    df["delta_sec"] = (df[COL_CALL_DONE] - df["created_adjusted"]).dt.total_seconds()

    # 4) Solo llamadas posteriores o iguales a la creación ajustada
    df = df[df["delta_sec"] >= 0]

    # 5) Primera llamada por negocio = delta mínimo
    def pick_first(group: pd.DataFrame) -> pd.Series:
        if len(group) == 0:
            return pd.Series({"first_call_time": pd.NaT, "delta_sec": np.nan})
        idx = group["delta_sec"].idxmin()
        return pd.Series(
            {
                "first_call_time": group.loc[idx, COL_CALL_DONE],
                "delta_sec": group.loc[idx, "delta_sec"],
            }
        )

    first = df.groupby(COL_DEAL_ID).apply(pick_first).reset_index()

    # Cabecera por negocio (creación original, creación ajustada y propietario)
    created = df.groupby(COL_DEAL_ID)[COL_CREATED].min().reset_index()
    created_adj = df.groupby(COL_DEAL_ID)["created_adjusted"].min().reset_index()

    owners = None
    if COL_OWNER in df.columns:
        owners = (
            df.groupby(COL_DEAL_ID)[COL_OWNER]
            .agg(lambda s: s.dropna().iloc[0] if len(s.dropna()) else np.nan)
            .reset_index()
        )

    # Resultado final: 1 fila por negocio
    res = created.merge(created_adj, on=COL_DEAL_ID, how="left").merge(first, on=COL_DEAL_ID, how="left")
    if owners is not None:
        res = res.merge(owners, on=COL_DEAL_ID, how="left")

    res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)

    # Resumen por agente (solo con la primera llamada por negocio)
    if owners is not None:
        agent_stats = (
            res.dropna(subset=["delta_sec"])
            .groupby(COL_OWNER)
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

    media_total = format_duration_exact(res["delta_sec"].mean())
    mediana_total = format_duration_exact(res["delta_sec"].median())

    return res, agent_stats, media_total, mediana_total

def to_excel_bytes(res: pd.DataFrame, agent_stats: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="primera_llamada_por_negocio")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
    return output.getvalue()

if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    # Validación de columnas mínimas
    missing = [c for c in [COL_DEAL_ID, COL_CREATED, COL_CALL_DONE] if c not in df.columns]
    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    res, agent_stats, media_total, mediana_total = compute_first_call(df)

    col1, col2 = st.columns(2)
    col1.metric("Media total (tiempo hasta 1ª llamada, ajustado a horario)", media_total)
    col2.metric("Mediana total (ajustado a horario)", mediana_total)

    st.subheader("✅ Primera llamada por negocio (1 fila por ID)")
    st.dataframe(res, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente")
        st.dataframe(agent_stats[[COL_OWNER, "leads", "media", "mediana"]], use_container_width=True)

    xlsx_bytes = to_excel_bytes(res, agent_stats)
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name="primera_llamada_por_negocio_y_agente.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel para calcular la primera llamada por negocio y el tiempo medio por agente.")
