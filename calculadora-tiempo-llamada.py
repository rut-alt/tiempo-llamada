import io
import requests
import pandas as pd
import streamlit as st
import unicodedata
from datetime import time

st.set_page_config(page_title="Tiempo hasta la primera llamada por asignación (Pipedrive)", layout="wide")

st.title("📞 Tiempo hasta la primera llamada por asignación")
st.write(
    "Sube un Excel exportado de Pipedrive (Actividades). "
    "La app calcula, para cada negocio y para cada tramo de asignación, "
    "el tiempo hasta la primera llamada/contacto del agente asignado tras esa asignación. "
    "El owner real de cada actividad se reconstruye usando el flow del deal."
)

uploaded = st.file_uploader("Sube tu Excel (.xlsx)", type=["xlsx"])
apply_filter_1day = st.checkbox("Excluir tramos cuyo primer contacto tarde 1 día o más", value=False)
hide_segments_without_contact = st.checkbox("Ocultar tramos sin contacto", value=False)

contact_mode = st.radio(
    "Qué quieres medir",
    ["Primera llamada saliente", "Primer contacto (llamada + WhatsApp)"],
    horizontal=True,
)

st.subheader("🔐 Configuración API Pipedrive")
api_token = st.text_input("API token", type="password")
company_domain = st.text_input("Subdominio de Pipedrive", placeholder="tuempresa")

COL_DEAL_ID = "Negocio - ID"
COL_CREATED = "Negocio - Negocio creado el"
COL_DUE_DATE = "Actividad - Fecha de vencimiento"
COL_SUBJECT = "Actividad - Asunto"
COL_OWNER = "Negocio - Propietario"
COL_ACTIVITY_OWNER = "Actividad - Asignada al usuario"  # solo debug, no verdad analítica

ONE_DAY_SECONDS = 86400
LOCAL_TIMEZONE = "Europe/Madrid"

HOLIDAYS_2026 = {
    pd.Timestamp("2026-01-01").date(),
    pd.Timestamp("2026-01-06").date(),
    pd.Timestamp("2026-04-03").date(),
    pd.Timestamp("2026-05-01").date(),
    pd.Timestamp("2026-08-15").date(),
    pd.Timestamp("2026-10-12").date(),
    pd.Timestamp("2026-11-01").date(),
    pd.Timestamp("2026-12-08").date(),
    pd.Timestamp("2026-12-25").date(),
}


def clean_text(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def normalize_name(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    text = str(value).strip().lower()
    text = strip_accents(text)
    text = " ".join(text.split())
    return text


def canonical_agent_name(value) -> str:
    norm = normalize_name(value)

    alias_map = {
        "toni": "antonia campos gil",
        "toni ": "antonia campos gil",
        "toñi": "antonia campos gil",
        "antonia campos gil": "antonia campos gil",
        "antonia  campos gil": "antonia campos gil",

        "mayra diaz": "mayra diaz",
        "mayra alejandra diaz": "mayra diaz",

        "isabel tortosa": "isabel tortosa vivas",
        "isabel tortosa vivas": "isabel tortosa vivas",
    }

    return alias_map.get(norm, norm)


def to_madrid_ts(value):
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if isinstance(ts, pd.Series):
        return ts.dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)
    return ts.tz_convert(LOCAL_TIMEZONE).tz_localize(None) if pd.notna(ts) else pd.NaT


TEAM_SCHEDULE = {
    0: [(time(9, 0), time(20, 0))],
    1: [(time(9, 0), time(20, 0))],
    2: [(time(9, 0), time(20, 0))],
    3: [(time(9, 0), time(20, 0))],
    4: [(time(9, 0), time(20, 0))],
    5: [(time(12, 30), time(20, 0))],
}


def is_holiday(ts: pd.Timestamp) -> bool:
    return ts.date() in HOLIDAYS_2026


def get_day_windows(ts: pd.Timestamp):
    if is_holiday(ts):
        return []

    weekday = ts.weekday()
    windows = TEAM_SCHEDULE.get(weekday, [])

    return [
        (
            pd.Timestamp.combine(ts.date(), start_t),
            pd.Timestamp.combine(ts.date(), end_t),
        )
        for start_t, end_t in windows
    ]


def move_to_next_work_moment(ts: pd.Timestamp) -> pd.Timestamp:
    cur = ts

    for _ in range(370):
        windows = get_day_windows(cur)

        if not windows:
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        for start_dt, end_dt in windows:
            if cur <= start_dt:
                return start_dt
            if start_dt <= cur < end_dt:
                return cur

        cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
        cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)

    return ts


def business_seconds_between(start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> float:
    if pd.isna(start_ts) or pd.isna(end_ts):
        return float("nan")
    if end_ts < start_ts:
        return float("nan")

    cur = move_to_next_work_moment(start_ts)
    total_seconds = 0.0

    for _ in range(370):
        if cur >= end_ts:
            break

        windows = get_day_windows(cur)
        if not windows:
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        progressed = False

        for start_dt, end_dt in windows:
            if cur < start_dt:
                cur = start_dt

            if start_dt <= cur < end_dt:
                segment_end = min(end_dt, end_ts)
                total_seconds += (segment_end - cur).total_seconds()
                cur = segment_end
                progressed = True

                if cur >= end_ts:
                    break

        if cur >= end_ts:
            break

        if not progressed or all(cur >= end_dt for _, end_dt in windows):
            cur = pd.Timestamp(cur.date()) + pd.Timedelta(days=1)
            cur = cur.replace(hour=0, minute=0, second=0, microsecond=0)

    return total_seconds


def format_duration_exact(seconds: float) -> str:
    if pd.isna(seconds):
        return ""
    sign = "-" if seconds < 0 else ""
    total_seconds = abs(int(seconds))

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)

    if days > 0:
        return f"{sign}{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"


@st.cache_data(show_spinner=False)
def fetch_deal_flow(_api_token: str, _company_domain: str, deal_id: int) -> dict:
    url = f"https://{_company_domain}.pipedrive.com/api/v1/deals/{deal_id}/flow?api_token={_api_token}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_activity_filter_pattern(selected_mode: str) -> str:
    if selected_mode == "Primera llamada saliente":
        return r"llamada saliente"
    return r"llamada saliente|whatsapp chat"


def get_result_labels(selected_mode: str):
    if selected_mode == "Primera llamada saliente":
        return {
            "title": "✅ Primera llamada por tramo de asignación",
            "metric_count": "Tramos con 1ª llamada",
            "download_name": "primera_llamada_por_asignacion.xlsx",
            "time_col": "tiempo_hasta_primera_llamada",
        }
    return {
        "title": "✅ Primer contacto por tramo de asignación",
        "metric_count": "Tramos con 1er contacto",
        "download_name": "primer_contacto_por_asignacion.xlsx",
        "time_col": "tiempo_hasta_primer_contacto",
    }


def extract_owner_changes(flow_json: dict) -> pd.DataFrame:
    rows = []

    for item in flow_json.get("data", []) or []:
        obj = item.get("object")
        data = item.get("data", {}) or {}

        if obj == "dealChange" and data.get("field_key") == "user_id":
            event_time = to_madrid_ts(data.get("log_time"))
            old_owner = clean_text((data.get("additional_data") or {}).get("old_value_formatted"))
            new_owner = clean_text((data.get("additional_data") or {}).get("new_value_formatted"))

            if pd.notna(event_time):
                rows.append({
                    "event_time": event_time,
                    "old_owner": old_owner,
                    "new_owner": new_owner,
                    "old_owner_canonical": canonical_agent_name(old_owner),
                    "new_owner_canonical": canonical_agent_name(new_owner),
                })

    if not rows:
        return pd.DataFrame(columns=[
            "event_time", "old_owner", "new_owner",
            "old_owner_canonical", "new_owner_canonical"
        ])

    return pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)


def build_assignment_segments_for_deal(
    deal_id: int,
    deal_created: pd.Timestamp,
    initial_owner: str,
    flow_json: dict
) -> pd.DataFrame:
    owner_changes = extract_owner_changes(flow_json)
    rows = []

    initial_owner = clean_text(initial_owner)

    if initial_owner:
        rows.append({
            "deal_id": deal_id,
            "segment_start": deal_created,
            "agent_owner_raw": initial_owner,
            "agent_owner": canonical_agent_name(initial_owner),
            "segment_source": "deal_created",
            "from_owner": "",
            "to_owner": initial_owner,
        })

    for _, ch in owner_changes.iterrows():
        new_owner = clean_text(ch["new_owner"])
        old_owner = clean_text(ch["old_owner"])
        event_time = ch["event_time"]

        if new_owner and pd.notna(event_time):
            rows.append({
                "deal_id": deal_id,
                "segment_start": event_time,
                "agent_owner_raw": new_owner,
                "agent_owner": canonical_agent_name(new_owner),
                "segment_source": "owner_reassignment",
                "from_owner": old_owner,
                "to_owner": new_owner,
            })

    if not rows:
        return pd.DataFrame(columns=[
            "deal_id", "segment_start", "agent_owner_raw",
            "agent_owner", "segment_source", "from_owner", "to_owner"
        ])

    seg = pd.DataFrame(rows).sort_values("segment_start").reset_index(drop=True)

    seg["prev_owner"] = seg["agent_owner"].shift(1)
    seg = seg[(seg.index == 0) | (seg["agent_owner"] != seg["prev_owner"])].copy()
    seg = seg.drop(columns=["prev_owner"]).reset_index(drop=True)

    seg["segment_end"] = seg["segment_start"].shift(-1)

    return seg


def prepare_activities(df: pd.DataFrame, selected_mode: str) -> pd.DataFrame:
    df = df.copy()

    df[COL_DEAL_ID] = pd.to_numeric(df[COL_DEAL_ID], errors="coerce").astype("Int64")
    df[COL_CREATED] = pd.to_datetime(df[COL_CREATED], errors="coerce")
    df[COL_DUE_DATE] = pd.to_datetime(df[COL_DUE_DATE], errors="coerce")
    df[COL_SUBJECT] = df[COL_SUBJECT].astype(str).str.strip()
    df[COL_OWNER] = df[COL_OWNER].apply(clean_text)

    if COL_ACTIVITY_OWNER in df.columns:
        df[COL_ACTIVITY_OWNER] = df[COL_ACTIVITY_OWNER].apply(clean_text)
    else:
        df[COL_ACTIVITY_OWNER] = ""

    df = df.dropna(subset=[COL_DEAL_ID, COL_CREATED, COL_DUE_DATE, COL_SUBJECT]).copy()

    df["deal_owner"] = df[COL_OWNER]
    df["activity_owner_raw_excel"] = df[COL_ACTIVITY_OWNER]

    df["has_any_call"] = df[COL_SUBJECT].str.contains(
        r"llamada saliente|llamada entrante",
        case=False,
        na=False
    )

    deals_with_call = df.loc[df["has_any_call"], COL_DEAL_ID].dropna().unique()
    df = df[df[COL_DEAL_ID].isin(deals_with_call)].copy()

    pattern = get_activity_filter_pattern(selected_mode)
    df = df[df[COL_SUBJECT].str.contains(pattern, case=False, na=False)].copy()

    df = df.sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]).reset_index(drop=True)
    return df


def assign_real_owner_to_activities(deal_activities: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    deal_activities = deal_activities.copy()
    deal_activities["real_owner"] = pd.NA
    deal_activities["real_owner_raw"] = pd.NA
    deal_activities["segment_index_owner"] = pd.NA

    if deal_activities.empty or segments.empty:
        return deal_activities

    for idx, seg in segments.iterrows():
        start = seg["segment_start"]
        end = seg["segment_end"]
        owner = seg["agent_owner"]
        owner_raw = seg["agent_owner_raw"]
        segment_index = idx + 1

        if pd.isna(end):
            mask = deal_activities[COL_DUE_DATE] >= start
        else:
            mask = (
                (deal_activities[COL_DUE_DATE] >= start) &
                (deal_activities[COL_DUE_DATE] < end)
            )

        deal_activities.loc[mask, "real_owner"] = owner
        deal_activities.loc[mask, "real_owner_raw"] = owner_raw
        deal_activities.loc[mask, "segment_index_owner"] = segment_index

    return deal_activities


def compute_by_assignment(df: pd.DataFrame, apply_filter_1day: bool, selected_mode: str):
    activities = prepare_activities(df, selected_mode)

    if activities.empty:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            "",
            "",
            activities,
            pd.DataFrame(),
            pd.DataFrame(),
        )

    deal_base = (
        activities.groupby(COL_DEAL_ID, dropna=False)
        .agg(
            deal_created=(COL_CREATED, "min"),
            initial_owner=("deal_owner", "first"),
        )
        .reset_index()
    )

    rows = []
    all_segments = []
    activities_with_real_owner = []

    progress = st.progress(0)
    total_deals = len(deal_base)

    for i, (_, deal_row) in enumerate(deal_base.iterrows(), start=1):
        deal_id = int(deal_row[COL_DEAL_ID])
        deal_created = deal_row["deal_created"]
        initial_owner = clean_text(deal_row["initial_owner"])

        try:
            flow_json = fetch_deal_flow(api_token, company_domain, deal_id)
            segments = build_assignment_segments_for_deal(
                deal_id=deal_id,
                deal_created=deal_created,
                initial_owner=initial_owner,
                flow_json=flow_json
            )
        except Exception:
            segments = pd.DataFrame()

        if segments.empty:
            if initial_owner:
                segments = pd.DataFrame([{
                    "deal_id": deal_id,
                    "segment_start": deal_created,
                    "segment_end": pd.NaT,
                    "agent_owner_raw": initial_owner,
                    "agent_owner": canonical_agent_name(initial_owner),
                    "segment_source": "deal_created",
                    "from_owner": "",
                    "to_owner": initial_owner,
                }])
            else:
                progress.progress(i / total_deals if total_deals else 1)
                continue

        all_segments.append(segments.copy())

        deal_acts = activities[activities[COL_DEAL_ID] == deal_id].copy()
        deal_acts = assign_real_owner_to_activities(deal_acts, segments)
        activities_with_real_owner.append(deal_acts.copy())

        for seg_idx, seg in segments.iterrows():
            agent_owner = clean_text(seg["agent_owner"])
            agent_owner_raw = clean_text(seg["agent_owner_raw"])
            segment_start = seg["segment_start"]
            segment_end = seg["segment_end"]
            segment_source = seg["segment_source"]
            from_owner = seg["from_owner"]
            to_owner = seg["to_owner"]

            segment_start_adjusted = move_to_next_work_moment(segment_start)

            candidate = deal_acts[
                (deal_acts["real_owner"] == agent_owner) &
                (deal_acts[COL_DUE_DATE] >= segment_start_adjusted)
            ].copy()

            if pd.notna(segment_end):
                candidate = candidate[candidate[COL_DUE_DATE] < segment_end].copy()

            candidate = candidate.sort_values([COL_DUE_DATE, COL_SUBJECT])

            if len(candidate) == 0:
                rows.append({
                    "deal_id": deal_id,
                    "deal_created": deal_created,
                    "segment_index": seg_idx + 1,
                    "segment_source": segment_source,
                    "from_owner": from_owner,
                    "to_owner": to_owner,
                    "agent_owner_raw": agent_owner_raw,
                    "agent_owner": agent_owner,
                    "segment_start": segment_start,
                    "segment_start_adjusted": segment_start_adjusted,
                    "segment_end": segment_end,
                    "first_contact_time": pd.NaT,
                    "first_contact_subject": "",
                    "delta_sec": float("nan"),
                    "has_contact": False,
                })
                continue

            first_row = candidate.iloc[0]
            first_contact_time = first_row[COL_DUE_DATE]
            first_contact_subject = first_row[COL_SUBJECT]
            delta_sec = business_seconds_between(segment_start_adjusted, first_contact_time)

            rows.append({
                "deal_id": deal_id,
                "deal_created": deal_created,
                "segment_index": seg_idx + 1,
                "segment_source": segment_source,
                "from_owner": from_owner,
                "to_owner": to_owner,
                "agent_owner_raw": agent_owner_raw,
                "agent_owner": agent_owner,
                "segment_start": segment_start,
                "segment_start_adjusted": segment_start_adjusted,
                "segment_end": segment_end,
                "first_contact_time": first_contact_time,
                "first_contact_subject": first_contact_subject,
                "delta_sec": delta_sec,
                "has_contact": True,
            })

        progress.progress(i / total_deals if total_deals else 1)

    res = pd.DataFrame(rows)
    segments_debug = pd.concat(all_segments, ignore_index=True) if all_segments else pd.DataFrame()
    activities_debug = (
        pd.concat(activities_with_real_owner, ignore_index=True)
        if activities_with_real_owner else pd.DataFrame()
    )

    if res.empty:
        return res, pd.DataFrame(), "", "", activities_debug, segments_debug, activities

    if apply_filter_1day:
        res = res[(res["delta_sec"].isna()) | (res["delta_sec"] < ONE_DAY_SECONDS)].copy()

    if selected_mode == "Primera llamada saliente":
        res["tiempo_hasta_primera_llamada"] = res["delta_sec"].apply(format_duration_exact)
    else:
        res["tiempo_hasta_primer_contacto"] = res["delta_sec"].apply(format_duration_exact)

    res = res.sort_values(["deal_id", "segment_start"]).reset_index(drop=True)

    res_with_contact = res[res["has_contact"] == True].copy()

    if len(res_with_contact) > 0:
        agent_stats = (
            res_with_contact.groupby("agent_owner", dropna=False)
            .agg(
                tramos_con_contacto=("deal_id", "count"),
                media_seg=("delta_sec", "mean"),
                mediana_seg=("delta_sec", "median"),
            )
            .reset_index()
        )
        agent_stats["media"] = agent_stats["media_seg"].apply(format_duration_exact)
        agent_stats["mediana"] = agent_stats["mediana_seg"].apply(format_duration_exact)
        agent_stats = agent_stats.sort_values("media_seg", na_position="last")

        media_total = format_duration_exact(res_with_contact["delta_sec"].mean())
        mediana_total = format_duration_exact(res_with_contact["delta_sec"].median())
    else:
        agent_stats = pd.DataFrame()
        media_total = ""
        mediana_total = ""

    return res, agent_stats, media_total, mediana_total, activities_debug, segments_debug, activities


def to_excel_bytes(
    res: pd.DataFrame,
    agent_stats: pd.DataFrame,
    debug_activities_real_owner: pd.DataFrame,
    debug_segments: pd.DataFrame,
    debug_filtered_source: pd.DataFrame
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        res.to_excel(writer, index=False, sheet_name="por_asignacion")
        if len(agent_stats) > 0:
            agent_stats.to_excel(writer, index=False, sheet_name="resumen_por_agente")
        debug_activities_real_owner.to_excel(writer, index=False, sheet_name="debug_owner_real")
        debug_segments.to_excel(writer, index=False, sheet_name="debug_segmentos")
        debug_filtered_source.to_excel(writer, index=False, sheet_name="debug_fuente_filtrada")
    return output.getvalue()


if uploaded:
    try:
        df = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"No he podido leer el Excel: {e}")
        st.stop()

    required_cols = [
        COL_DEAL_ID,
        COL_CREATED,
        COL_DUE_DATE,
        COL_SUBJECT,
        COL_OWNER,
    ]
    missing = [c for c in required_cols if c not in df.columns]

    if missing:
        st.error("Faltan columnas necesarias: " + ", ".join(missing))
        st.write("Columnas detectadas:", list(df.columns))
        st.stop()

    if not api_token or not company_domain:
        st.warning("Para calcular por tramos de asignación necesitas API token y subdominio.")
        st.stop()

    labels = get_result_labels(contact_mode)

    res, agent_stats, media_total, mediana_total, debug_activities_real_owner, debug_segments, debug_filtered_source = compute_by_assignment(
        df,
        apply_filter_1day,
        contact_mode
    )

    res_to_show = res.copy()
    if hide_segments_without_contact and len(res_to_show) > 0:
        res_to_show = res_to_show[res_to_show["has_contact"] == True].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric(labels["metric_count"], f"{len(res[res['has_contact'] == True]):,}".replace(",", "."))
    col2.metric("Media total", media_total)
    col3.metric("Mediana total", mediana_total)

    st.subheader(labels["title"])
    st.dataframe(res_to_show, use_container_width=True)

    if len(agent_stats) > 0:
        st.subheader("👤 Resumen por agente")
        st.dataframe(
            agent_stats[["agent_owner", "tramos_con_contacto", "media", "mediana"]],
            use_container_width=True
        )

    with st.expander("🔎 Debug owner real por actividad"):
        if len(debug_activities_real_owner) > 0:
            debug_cols = [
                COL_DEAL_ID,
                COL_CREATED,
                COL_DUE_DATE,
                COL_SUBJECT,
                "deal_owner",
                "activity_owner_raw_excel",
                "real_owner_raw",
                "real_owner",
                "segment_index_owner",
            ]
            existing_cols = [c for c in debug_cols if c in debug_activities_real_owner.columns]
            st.dataframe(
                debug_activities_real_owner[existing_cols].sort_values([COL_DEAL_ID, COL_DUE_DATE, COL_SUBJECT]),
                use_container_width=True
            )
        else:
            st.info("No hay actividades para mostrar.")

    with st.expander("🔎 Debug segmentos"):
        if len(debug_segments) > 0:
            st.dataframe(debug_segments.sort_values(["deal_id", "segment_start"]), use_container_width=True)
        else:
            st.info("No hay segmentos para mostrar.")

    with st.expander("🔎 Debug fuente filtrada"):
        st.dataframe(debug_filtered_source, use_container_width=True)

    xlsx_bytes = to_excel_bytes(
        res,
        agent_stats,
        debug_activities_real_owner,
        debug_segments,
        debug_filtered_source
    )
    st.download_button(
        "⬇️ Descargar Excel con resultados",
        data=xlsx_bytes,
        file_name=labels["download_name"],
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Sube un Excel para calcular el tiempo hasta la primera llamada por asignación.")
