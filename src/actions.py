from datetime import datetime
import streamlit as st
from data import (
    exec_multi, fetch_all,
    SQL_CREATE_STRUCTURES, SQL_CAPTURE_BASELINE,
    SQL_SELECT_TOTALS, SQL_SELECT_TOTALS_FSUE,
    fetch_count_total, fetch_count_filtered
)

PAGE_SIZE_DEFAULT = 200

def init_state():
    defaults = {
        "baseline_done": False,
        "last_refresh": None,
        "refresh_mode": None,  # "fsue" | "all"
        "totals_cache": {
            "TotalNodos": 0,
            "Total_FSUE_OK": 0,
            "Total_UFA_OK": 0,
            "Total_UFH_OK": 0,
        },
        "page": 0,
        "structures_ready": False,
        "PAGE_SIZE": PAGE_SIZE_DEFAULT,
        "SEARCH_QUERY": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v if not isinstance(v, dict) else v.copy()

def ensure_structures(conn):
    exec_multi(conn, SQL_CREATE_STRUCTURES)
    st.session_state.structures_ready = True

def capture_baseline(conn):
    exec_multi(conn, SQL_CAPTURE_BASELINE)
    st.session_state.baseline_done = True
    st.session_state.page = 0

def set_refresh(mode: str):
    st.session_state.last_refresh = datetime.utcnow()
    st.session_state.refresh_mode = mode
    st.session_state.page = 0

def get_totals(conn, mode: str):
    if mode == "fsue":
        t = fetch_all(conn, SQL_SELECT_TOTALS_FSUE)[0]
        st.session_state.totals_cache["Total_FSUE_OK"] = int(t["Total_FSUE_OK"] or 0)
    else:
        t = fetch_all(conn, SQL_SELECT_TOTALS)[0]
        st.session_state.totals_cache["TotalNodos"]    = int(t["TotalNodos"] or 0)
        st.session_state.totals_cache["Total_FSUE_OK"] = int(t["Total_FSUE_OK"] or 0)
        st.session_state.totals_cache["Total_UFA_OK"]  = int(t["Total_UFA_OK"] or 0)
        st.session_state.totals_cache["Total_UFH_OK"]  = int(t["Total_UFH_OK"] or 0)

def get_total_rows(conn, search_query: str | None = None) -> int:
    if search_query:
        return fetch_count_filtered(conn, search_query)
    return fetch_count_total(conn)
