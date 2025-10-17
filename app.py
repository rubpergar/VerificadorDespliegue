# app.py — Punto de entrada y config
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent / "src"))

import time
from datetime import datetime
import streamlit as st

from db import ssh_tunnel, get_conn
from ui import render_controls, render_dashboard, render_empty_placeholder
from actions import init_state, ensure_structures, capture_baseline, set_refresh

# --- Config mínima de la app (título, layout)
st.set_page_config(page_title="Verificador de nodos", layout="wide")
st.title("Verificador de nodos")

def main():
    # Estado inicial (session_state y constantes)
    init_state()

    # Controles de UI
    do_baseline, do_refresh_fsue, do_refresh_all, auto, interval = render_controls()

    # Conexión (SSH + MySQL) bajo demanda
    with ssh_tunnel() as (lh, lp):
        conn = get_conn(lh, lp)

        # Asegura estructuras al inicio y antes de acciones importantes
        if (not st.session_state.structures_ready) or any([do_baseline, do_refresh_fsue, do_refresh_all]):
            ensure_structures(conn)
            st.session_state.page = 0

        # Acciones
        if do_baseline:
            capture_baseline(conn)
            st.success("Imagen actual de la base de datos cargada/actualizada.")
        if do_refresh_fsue:
            set_refresh("fsue")
        if do_refresh_all:
            set_refresh("all")

        # Vista
        if st.session_state.baseline_done and (st.session_state.last_refresh is not None):
            render_dashboard(conn, mode=st.session_state.refresh_mode or "all")
        else:
            render_empty_placeholder()

        # Auto-refresh
        if auto and st.session_state.baseline_done:
            st.caption(f"Auto-refresh activado cada {interval} s.")
            time.sleep(interval)
            st.rerun()

if __name__ == "__main__":
    main()


## -------------- Consultas útiles para diagnóstico (futura implementación en la app) --------------
## -- Snapshot compacto de estado global
## SELECT
##   NOW()                                                    AS ts,
##   MAX(CASE WHEN VARIABLE_NAME='Threads_connected' THEN VARIABLE_VALUE END) AS Threads_connected,
##   MAX(CASE WHEN VARIABLE_NAME='Threads_running'   THEN VARIABLE_VALUE END) AS Threads_running,
##   MAX(CASE WHEN VARIABLE_NAME='Threads_created'   THEN VARIABLE_VALUE END) AS Threads_created,
##   MAX(CASE WHEN VARIABLE_NAME='Threads_cached'    THEN VARIABLE_VALUE END) AS Threads_cached,
##   MAX(CASE WHEN VARIABLE_NAME='Connections'       THEN VARIABLE_VALUE END) AS Connections,
##   MAX(CASE WHEN VARIABLE_NAME='Aborted_connects'  THEN VARIABLE_VALUE END) AS Aborted_connects
## FROM performance_schema.global_status
## WHERE VARIABLE_NAME IN (
##   'Threads_connected','Threads_running','Threads_created','Threads_cached',
##   'Connections','Aborted_connects'
## );