from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent / "src"))

import time
import streamlit as st
from db import ssh_tunnel, get_conn
from ui import (
    render_top_panel,            # panel con controles + estadísticas DB (con separador vertical)
    render_metrics,              # fila con métricas de los nodos
    render_dashboard,            # tabla principal + paginación + selector tamaño
    render_empty_placeholder
)
from actions import (
    init_state,
    ensure_structures,
    capture_baseline,
    set_refresh
)

st.set_page_config(page_title="Verificador de nodos", layout="wide")

def main():
    # Estado inicial
    init_state()

    # Conexión (SSH + MySQL)
    with ssh_tunnel() as (lh, lp):
        conn = get_conn(lh, lp)

        # Panel superior: controles + estadísticas (requiere conexión)
        (
            do_baseline,
            do_refresh_fsue,
            do_refresh_all,
            auto,
            interval
        ) = render_top_panel(conn)

        # Asegura estructuras al inicio y antes de acciones importantes
        ensure_structures(conn)

        # Acciones
        if do_baseline:
            capture_baseline(conn)
            st.success("Imagen actual de la base de datos cargada/actualizada.")
        if do_refresh_fsue:
            set_refresh("fsue")
        if do_refresh_all:
            set_refresh("all")

        # Fila de filtro + métricas
        search_query = render_metrics(conn, mode=st.session_state.refresh_mode or "all")

        # Vista principal
        if st.session_state.baseline_done and (st.session_state.last_refresh is not None):
            render_dashboard(conn, mode=st.session_state.refresh_mode or "all", search_query=search_query)
        else:
            render_empty_placeholder()

        # Auto-refresh
        if auto and st.session_state.baseline_done:
            st.caption(f"Auto-refresh activado cada {interval} s.")
            time.sleep(interval)
            st.rerun()

if __name__ == "__main__":
    main()
