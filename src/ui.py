# src/ui.py — Controles + vistas (controls + views)
import streamlit as st
import pandas as pd
from actions import get_totals, get_total_rows

# --------- Controles (entrada) ---------
def render_controls():
    left, mid1, mid2, right = st.columns([1,1,1,2])
    with left:
        do_baseline = st.button("📸 Cargar BBDD inicial")
    with mid1:
        do_refresh_fsue = st.button("🔄 Refrescar solo FSUE")
    with mid2:
        do_refresh_all = st.button("🔄 Refrescar comparación")
    with right:
        auto = st.checkbox("Auto-refresh", value=False)
        interval = st.slider("Intervalo (seg.)", 5, 60, 15, disabled=not auto)
    return do_baseline, do_refresh_fsue, do_refresh_all, auto, interval

# --------- Vistas (salida) -------------
def render_empty_placeholder():
    st.info("Pulsa “📸 Cargar BBDD inicial” para tomar la foto de la base de datos actual. "
            "Después pulsa “🔄 Refrescar comparación” para ver los resultados con los campos actualizados de la base de datos.")
    st.write("")
    st.write("**Tabla (vacía hasta cargar y refrescar):**")
    st.dataframe(pd.DataFrame(columns=[
        "NumeroNodo","VersionSoftware",
        "FSUE_old","FSUE_new","OK_FSUE",
        "UFA_old","UFA_new","OK_UFA",
        "UFH_old","UFH_new","OK_UFH"
    ]), use_container_width=True)

def render_dashboard(conn, mode: str):
    # Parámetros de sesión y paginación
    PAGE_SIZE = st.session_state.get("PAGE_SIZE", 200)
    offset = st.session_state.page * PAGE_SIZE

    # Import local para evitar ciclos
    from data import fetch_compare_page, to_df

    # Carga de página
    rows = fetch_compare_page(conn, offset, PAGE_SIZE)
    if not rows:
        st.info("No hay datos de comparación todavía. Carga baseline y luego refresca.")
        return

    # Totales (dependiendo del modo)
    get_totals(conn, mode)

    # DataFrame y flags
    df = to_df(rows)
    for k in ("OK_FSUE", "OK_UFA", "OK_UFH"):
        df[k] = df[k].map(lambda x: "✅" if x else "❌")

    # Métricas
    st.subheader("Métricas")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nodos iniciales enviando",   st.session_state.totals_cache["TotalNodos"])
    c2.metric("Nodos enviando",             st.session_state.totals_cache["Total_FSUE_OK"])
    c3.metric("Nodos actualizando señales", st.session_state.totals_cache["Total_UFA_OK"])
    c4.metric("Nodos registrando",          st.session_state.totals_cache["Total_UFH_OK"])

    # Tabla
    st.subheader("Información por nodo")
    st.dataframe(
        df[[
            "NumeroNodo","VersionSoftware",
            "FSUE_old","FSUE_new","OK_FSUE",
            "UFA_old","UFA_new","OK_UFA",
            "UFH_old","UFH_new","OK_UFH"
        ]],
        use_container_width=True
    )

    # Paginación
    total_rows = get_total_rows(conn)
    total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
    curr = st.session_state.page + 1

    pcol1, pcol2, pcol3 = st.columns([1,1,10])
    with pcol1:
        if st.button("⬅️ Anterior", disabled=st.session_state.page == 0):
            st.session_state.page = max(0, st.session_state.page - 1)
            st.rerun()
    with pcol2:
        if st.button("Siguiente ➡️", disabled=(st.session_state.page >= total_pages - 1)):
            st.session_state.page = min(total_pages - 1, st.session_state.page + 1)
            st.rerun()
    with pcol3:
        st.caption(f"Página {curr} de {total_pages} · Tamaño página: {PAGE_SIZE} · Filas totales: {total_rows}")

    # Marca de tiempo
    st.caption(f"Último refresco: {st.session_state.last_refresh} | Modo: {'solo FSUE' if mode=='fsue' else 'completo'}")
