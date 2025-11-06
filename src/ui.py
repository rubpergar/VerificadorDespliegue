import streamlit as st
import pandas as pd
from actions import get_totals, get_total_rows

# =========================
# Panel superior (2 mitades)
# =========================
def render_top_panel(conn):
    SEP_HEIGHT_PX = 260
    left, sep, right = st.columns([1, 0.2, 1])

    # ---- Controles (izquierda)
    with left:
        st.subheader("Acciones")
        c1, c2, c3 = st.columns(3)
        with c1:
            do_baseline = st.button("📸 Cargar BBDD")
        with c2:
            do_refresh_fsue = st.button("🔄 Solo FSUE")
        with c3:
            do_refresh_all = st.button("🔄 Refrescar todo")

        auto = st.checkbox("Auto-refresh", value=False)
        interval = st.slider("Intervalo (seg.)", 5, 60, 15, disabled=not auto)

    # ---- Separador vertical (centro)
    with sep:
        st.markdown(
            f"<div style='width:1px; height:{SEP_HEIGHT_PX}px; margin:8px auto 75px; background:#3a3a3a;'></div>",
            unsafe_allow_html=True
        )

    # ---- Estadísticas BD (derecha)
    from data import fetch_db_stats
    with right:
        st.subheader("Estado de la base de datos")
        try:
            stats = fetch_db_stats(conn)
            st.caption(f"Fecha última actualización: {stats['ts']}")
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Threads connected", stats["Threads_connected"])
                st.metric("Threads running",   stats["Threads_running"])
            with m2:
                st.metric("Threads created",   stats["Threads_created"])
                st.metric("Threads cached",    stats["Threads_cached"])
            with m3:
                st.metric("Conexiones",        stats["Connections"])
                st.metric("Conexiones abortadas", stats["Aborted_connects"])
        except Exception as e:
            st.warning(f"No se pudieron obtener estadísticas: {e}")

    return do_baseline, do_refresh_fsue, do_refresh_all, auto, interval

# ====================================
# Métricas globales de los nodos
# ====================================
def render_metrics(conn, mode: str):
    get_totals(conn, mode)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nodos iniciales enviando",   st.session_state.totals_cache["TotalNodos"])
    c2.metric("Nodos enviando",             st.session_state.totals_cache["Total_FSUE_OK"])
    c3.metric("Nodos actualizando señales", st.session_state.totals_cache["Total_UFA_OK"])
    c4.metric("Nodos registrando",          st.session_state.totals_cache["Total_UFH_OK"])

# ====================
# Tabla + paginación
# ====================
def render_dashboard(conn, mode: str, search_query: str = ""):
    PAGE_SIZE = st.session_state.get("PAGE_SIZE", 200)
    offset = st.session_state.page * PAGE_SIZE

    # Import local para evitar ciclos
    from data import (
        fetch_compare_page, fetch_compare_page_filtered, to_df
    )

    # Carga de página (con o sin filtro)
    if search_query:
        rows = fetch_compare_page_filtered(conn, search_query, offset, PAGE_SIZE)
        total_rows = get_total_rows(conn, search_query=search_query)
    else:
        rows = fetch_compare_page(conn, offset, PAGE_SIZE)
        total_rows = get_total_rows(conn)

    if not rows:
        st.info("No hay datos de comparación (ajusta el filtro o carga baseline y refresca).")
        return

    df = to_df(rows)
    df["Nodo"] = df.apply(
        lambda r: f"{r['NumeroNodo']} - {r['InstalacionNombre']}" if r.get("InstalacionNombre") else f"{r['NumeroNodo']}",
        axis=1
    )
    # Semáforo: todos OK -> 🟢 ; ninguno OK -> 🔴 ; en otro caso -> 🟡
    def semaforo(row):
        oks = [row["OK_FSUE"], row["OK_UFA"], row["OK_UFH"]]
        if all(bool(x) for x in oks):
            return "       🟢"
        if any(bool(x) for x in oks):
            return "       🟡"
        return "       🔴"
    df["Estado"] = df.apply(semaforo, axis=1)

    # Reemplaza flags por iconos ✅/❌
    for k in ("OK_FSUE", "OK_UFA", "OK_UFH"):
        df[k] = df[k].map(lambda x: "         ✅" if x else "         ❌")

    columns_order = [
        "Estado",
        "Nodo",
        "VersionSoftware",
        "FSUE_old", "FSUE_new", "OK_FSUE",
        "UFA_old",  "UFA_new",  "OK_UFA",
        "UFH_old",  "UFH_new",  "OK_UFH",
    ]
    columns_order = [c for c in columns_order if c in df.columns]

    st.subheader("Información por nodo")
    st.dataframe(df[columns_order], use_container_width=True)

    # Paginación + selector de tamaño
    total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
    curr = st.session_state.page + 1

    pcol1, pcol2, pcol3, pcol4 = st.columns([0.5, 0.5, 3, 1.5])
    with pcol1:
        if st.button("⬅️ Anterior", disabled=st.session_state.page == 0, key="prev_btn"):
            st.session_state.page = max(0, st.session_state.page - 1)
            st.rerun()
    with pcol2:
        if st.button("Siguiente ➡️", disabled=(st.session_state.page >= total_pages - 1), key="next_btn"):
            st.session_state.page = min(total_pages - 1, st.session_state.page + 1)
            st.rerun()
    with pcol3:
        st.caption(f"Página {curr} de {total_pages} · Filas totales (filtro{' activo' if search_query else ''}): {total_rows}")
    with pcol4:
        new_size = st.number_input(
            "Elementos por página", min_value=50, max_value=2000, step=50, value=int(PAGE_SIZE),
            help="Tamaño de página (por defecto 200)."
        )
        if int(new_size) != PAGE_SIZE:
            st.session_state.PAGE_SIZE = int(new_size)
            st.session_state.page = 0
            st.rerun()

    # Marca de tiempo
    st.caption(f"Último refresco: {st.session_state.last_refresh} | Modo: {'solo FSUE' if mode=='fsue' else 'completo'}")

# ====================
# Vista vacía inicial
# ====================
def render_empty_placeholder():
    st.info("Pulsa “📸 Cargar BBDD” para tomar la foto de la base actual y luego “🔄 Refrescar todo” para ver resultados.")
    st.write("")
    st.write("**Tabla (vacía hasta cargar y refrescar):**")
    st.dataframe(pd.DataFrame(columns=[
        "Estado",
        "Nodo",
        "VersionSoftware",
        "FSUE_old","FSUE_new","OK_FSUE",
        "UFA_old","UFA_new","OK_UFA",
        "UFH_old","UFH_new","OK_UFH"
    ]), use_container_width=True)
