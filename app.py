from contextlib import contextmanager
from datetime import datetime
import time
import pandas as pd
import streamlit as st
from sshtunnel import SSHTunnelForwarder
import pymysql

# --------------- Config ---------------
st.set_page_config(page_title="Verificador de despliegue", layout="wide")
st.title("Verificador de despliegue")

SSH_HOST = st.secrets["ssh"]["host"]
SSH_PORT = int(st.secrets["ssh"].get("port", 22))
SSH_USER = st.secrets["ssh"]["user"]
SSH_PASSWORD = st.secrets["ssh"].get("password")
SSH_KEYFILE = st.secrets["ssh"].get("key_file")

DB_HOST = st.secrets["mysql"]["host"]
DB_PORT = int(st.secrets["mysql"]["port"])
DB_USER = st.secrets["mysql"]["user"]
DB_PASS = st.secrets["mysql"]["password"]
DB_NAME = st.secrets["mysql"]["database"]

# --------------- SQL ---------------
SQL_CREATE_STRUCTURES = f"""
CREATE TABLE IF NOT EXISTS Proelan.NodoBaseline (
  NumeroNodo INT PRIMARY KEY,
  FSUE_old DATETIME,
  UFA_old  DATETIME,
  UFH_old  DATETIME,
  CapturedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DROP VIEW IF EXISTS Proelan.NodoCompare;
DROP VIEW IF EXISTS Proelan.NodoCurrent;

CREATE VIEW Proelan.NodoCurrent AS
SELECT
  n.NumeroNodo,
  /* Si hay varias filas por nodo, tomamos una (MAX). En MySQL 8+ podrías usar ANY_VALUE */
  MAX(n.VersionSoftware)            AS VersionSoftware,
  MAX(n.FechaServidorUltimaEmision) AS FSUE_new,
  MAX(s.FechaActual)                AS UFA_new,
  MAX(s.FechaHistorico)             AS UFH_new
FROM Proelan.Nodos n
LEFT JOIN Proelan.Controladores c ON c.IdNodo = n.IdNodo
LEFT JOIN Proelan.Senales s       ON s.IdControlador = c.IdControlador
GROUP BY n.NumeroNodo;

CREATE VIEW Proelan.NodoCompare AS
SELECT
  b.NumeroNodo,
  c.VersionSoftware,
  b.FSUE_old, c.FSUE_new, (c.FSUE_new > b.FSUE_old) AS OK_FSUE,
  b.UFA_old,  c.UFA_new,  (c.UFA_new  > b.UFA_old)  AS OK_UFA,
  b.UFH_old,  c.UFH_new,  (c.UFH_new   > b.UFH_old) AS OK_UFH
FROM Proelan.NodoBaseline b
LEFT JOIN Proelan.NodoCurrent c USING (NumeroNodo);
"""

SQL_CAPTURE_BASELINE = f"""
DELETE FROM Proelan.NodoBaseline;

INSERT INTO Proelan.NodoBaseline (NumeroNodo, FSUE_old, UFA_old, UFH_old)
SELECT
  n.NumeroNodo,
  MAX(n.FechaServidorUltimaEmision) AS FSUE_old,
  MAX(s.FechaActual)                AS UFA_old,
  MAX(s.FechaHistorico)             AS UFH_old
FROM Proelan.Nodos n
LEFT JOIN Proelan.Controladores c ON c.IdNodo = n.IdNodo
LEFT JOIN Proelan.Senales s       ON s.IdControlador = c.IdControlador
GROUP BY n.NumeroNodo
HAVING MAX(n.FechaServidorUltimaEmision) >= (NOW() - INTERVAL 3 HOUR);
"""

SQL_SELECT_COMPARE = f"SELECT * FROM Proelan.NodoCompare ORDER BY NumeroNodo;"

SQL_SELECT_TOTALS  = f"""
SELECT
  CAST(COUNT(*) AS UNSIGNED)                              AS TotalNodos,
  CAST(COALESCE(SUM(OK_FSUE), 0) AS UNSIGNED)             AS Total_FSUE_OK,
  CAST(COALESCE(SUM(OK_UFA),  0) AS UNSIGNED)             AS Total_UFA_OK,
  CAST(COALESCE(SUM(OK_UFH),  0) AS UNSIGNED)             AS Total_UFH_OK
FROM Proelan.NodoCompare;
"""

SQL_SELECT_TOTALS_FSUE = """
SELECT
  CAST(COUNT(*) AS UNSIGNED)                  AS TotalNodos,
  CAST(COALESCE(SUM(OK_FSUE), 0) AS UNSIGNED) AS Total_FSUE_OK
FROM Proelan.NodoCompare;
"""

SQL_SELECT_COUNT = "SELECT CAST(COUNT(*) AS UNSIGNED) AS total FROM Proelan.NodoCompare;"

# --------------- SSH + MySQL helpers ---------------
@contextmanager
def ssh_tunnel():
    kwargs = dict(
        ssh_address_or_host=(SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        set_keepalive=10.0,
        threaded=True,
        allow_agent=False,
    )
    if SSH_KEYFILE:
        kwargs["ssh_pkey"] = SSH_KEYFILE
    else:
        kwargs["ssh_password"] = SSH_PASSWORD

    server = SSHTunnelForwarder(**kwargs)
    server.start()
    try:
        yield ("127.0.0.1", server.local_bind_port)
    finally:
        server.stop()

def get_conn(local_host, local_port):
    return pymysql.connect(
        host=local_host,
        port=local_port,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=20,
        read_timeout=60,
        write_timeout=60
    )

def exec_multi(conn, multi_sql: str):
    # Ejecuta varias sentencias separadas por ';'
    with conn.cursor() as cur:
        for chunk in [s.strip() for s in multi_sql.split(";") if s.strip()]:
            cur.execute(chunk + ";")

def fetch_all(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()
    
def fetch_compare_page(conn, offset: int, limit: int):
    sql = """
    SELECT NumeroNodo,
           VersionSoftware,
           FSUE_old, FSUE_new, OK_FSUE,
           UFA_old,  UFA_new,  OK_UFA,
           UFH_old,  UFH_new,  OK_UFH
    FROM Proelan.NodoCompare
    ORDER BY NumeroNodo
    LIMIT %s OFFSET %s
    """
    return fetch_all(conn, sql, (limit, offset))

# --------------- Estado inicial ---------------
if "baseline_done" not in st.session_state:
    st.session_state.baseline_done = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = None
if "refresh_mode" not in st.session_state:
    st.session_state.refresh_mode = None
if "totals_cache" not in st.session_state:
    st.session_state.totals_cache = {
        "TotalNodos": 0,
        "Total_FSUE_OK": 0,
        "Total_UFA_OK": 0,
        "Total_UFH_OK": 0,
    }

PAGE_SIZE = 200
if "page" not in st.session_state:
    st.session_state.page = 0

# --------------- Controles de UI ---------------
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

# --------------- Lógica principal ---------------
def show_totals_and_table(conn, mode: str = "all"):
    total_rows = fetch_all(conn, SQL_SELECT_COUNT)[0]["total"]
    offset = st.session_state.page * PAGE_SIZE
    rows = fetch_compare_page(conn, offset, PAGE_SIZE)
    if not rows:
        st.info("No hay datos de comparación todavía. Carga baseline y luego refresca.")
        return

    df = pd.DataFrame(rows)
    def flag(x): return "✅" if x else "❌"

    if mode == "fsue":
        # Actualiza solo FSUE
        t_fsue = fetch_all(conn, SQL_SELECT_TOTALS_FSUE)[0]
        st.session_state.totals_cache["Total_FSUE_OK"] = int(t_fsue["Total_FSUE_OK"] or 0)
    else:
        # Refresco completo: actualiza los tres
        t_all = fetch_all(conn, SQL_SELECT_TOTALS)[0]
        st.session_state.totals_cache["TotalNodos"]    = int(t_all["TotalNodos"] or 0)
        st.session_state.totals_cache["Total_FSUE_OK"] = int(t_all["Total_FSUE_OK"] or 0)
        st.session_state.totals_cache["Total_UFA_OK"]  = int(t_all["Total_UFA_OK"] or 0)
        st.session_state.totals_cache["Total_UFH_OK"]  = int(t_all["Total_UFH_OK"] or 0)

    # Muestra SIEMPRE las 3 cabeceras
    st.subheader("Métricas")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nodos iniciales enviando",   st.session_state.totals_cache["TotalNodos"])
    c2.metric("Nodos enviando",             st.session_state.totals_cache["Total_FSUE_OK"])
    c3.metric("Nodos actualizando señales", st.session_state.totals_cache["Total_UFA_OK"])
    c4.metric("Nodos registrando",          st.session_state.totals_cache["Total_UFH_OK"])

    df_full = df.copy()
    for k in ("OK_FSUE", "OK_UFA", "OK_UFH"):
        df_full[k] = df_full[k].map(flag)

    st.subheader("Información por nodo")
    st.dataframe(
        df_full[[
            "NumeroNodo",
            "VersionSoftware",
            "FSUE_old", "FSUE_new", "OK_FSUE",
            "UFA_old",  "UFA_new",  "OK_UFA",
            "UFH_old",  "UFH_new",  "OK_UFH"
        ]],
        use_container_width=True
    )
    
    total_pages = max(1, (total_rows + PAGE_SIZE - 1) // PAGE_SIZE)
    curr = st.session_state.page + 1  # 1-based para UI

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

# Conectamos bajo demanda y ejecutamos acciones cuando se pulsan botones
with ssh_tunnel() as (lh, lp):
    conn = get_conn(lh, lp)

    if "structures_ready" not in st.session_state:
        exec_multi(conn, SQL_CREATE_STRUCTURES)
        st.session_state.structures_ready = True
        
    # Asegura estructura siempre que pulses baseline o refresh
    if do_baseline or do_refresh_fsue or do_refresh_all:
        exec_multi(conn, SQL_CREATE_STRUCTURES)
        st.session_state.page = 0

    if do_baseline:
        exec_multi(conn, SQL_CAPTURE_BASELINE)
        st.session_state.baseline_done = True
        st.success("Imagen actual de la base de datos cargada/actualizada.")
        st.session_state.page = 0

    if do_refresh_fsue:
        st.session_state.last_refresh = datetime.utcnow()
        st.session_state.refresh_mode = "fsue"
        st.session_state.page = 0

    if do_refresh_all:
        st.session_state.last_refresh = datetime.utcnow()
        st.session_state.refresh_mode = "all"
        st.session_state.page = 0

    # Mostrar datos SOLO si ya hay baseline y has refrescado explícitamente
    if st.session_state.baseline_done and (st.session_state.last_refresh is not None):
        show_totals_and_table(conn, mode=st.session_state.refresh_mode or "all")
    else:
        # Vista inicial vacía
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

    # Auto-refresh
    if auto and st.session_state.baseline_done:
        st.caption(f"Auto-refresh activado cada {interval} s.")
        time.sleep(interval)
        st.rerun()

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