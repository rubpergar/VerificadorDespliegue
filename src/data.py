import pandas as pd

# --- SQL: estructuras
SQL_CREATE_STRUCTURES = """
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
  n.IdInstalacion,
  MAX(n.VersionSoftware)            AS VersionSoftware,
  MAX(n.FechaServidorUltimaEmision) AS FSUE_new,
  MAX(s.FechaActual)                AS UFA_new,
  MAX(s.FechaHistorico)             AS UFH_new,
  MAX(i.Nombre)                     AS InstalacionNombre
FROM Proelan.Nodos n
LEFT JOIN Proelan.Controladores c ON c.IdNodo = n.IdNodo
LEFT JOIN Proelan.Senales s       ON s.IdControlador = c.IdControlador
LEFT JOIN Proelan.Instalaciones i ON i.IdInstalacion = n.IdInstalacion
GROUP BY n.NumeroNodo, n.IdInstalacion;

CREATE VIEW Proelan.NodoCompare AS
SELECT
  b.NumeroNodo,
  c.IdInstalacion,
  c.InstalacionNombre,
  c.VersionSoftware,
  b.FSUE_old, c.FSUE_new, (c.FSUE_new > b.FSUE_old) AS OK_FSUE,
  b.UFA_old,  c.UFA_new,  (c.UFA_new  > b.UFA_old)  AS OK_UFA,
  b.UFH_old,  c.UFH_new,  (c.UFH_new   > b.UFH_old) AS OK_UFH
FROM Proelan.NodoBaseline b
LEFT JOIN Proelan.NodoCurrent c USING (NumeroNodo);
"""

# --- SQL: baseline
SQL_CAPTURE_BASELINE = """
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

# --- SQL: totales y conteos
SQL_SELECT_TOTALS  = """
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

# --- SQL: consultas paginadas + filtrado
SQL_PAGE_BASE = """
SELECT
  nc.NumeroNodo,
  i.Nombre AS InstalacionNombre,
  nc.VersionSoftware,
  nc.FSUE_old, nc.FSUE_new, nc.OK_FSUE,
  nc.UFA_old, nc.UFA_new,  nc.OK_UFA,
  nc.UFH_old, nc.UFH_new,  nc.OK_UFH
FROM Proelan.NodoCompare nc
LEFT JOIN Proelan.Nodos n ON n.NumeroNodo = nc.NumeroNodo
LEFT JOIN Proelan.Instalaciones i ON i.IdInstalacion = n.IdInstalacion
"""

SQL_COUNT_FILTERED = """
SELECT CAST(COUNT(*) AS UNSIGNED) AS total
FROM Proelan.NodoCompare nc
LEFT JOIN Proelan.Nodos n ON n.NumeroNodo = nc.NumeroNodo
LEFT JOIN Proelan.Instalaciones i ON i.IdInstalacion = n.IdInstalacion
WHERE (CAST(nc.NumeroNodo AS CHAR) LIKE %s OR i.Nombre LIKE %s);
"""

SQL_PAGE_FILTERED = SQL_PAGE_BASE + """
WHERE (CAST(nc.NumeroNodo AS CHAR) LIKE %s OR i.Nombre LIKE %s)
ORDER BY nc.NumeroNodo
LIMIT %s OFFSET %s;
"""

SQL_PAGE_UNFILTERED = SQL_PAGE_BASE + """
ORDER BY nc.NumeroNodo
LIMIT %s OFFSET %s;
"""

# --- SQL: estadísticas de MySQL
SQL_DB_STATS = """
SELECT
  NOW() AS ts,
  MAX(CASE WHEN VARIABLE_NAME='Threads_connected' THEN VARIABLE_VALUE END) AS Threads_connected,
  MAX(CASE WHEN VARIABLE_NAME='Threads_running'   THEN VARIABLE_VALUE END) AS Threads_running,
  MAX(CASE WHEN VARIABLE_NAME='Threads_created'   THEN VARIABLE_VALUE END) AS Threads_created,
  MAX(CASE WHEN VARIABLE_NAME='Threads_cached'    THEN VARIABLE_VALUE END) AS Threads_cached,
  MAX(CASE WHEN VARIABLE_NAME='Connections'       THEN VARIABLE_VALUE END) AS Connections,
  MAX(CASE WHEN VARIABLE_NAME='Aborted_connects'  THEN VARIABLE_VALUE END) AS Aborted_connects
FROM performance_schema.global_status
WHERE VARIABLE_NAME IN (
  'Threads_connected','Threads_running','Threads_created','Threads_cached',
  'Connections','Aborted_connects'
);
"""

# --- Helpers de acceso a datos
def exec_multi(conn, multi_sql: str):
    with conn.cursor() as cur:
        for chunk in [s.strip() for s in multi_sql.split(";") if s.strip()]:
            cur.execute(chunk + ";")

def fetch_all(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

# --- Paginación sin filtro
def fetch_compare_page(conn, offset: int, limit: int):
    return fetch_all(conn, SQL_PAGE_UNFILTERED, (limit, offset))

# --- Paginación con filtro
def fetch_compare_page_filtered(conn, query: str, offset: int, limit: int):
    like = f"%{query}%"
    return fetch_all(conn, SQL_PAGE_FILTERED, (like, like, limit, offset))

def fetch_count_filtered(conn, query: str) -> int:
    like = f"%{query}%"
    return int(fetch_all(conn, SQL_COUNT_FILTERED, (like, like))[0]["total"])

# --- Conteo total sin filtro
def fetch_count_total(conn) -> int:
    return int(fetch_all(conn, SQL_SELECT_COUNT)[0]["total"])

# --- DataFrame helper
def to_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)

# --- Estadísticas de MySQL
def fetch_db_stats(conn) -> dict:
    row = fetch_all(conn, SQL_DB_STATS)[0]
    return {
        "ts": row["ts"],
        "Threads_connected": int(row["Threads_connected"] or 0),
        "Threads_running":   int(row["Threads_running"] or 0),
        "Threads_created":   int(row["Threads_created"] or 0),
        "Threads_cached":    int(row["Threads_cached"] or 0),
        "Connections":       int(row["Connections"] or 0),
        "Aborted_connects":  int(row["Aborted_connects"] or 0),
    }
