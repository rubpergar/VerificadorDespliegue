# src/data.py — SQL + helpers (exec_multi/fetch/page)
import pandas as pd

# --- SQL (tus sentencias originales)
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

SQL_SELECT_COMPARE = "SELECT * FROM Proelan.NodoCompare ORDER BY NumeroNodo;"

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

# --- Helpers de acceso a datos
def exec_multi(conn, multi_sql: str):
    with conn.cursor() as cur:
        for chunk in [s.strip() for s in multi_sql.split(";") if s.strip()]:
            cur.execute(chunk + ";")

def fetch_all(conn, sql: str, params=None):
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

def to_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)
