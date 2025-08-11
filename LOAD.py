import os
import shutil
import glob
import time
import pyodbc
import pandas as pd

# ====== CONFIG ======
SERVER = r"RWNEWMISSVR\RATDB"  # or "RWNEWMISSVR,1433" if instance resolution fails
DATABASE = "GGSN_SGSN"
TABLE = "dbo.QRC"
CSV_DIR = r"C:\Users\kwizerah\Desktop\COMPLAIN DAILY QRC\dt"
PROCESSED_DIR = os.path.join(CSV_DIR, "processed")
CONVERTED_DIR = os.path.join(CSV_DIR, "converted")  # where .xls/.xlsx become .csv
CHUNK_ROWS = 10_000
ENCODING = "latin-1"   # robust for Windows CSVs from external tools
BAD_LINES = "skip"     # or "warn"
# ====================

EXPECTED_COLS = [
    "Date", "MobileNo", "AgentNameID", "SkillsetName", "StartTime",
    "EndTime", "HandlingTime", "Wrap-UP", "QRC", "LOB"
]

MAX_LEN = {
    "Date": 50, "MobileNo": 50, "AgentNameID": 100, "SkillsetName": 50,
    "StartTime": 50, "EndTime": 50, "HandlingTime": 50,
    "Wrap-UP": 100, "QRC": 50, "LOB": 50,
}

INSERT_SQL = f"""
INSERT INTO {TABLE} ([Date],[MobileNo],[AgentNameID],[SkillsetName],[StartTime],
[EndTime],[HandlingTime],[Wrap-UP],[QRC],[LOB])
VALUES (?,?,?,?,?,?,?,?,?,?)
"""

def connect():
    available = set(pyodbc.drivers())
    preferred = ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server", "SQL Server")
    picked = next((d for d in preferred if d in available), None)
    if not picked:
        raise RuntimeError(f"No suitable SQL Server ODBC driver found. Installed: {sorted(available)}")

    extra = "Encrypt=yes;TrustServerCertificate=yes;" if "18" in picked else "Encrypt=no;"
    conn_str = (
        f"DRIVER={{{picked}}};SERVER={SERVER};DATABASE={DATABASE};"
        "Trusted_Connection=yes;" + extra + "Timeout=30;"
    )
    print(f"[INFO] Using driver: {picked}")
    print(f"[INFO] Connecting to: {SERVER} (DB: {DATABASE})")
    conn = pyodbc.connect(conn_str)
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return conn

def normalize_headers(cols):
    mapping = {c.lower().replace(" ", "").replace("_", "").replace("-", ""): c for c in EXPECTED_COLS}
    out = []
    for col in cols:
        key = col.strip().lower().replace(" ", "").replace("_", "").replace("-", "")
        out.append(mapping.get(key, col.strip()))
    return out

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = normalize_headers(df.columns)
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df.loc[:, EXPECTED_COLS].copy()
    for c in EXPECTED_COLS:
        s = df[c].astype(str)
        s = s.str.strip().str.replace("\r", " ", regex=False).str.replace("\n", " ", regex=False)
        df.loc[:, c] = s.str.slice(0, MAX_LEN[c])
    return df

def read_csv_chunks(path, *, encoding, chunksize=CHUNK_ROWS):
    return pd.read_csv(
        path,
        dtype=str,
        encoding=encoding,
        chunksize=chunksize,
        engine="python",
        keep_default_na=False,
        on_bad_lines=BAD_LINES,
    )

def convert_excel_to_csv(xl_path: str) -> str:
    """
    Convert first sheet of .xls/.xlsx to CSV (UTF-8 with BOM) in CONVERTED_DIR.
    Returns the CSV path.
    """
    os.makedirs(CONVERTED_DIR, exist_ok=True)
    base = os.path.splitext(os.path.basename(xl_path))[0]
    csv_path = os.path.join(CONVERTED_DIR, f"{base}.csv")
    print(f"[CONVERT] {os.path.basename(xl_path)} → {os.path.basename(csv_path)} (sheet 1)")
    # dtype=str to preserve leading zeros; requires 'openpyxl' for .xlsx and 'xlrd' for .xls installed
    df = pd.read_excel(
    xl_path,
    sheet_name=0,
    dtype=str,
    engine="xlrd" if xl_path.lower().endswith(".xls") else "openpyxl"
)

    # Write as UTF-8 with BOM to be excel-friendly
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path

def load_csv_file(path: str, conn: pyodbc.Connection, *, encoding: str):
    print(f"[LOAD] {os.path.basename(path)}")
    t0 = time.time()
    total_rows = 0

    chunks = read_csv_chunks(path, encoding=encoding)
    with conn.cursor() as cur:
        cur.fast_executemany = True
        for chunk in chunks:
            chunk = clean_df(chunk)
            rows = list(chunk.itertuples(index=False, name=None))
            if not rows:
                continue
            cur.executemany(INSERT_SQL, rows)
            conn.commit()
            total_rows += len(rows)
            print(f"  inserted {len(rows):,} rows (running total: {total_rows:,})")

    print(f"[DONE] {os.path.basename(path)} → {total_rows:,} rows in {time.time()-t0:,.1f}s")

def discover_files(folder: str):
    # collect csv, xls, xlsx
    files = []
    files += glob.glob(os.path.join(folder, "*.csv"))
    files += glob.glob(os.path.join(folder, "*.xls"))
    files += glob.glob(os.path.join(folder, "*.xlsx"))
    # sort for deterministic order
    return sorted(files, key=lambda p: os.path.basename(p).lower())

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    src_files = discover_files(CSV_DIR)
    if not src_files:
        print("[INFO] No CSV/XLS/XLSX files found.")
        return

    conn = connect()
    try:
        for src in src_files:
            try:
                ext = os.path.splitext(src)[1].lower()
                target_csv = src
                target_encoding = ENCODING

                if ext in (".xls", ".xlsx"):
                    # Convert first, then use UTF-8 for the converted CSV
                    target_csv = convert_excel_to_csv(src)
                    target_encoding = "utf-8-sig"

                load_csv_file(target_csv, conn, encoding=target_encoding)

                # Move original file (not the converted one) to processed/
                shutil.move(src, os.path.join(PROCESSED_DIR, os.path.basename(src)))
            except Exception as e:
                print(f"[ERROR] Failed on {src}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
