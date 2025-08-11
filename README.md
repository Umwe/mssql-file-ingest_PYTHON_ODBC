# MSSQL CSV/Excel Bulk Loader (Python, Windows/ODBC)

Bulk-load **CSV** and **Excel** files into a SQL Server table using Python + `pyodbc`.
- Reads files from the **same folder** where `loader.py` resides
- Supports `.csv`, `.xls`, `.xlsx`
- Converts Excel → CSV automatically (first sheet)
- Cleans headers, trims values, enforces `VARCHAR` lengths
- Fast inserts with `fast_executemany`
- Moves processed originals to `processed/` and converted CSVs to `converted/`

---

## Requirements

- Python 3.9+
- ODBC driver for SQL Server (x64 recommended)
  - `ODBC Driver 18 for SQL Server` or `ODBC Driver 17 for SQL Server`
- Python packages:
  ```bash
  pip install pyodbc pandas openpyxl "xlrd>=2.0.1"
#Full ODBC string
# Windows Auth example
set MSSQL_ODBC_STR=DRIVER={ODBC Driver 18 for SQL Server};SERVER=MYHOST\SQLEXPRESS;DATABASE=MyDb;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;Timeout=30;

Individual parts

# Required
set MSSQL_SERVER=MYHOST\MYINSTANCE   # or "MYHOST,1433"
set MSSQL_DATABASE=MyDb
set MSSQL_TABLE=dbo.QRC              # optional (defaults to dbo.QRC)

# Auth mode (choose one)
# 1) Windows Auth (default on Windows):
set MSSQL_TRUSTED=1
# 2) SQL Auth:
set MSSQL_TRUSTED=0
set MSSQL_USERNAME=myuser
set MSSQL_PASSWORD=mypassword

# Optional TLS tweaks
set MSSQL_ENCRYPT=yes                 # yes/no/auto (default auto for Driver 18)
set MSSQL_TRUSTSERVERCERT=yes         # yes/no/auto (default auto for Driver 18)

# Run: python loader.py
