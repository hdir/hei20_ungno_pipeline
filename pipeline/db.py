"""Azure SQL-tilkobling og alle SQL-operasjoner mot dbo.qa (crawl + prune)."""
import hashlib
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict, Optional

import pyodbc

log = logging.getLogger("ung_pipeline")

# ODBC-tilkoblingsstreng, f.eks.
#   Driver={ODBC Driver 18 for SQL Server};Server=tcp:digiung.database.windows.net,1433;
#   Database=UngNo_QA;Uid=digiung;Pwd=***;Encrypt=yes;TrustServerCertificate=no;
ODBC_CONN_STR_ENV = "UNG_AZURESQL_ODBC"

SCHEMA = "dbo"
TABLE = "qa"

# Behold kun Q&A besvart de siste N månedene (0 = ingen grense)
QA_MAX_AGE_MONTHS = int(os.getenv("QA_MAX_AGE_MONTHS", "12"))


def connect_mssql(timeout_sec: int = 180, max_retries: int = 10) -> pyodbc.Connection:
    """Kobler til Azure SQL. Serverless-databasen kaster 40613 mens den våkner, så vi prøver på nytt."""
    cs = os.environ.get(ODBC_CONN_STR_ENV)
    if not cs:
        raise RuntimeError(f"Missing env var {ODBC_CONN_STR_ENV} with ODBC connection string")

    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            cn = pyodbc.connect(cs, timeout=timeout_sec)
            cn.autocommit = False
            return cn
        except pyodbc.Error as e:
            msg = str(e)
            if "40613" not in msg and "is not currently available" not in msg:
                raise
            last_err = e
            sleep_s = min(60, 2 ** attempt)
            log.info("Azure SQL resuming (40613). Retry %d/%d in %ds...", attempt, max_retries, sleep_s)
            time.sleep(sleep_s)
    raise last_err


def ensure_conn(conn: pyodbc.Connection) -> pyodbc.Connection:
    """Returnerer conn hvis den lever, ellers en ny tilkobling (f.eks. etter 08S01 'Communication link failure')."""
    try:
        conn.cursor().execute("SELECT 1").fetchone()
        return conn
    except Exception as e:
        log.warning("DB connection lost (%s), reconnecting...", e)
        try:
            conn.close()
        except Exception:
            pass
        new_conn = connect_mssql()
        log.info("DB reconnected")
        return new_conn


def safe_commit(conn: pyodbc.Connection, context: str) -> bool:
    try:
        conn.commit()
        return True
    except Exception as e:
        log.exception("DB commit FAILED (%s): %s", context, e)
        try:
            conn.rollback()
        except Exception:
            log.exception("DB rollback ALSO failed (%s)", context)
        return False


# ---------------- datoer ----------------

def qa_cutoff_date(months: int = QA_MAX_AGE_MONTHS) -> Optional[date]:
    """Eldste besvart-dato som beholdes, `months` tilbake fra i dag. None = ingen grense."""
    if months <= 0:
        return None
    today = date.today()
    y, m = divmod(today.month - 1 - months, 12)
    year, month = today.year + y, m + 1
    # klem dag (f.eks. 31 -> 30/28)
    for day in (today.day, 30, 29, 28):
        try:
            return date(year, month, day)
        except ValueError:
            continue
    return None


def parse_answered_date(s: Optional[str]) -> Optional[date]:
    """Parser ung.no-format 'd.m.yyyy' (f.eks. '1.11.2022')."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None


# ---------------- schema ----------------

def init_db(cn: pyodbc.Connection) -> None:
    """Oppretter dbo.qa med indekser hvis tabellen mangler."""
    cn.cursor().execute(f"""
    IF OBJECT_ID(N'{SCHEMA}.{TABLE}', N'U') IS NULL
    BEGIN
        CREATE TABLE [{SCHEMA}].[{TABLE}] (
            [url] NVARCHAR(2048) NOT NULL,
            [title] NVARCHAR(512) NULL,
            [meta] NVARCHAR(MAX) NULL,
            [gender] NVARCHAR(64) NULL,
            [age] INT NULL,
            [question] NVARCHAR(MAX) NULL,
            [answer] NVARCHAR(MAX) NULL,
            [answered_date] NVARCHAR(64) NULL,
            [status] INT NULL,
            [ok] BIT NOT NULL CONSTRAINT DF_{TABLE}_ok DEFAULT (0),
            [updated_at] DATETIME2(0) NOT NULL DEFAULT (SYSUTCDATETIME()),
            [sitemap_lastmod] NVARCHAR(64) NULL,
            [content_hash] NVARCHAR(64) NULL,
            CONSTRAINT PK_{TABLE} PRIMARY KEY ([url])
        );
        CREATE INDEX IX_{TABLE}_lastmod ON [{SCHEMA}].[{TABLE}] ([sitemap_lastmod]);
        CREATE INDEX IX_{TABLE}_ok ON [{SCHEMA}].[{TABLE}] ([ok]);
        CREATE INDEX IX_{TABLE}_content_hash ON [{SCHEMA}].[{TABLE}] ([content_hash]);
    END
    """)
    cn.commit()


# ---------------- crawl-skriving ----------------

def load_existing_urls(cn: pyodbc.Connection) -> set[str]:
    cur = cn.cursor()
    return {row[0] for row in cur.execute(f"SELECT url FROM [{SCHEMA}].[{TABLE}]")}


def compute_content_hash(item: Dict[str, Any]) -> str:
    parts = [
        item.get("url") or "",
        item.get("title") or "",
        item.get("meta") or "",
        item.get("gender") or "",
        str(item.get("age") or ""),
        item.get("question") or "",
        item.get("answer") or "",
        item.get("answered_date") or "",
    ]
    return hashlib.sha256("\n".join(parts).encode("utf-8", errors="ignore")).hexdigest()


def upsert_qa(cn: pyodbc.Connection, item: Dict[str, Any], status: Optional[int],
              sitemap_lastmod: Optional[str]) -> None:
    """MERGE som setter ok=1. Oppdaterer kun hvis innhold, lastmod, ok eller status er endret."""
    cn.cursor().execute(f"""
    MERGE [{SCHEMA}].[{TABLE}] WITH (HOLDLOCK) AS tgt
    USING (
        SELECT ? AS url, ? AS title, ? AS meta, ? AS gender, ? AS age,
               ? AS question, ? AS answer, ? AS answered_date,
               ? AS status, 1 AS ok, ? AS sitemap_lastmod, ? AS content_hash
    ) AS src
    ON (tgt.[url] = src.[url])
    WHEN MATCHED AND (
           ISNULL(tgt.content_hash, '') <> src.content_hash
        OR (src.sitemap_lastmod IS NOT NULL AND ISNULL(tgt.sitemap_lastmod, '') <> src.sitemap_lastmod)
        OR ISNULL(tgt.ok, 0) <> 1
        OR ISNULL(tgt.status, -2147483648) <> ISNULL(src.status, -2147483648)
    )
    THEN UPDATE SET
        [title] = src.title, [meta] = src.meta, [gender] = src.gender, [age] = src.age,
        [question] = src.question, [answer] = src.answer, [answered_date] = src.answered_date,
        [status] = src.status, [ok] = 1, [sitemap_lastmod] = src.sitemap_lastmod,
        [content_hash] = src.content_hash, [updated_at] = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([url], [title], [meta], [gender], [age], [question], [answer], [answered_date],
                [status], [ok], [sitemap_lastmod], [content_hash])
        VALUES (src.url, src.title, src.meta, src.gender, src.age, src.question, src.answer,
                src.answered_date, src.status, 1, src.sitemap_lastmod, src.content_hash);
    """, (
        item.get("url"), item.get("title"), item.get("meta"), item.get("gender"), item.get("age"),
        item.get("question"), item.get("answer"), item.get("answered_date"),
        status, sitemap_lastmod, compute_content_hash(item),
    ))


def upsert_failed(cn: pyodbc.Connection, url: str, status: Optional[int]) -> None:
    """Registrerer feilet URL (ok=0). Oppdaterer ikke hvis den allerede har feilet med samme status."""
    cn.cursor().execute(f"""
    MERGE [{SCHEMA}].[{TABLE}] WITH (HOLDLOCK) AS tgt
    USING (SELECT ? AS url, ? AS status) AS src
    ON (tgt.url = src.url)
    WHEN MATCHED AND (
        ISNULL(tgt.ok, 0) <> 0
        OR ISNULL(tgt.status, -2147483648) <> ISNULL(src.status, -2147483648)
    )
    THEN UPDATE SET status = src.status, ok = 0, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (url, status, ok) VALUES (src.url, src.status, 0);
    """, (url, status))


# ---------------- prune ----------------

def prune_old_qa(cn: pyodbc.Connection, cutoff: date) -> int:
    """
    Sletter rader besvart før `cutoff`. Bruker answered_date (d.m.yyyy), med
    sitemap_lastmod (ISO) som fallback. Rader uten noen av dem beholdes.
    """
    cur = cn.cursor()
    cur.execute(f"""
        DELETE FROM [{SCHEMA}].[{TABLE}]
        WHERE COALESCE(
                TRY_CONVERT(date, [answered_date], 104),
                TRY_CONVERT(date, LEFT([sitemap_lastmod], 10), 23)
              ) < ?
    """, cutoff)
    deleted = cur.rowcount
    cn.commit()
    return deleted
