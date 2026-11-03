"""
Steg 4: ukentlig topic discovery.

For hver av de siste N hele ISO-ukene (Europe/Oslo): hent spørsmål besvart den uka,
embed med Azure OpenAI, klyng med KMeans (k<=10), og la en LLM navngi hver klynge.
Resultat lagres i qa_discovered_topics_weekly (ett tema per rad, rangert etter størrelse)
og qa_discovered_topic_questions_weekly (hvilke spørsmål som hører til hvilket tema).

Kjøres som egen prosess fra run_pipeline.py (frigjør minnet fra torch-modellen):
    python -m pipeline.discover_weekly [--weeks 1] [--overwrite] [--max-rows N]
"""
import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
from openai import AzureOpenAI
from sklearn.cluster import KMeans

from pipeline import db

log = logging.getLogger("ung_pipeline")

TOPICS_TABLE = "qa_discovered_topics_weekly"
ASSIGN_TABLE = "qa_discovered_topic_questions_weekly"
TZ_NAME = "Europe/Oslo"
K_MAX = 10
EMBED_BATCH = 100


@dataclass
class QA:
    url: str
    title: Optional[str]
    question: str
    answered_date_raw: Optional[str]
    answered_date_dt: Optional[date]


def last_full_weeks(n_weeks: int) -> List[Tuple[date, date]]:
    """(week_start, week_end) for de siste n hele ISO-ukene; inneværende uke tas ikke med."""
    today = datetime.now(tz=ZoneInfo(TZ_NAME)).date()
    this_week_start = today - timedelta(days=today.weekday())
    weeks = []
    for i in range(1, n_weeks + 1):
        ws = this_week_start - timedelta(days=7 * i)
        weeks.append((ws, ws + timedelta(days=7)))
    return weeks


# ---------------- SQL ----------------

def ensure_tables(conn) -> None:
    s = db.SCHEMA
    cur = conn.cursor()
    cur.execute(f"""
    IF OBJECT_ID(N'{s}.{TOPICS_TABLE}', N'U') IS NULL
    BEGIN
        CREATE TABLE [{s}].[{TOPICS_TABLE}] (
            [week_start] DATE NOT NULL,
            [week_end]   DATE NOT NULL,
            [topic_rank] INT NOT NULL,
            [topic_label] NVARCHAR(512) NOT NULL,
            [topic_description] NVARCHAR(2048) NULL,
            [keywords_json] NVARCHAR(MAX) NULL,
            [question_count] INT NOT NULL,
            [created_at] DATETIME2(0) NOT NULL DEFAULT (SYSUTCDATETIME()),
            [updated_at] DATETIME2(0) NOT NULL DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_{TOPICS_TABLE} PRIMARY KEY ([week_start], [topic_rank])
        );
        CREATE INDEX IX_{TOPICS_TABLE}_week ON [{s}].[{TOPICS_TABLE}] ([week_start]);
    END
    """)
    cur.execute(f"""
    IF OBJECT_ID(N'{s}.{ASSIGN_TABLE}', N'U') IS NULL
    BEGIN
        CREATE TABLE [{s}].[{ASSIGN_TABLE}] (
            [week_start] DATE NOT NULL,
            [topic_rank] INT NOT NULL,
            [url] NVARCHAR(2048) NOT NULL,
            [answered_date] DATE NULL,
            [answered_date_raw] NVARCHAR(64) NULL,
            [title] NVARCHAR(512) NULL,
            [question] NVARCHAR(MAX) NULL,
            [created_at] DATETIME2(0) NOT NULL DEFAULT (SYSUTCDATETIME()),
            [updated_at] DATETIME2(0) NOT NULL DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_{ASSIGN_TABLE} PRIMARY KEY ([week_start], [topic_rank], [url])
        );
        CREATE INDEX IX_{ASSIGN_TABLE}_week_topic ON [{s}].[{ASSIGN_TABLE}] ([week_start], [topic_rank]);
    END
    """)
    conn.commit()


def week_exists(conn, week_start: date) -> bool:
    cur = conn.cursor()
    cur.execute(f"SELECT TOP 1 1 FROM [{db.SCHEMA}].[{TOPICS_TABLE}] WHERE week_start = ?;", week_start.isoformat())
    return cur.fetchone() is not None


def delete_week(conn, week_start: date) -> None:
    cur = conn.cursor()
    for table in (ASSIGN_TABLE, TOPICS_TABLE):
        cur.execute(f"DELETE FROM [{db.SCHEMA}].[{table}] WHERE week_start = ?;", week_start.isoformat())
    conn.commit()


def fetch_week_questions(conn, week_start: date, week_end: date, max_rows: Optional[int]) -> List[QA]:
    top = f"TOP ({int(max_rows)})" if max_rows else ""
    cur = conn.cursor()
    cur.execute(f"""
    SELECT {top} url, title, question, answered_date,
           TRY_CONVERT(date, answered_date, 104) AS answered_date_dt
    FROM [{db.SCHEMA}].[{db.TABLE}]
    WHERE ok = 1
      AND question IS NOT NULL AND LTRIM(RTRIM(question)) <> ''
      AND TRY_CONVERT(date, answered_date, 104) >= ?
      AND TRY_CONVERT(date, answered_date, 104) < ?
    ORDER BY answered_date_dt ASC, updated_at ASC;
    """, week_start.isoformat(), week_end.isoformat())
    return [QA(*row) for row in cur.fetchall()]


def store_week(conn, week_start: date, week_end: date, qas: List[QA], ranks: np.ndarray,
               topics: Dict[int, Dict[str, Any]]) -> None:
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(f"""
        INSERT INTO [{db.SCHEMA}].[{TOPICS_TABLE}]
            (week_start, week_end, topic_rank, topic_label, topic_description, keywords_json, question_count, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """, [
        (week_start.isoformat(), week_end.isoformat(), rank, t["label"], t["description"],
         json.dumps(t["keywords"], ensure_ascii=False), t["count"])
        for rank, t in sorted(topics.items())
    ])
    cur.executemany(f"""
        INSERT INTO [{db.SCHEMA}].[{ASSIGN_TABLE}]
            (week_start, topic_rank, url, answered_date, answered_date_raw, title, question, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """, [
        (week_start.isoformat(), int(rank), qa.url,
         qa.answered_date_dt.isoformat() if qa.answered_date_dt else None,
         qa.answered_date_raw, qa.title, qa.question)
        for qa, rank in zip(qas, ranks) if int(rank) in topics
    ])
    conn.commit()


# ---------------- Azure OpenAI ----------------

def embedding_client() -> AzureOpenAI:
    return AzureOpenAI(
        api_key=os.environ["AZURE_OPENAI_EMBEDDINGS_API_KEY"],
        azure_endpoint=os.environ["AZURE_OPENAI_EMBEDDINGS_ENDPOINT"],
        api_version=os.environ["AZURE_OPENAI_EMBEDDINGS_API_VERSION"],
    )


def chat_client() -> AzureOpenAI:
    return AzureOpenAI(
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_version=os.environ["AZURE_OPENAI_API_VERSION"],
        timeout=120,
    )


def embed_texts(client: AzureOpenAI, texts: List[str]) -> np.ndarray:
    deployment = os.environ["AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT"]
    vecs: List[List[float]] = []
    for i in range(0, len(texts), EMBED_BATCH):
        resp = client.embeddings.create(model=deployment, input=texts[i:i + EMBED_BATCH])
        vecs.extend(d.embedding for d in sorted(resp.data, key=lambda d: d.index))
    return np.asarray(vecs, dtype=np.float32)


def llm_label_cluster(client: AzureOpenAI, questions: List[str], typical: List[str]) -> Dict[str, Any]:
    """Ber LLM-en om {"label", "description", "keywords"} for en klynge spørsmål."""
    suggested = ""
    if typical:
        suggested = (
            "\nForeslåtte typiske spørsmål (bruk gjerne disse som 'ofte stilte spørsmål' hvis de passer):\n"
            "- " + "\n- ".join(s[:90].rstrip() for s in typical) + "\n"
        )
    prompt = (
        "Du får en liste med spørsmål som handler om samme tema.\n\n"
        "Lag et tema i to nivåer:\n"
        "A) label: kort temnavn (maks 8 ord)\n"
        "B) description: én setning i FAST format:\n"
        '   "<temaområder>: <konkret problemstilling/ønske>: <ofte stilte spørsmål>"\n\n'
        "Regler for description:\n"
        "- Start med 2–4 temaområder adskilt med komma (f.eks. 'kropp, helse og kosthold').\n"
        "- Etter første kolon (<konkret problemstilling/ønske>): konkretiser hva de lurer på "
        "(bruk 'hvordan', 'om', 'hva betyr', 'er det normalt', osv.).\n"
        "- Etter andre kolon (<ofte stilte spørsmål>): vis 2 eksempler på spørsmål som oftest er stilt "
        "innenfor problemstilling/ønske.\n"
        "  *Skriv de 2 eksemplene veldig kort, separert med ' / '.\n"
        "- Maks 140 tegn.\n"
        "- Ikke bruk punktum til slutt.\n\n"
        "keywords:\n"
        "- 3–8 nøkkelord, små bokstaver, uten punktum.\n"
        + suggested +
        "\nSvar KUN som gyldig JSON på formen:\n"
        '{"label":"...","description":"...","keywords":["...","..."]}\n\n'
        "Spørsmål:\n- " + "\n- ".join(q.replace("\n", " ").strip() for q in questions[:25])
    )
    resp = client.chat.completions.create(
        model=os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"],
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
    )
    text = (resp.choices[0].message.content or "").strip()
    if text.startswith("```"):  # fjern ev. kodegjerde
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    data = json.loads(text)

    keywords = data.get("keywords", [])
    if not isinstance(keywords, list):
        keywords = []
    return {
        "label": str(data.get("label", "Ukjent tema")).strip()[:512],
        "description": str(data.get("description", "")).strip().rstrip(".")[:200].rstrip(),
        "keywords": [str(k).strip() for k in keywords[:8] if str(k).strip()],
    }


# ---------------- klynging ----------------

def normalize(emb: np.ndarray) -> np.ndarray:
    return emb / (np.linalg.norm(emb, axis=1, keepdims=True) + 1e-12)


def cluster(emb_n: np.ndarray, seed: int = 42) -> np.ndarray:
    k = min(K_MAX, emb_n.shape[0])
    if k <= 1:
        return np.zeros((emb_n.shape[0],), dtype=int)
    return KMeans(n_clusters=k, random_state=seed, n_init="auto").fit_predict(emb_n).astype(int)


def typical_questions(emb_n: np.ndarray, idx: np.ndarray, qas: List[QA], k: int = 2) -> List[str]:
    """De k spørsmålene nærmest klyngens sentroide (cosinus)."""
    vecs = emb_n[idx]
    centroid = normalize(vecs.mean(axis=0, keepdims=True))
    top = idx[(vecs @ centroid.T).ravel().argsort()[::-1][:k]]
    return [qas[i].question.strip().replace("\n", " ") for i in top if qas[i].question]


def process_week(conn, emb_client, llm_client, ws: date, we: date, max_rows: Optional[int]) -> None:
    qas = fetch_week_questions(conn, ws, we, max_rows)
    log.info("[%s..%s) fetched %d questions", ws, we, len(qas))
    if not qas:
        return

    texts = [f"{(q.title or '').strip()}\n{q.question.strip()}".strip() for q in qas]
    emb_n = normalize(embed_texts(emb_client, texts))
    cluster_ids = cluster(emb_n)

    # Rang 1 = største klynge
    unique, counts = np.unique(cluster_ids, return_counts=True)
    order = unique[np.argsort(-counts)]
    cid_to_rank = {int(cid): rank for rank, cid in enumerate(order, start=1)}
    ranks = np.array([cid_to_rank[int(c)] for c in cluster_ids], dtype=int)

    topics: Dict[int, Dict[str, Any]] = {}
    for cid, rank in cid_to_rank.items():
        idx = np.where(cluster_ids == cid)[0]
        meta = llm_label_cluster(llm_client, [qas[i].question for i in idx], typical_questions(emb_n, idx, qas))
        topics[rank] = {**meta, "count": len(idx)}
        log.info("  rank %d: %s (%d) – %s", rank, meta["label"], len(idx), meta["description"])

    store_week(conn, ws, we, qas, ranks, topics)
    log.info("[OK] %s: stored %d topics", ws, len(topics))


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--weeks", type=int, default=int(os.getenv("DISCOVERY_WEEKS", "1")),
                   help="antall siste hele uker (default 1)")
    p.add_argument("--overwrite", action="store_true",
                   default=os.getenv("DISCOVERY_OVERWRITE", "0") == "1",
                   help="regenerer uker som allerede finnes")
    p.add_argument("--max-rows", type=int, default=None, help="maks spørsmål per uke (for testing)")
    args = p.parse_args(argv)

    emb_client, llm_client = embedding_client(), chat_client()
    conn = db.connect_mssql()
    try:
        ensure_tables(conn)
        for ws, we in last_full_weeks(args.weeks):
            if week_exists(conn, ws):
                if not args.overwrite:
                    log.info("[SKIP] %s: already generated (use --overwrite)", ws)
                    continue
                log.info("[OVERWRITE] %s: deleting old results", ws)
                delete_week(conn, ws)
            process_week(conn, emb_client, llm_client, ws, we, args.max_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    from pipeline.logging_setup import setup_logging
    setup_logging()
    main()
