"""
Steg 2 og 3: tematisk kategorisering av dbo.qa med sentence-transformers.

Hvert spørsmål+svar embeddes og sammenlignes (cosinus) med embeddings av
tema-beskrivelser. Beste treff -> *_top1, alle over THRESHOLD -> *_multi (JSON-liste).

- run_main_topics():  44 hovedtemaer      -> topic_top1, topics_multi
- run_psa_ssa():      PSA/SSA-undertemaer -> topic_top1_psa_ssa, topics_multi_psa_ssa
                      (kun rader der topic_top1 er et PSA/SSA-relevant hovedtema)
"""
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pyodbc

from pipeline import db

log = logging.getLogger("ung_pipeline")

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
BATCH_SIZE = 1024
COMMIT_EVERY = 20000
THRESHOLD = 0.38
MAX_CHARS_QUESTION = 800
MAX_CHARS_ANSWER = 1200

MAIN_TOPICS: Dict[str, str] = {
    "Barnevern": "barnevern, fosterhjem, omsorgsovertakelse, bekymringsmelding, tiltak for barn og unge",
    "Bolig / flytte hjemmefra": "bolig, flytte hjemmefra, hybel, leie, kontrakt, depositum, boforhold",
    "Familie": "familie, problemer hjemme, skilsmisse, foreldre, søsken, konflikter i familien",
    "Fritid": "fritid, hobbyer, aktiviteter, klubber, interesser, fritidstilbud",
    "Førerkort": "førerkort, øvelseskjøring, trafikkskole, teoriprøve, oppkjøring",
    "Konfirmasjon": "konfirmasjon, kirkelig konfirmasjon, borgerlig konfirmasjon, forberedelser",
    "Nettvett og sosiale medier": "nett, sosiale medier, nettvett, personvern, deling, spill, gaming, digital adferd",
    "Reise": "reise, ferie, utenlandsreise, pass, visum, reise alene, samtykke fra foreldre",
    "Vennskap": "vennskap, venner, venninne, konflikter mellom venner, utenforskap",
    "Økonomien din": "økonomi, penger, budsjett, gjeld, spare, bank, betalingsproblemer",
    "Forelskelse": "forelskelse, crush, liker noen, romantiske følelser, usikkerhet",
    "Forhold": "forhold, kjæreste, dating, samliv, brudd, grenser i forhold",
    "Kjønnsidentitet": "kjønnsidentitet, trans, ikke-binær, føle seg som et annet kjønn, pronomen",
    "Krysskulturell": "krysskulturell, kulturkonflikt, ære, religiøse regler, forventninger fra familie",
    "Mobbing": "mobbing, trakassering, utestenging, baksnakking, skolemiljø",
    "Psykisk helse og følelser": (
        "psykisk helse, psykiske vansker, sorg, krise, "
        "selvtillit, identitet, angst, depresjon, følelser, ensomhet"
    ),
    "Selvskading": "selvskading, kutting, skade seg selv, selvskadingsatferd",
    "Skeiv": "skeiv, lhbt, homofil, lesbisk, bifil, panfil, komme ut",
    "Spiseforstyrrelser": "spiseforstyrrelser, anoreksi, bulimi, overspising, kontroll av mat",
    "Doping": "doping, steroider, anabole steroider, prestasjonsfremmende midler",
    "Funksjonsnedsettelser": "funksjonsnedsettelser, diagnose, tilrettelegging, hjelpemidler",
    "Graviditet": "graviditet, svangerskap, abort, fødsel, gravid test",
    "Helsa di": (
        "helse, sykdom, kjønnssykdommer, seksuelt overførbare infeksjoner, "
        "tannhelse, smerter, symptomer, lege, helsesykepleier"
    ),
    "Kropp": (
        "kropp, pubertet, mensen, penis og pung, "
        "vulva og vagina, kroppspress, kroppsbilde"
    ),
    "Mat og kosthold": "mat, kosthold, matvaner, næring, slanking, sunn mat",
    "Prevensjon": "prevensjon, p-piller, spiral, kondom, nødprevensjon",
    "Rusmidler": "rusmidler, cannabis, tobakk, alkohol, hasj, narkotika, rusbruk",
    "Sex": "sex, samleie, seksualitet, onani, porno, samtykke, grenser",
    "Trening og idrett": "trening, idrett, fysisk aktivitet, styrke, kondisjon, prestasjon",
    "Demokrati og valg": "demokrati, valg, førstegangsvelger, stemmerett, politikk",
    "Forbruker": "forbruker, kjøp, reklamasjon, angrerett, abonnement, svindel",
    "Frivillig arbeid": "frivillig arbeid, frivillighet, organisasjoner, hjelpe andre",
    "Klima og miljø": "klima, miljø, bærekraft, natur, klimaangst, miljøvern",
    "Kriminalitet": "kriminalitet, lovbrudd, straff, politiet, anmeldelse",
    "Likestilling og diskriminering": "likestilling, diskriminering, rasisme, trakassering, rettigheter",
    "Jobb": "jobb, sommerjobb, arbeidsliv, lønn, kontrakt, arbeidserfaring",
    "Russetid": "russetid, russefeiring, russebuss, press, alkohol i russetiden",
    "Skolehverdag": "skolehverdag, leksehjelp, eksamen, prøver, fravær, karakterer",
    "Stipend og støtte": "stipend, støtte, NAV, økonomisk hjelp, lånekassen",
    "Studier i utlandet": "studier i utlandet, utveksling, skole i utlandet",
    "Utdanning": (
        "utdanning, ungdomsskole, videregående skole, "
        "høyere utdanning, studievalg, karriere"
    ),
    "Verneplikt / Forsvaret": "verneplikt, førstegangstjeneste, forsvaret, sesjon",
    "Seksuelle overgrep": "seksuelle overgrep, voldtekt, ufrivillig sex, krenkelser",
    "Vold": "vold, fysisk vold, psykisk vold, trusler, vold i nære relasjoner",
}

# Kun rader med disse hovedtemaene PSA/SSA-kategoriseres
PSA_SSA_PARENT_TOPICS = [
    "Nettvett og sosiale medier", "Vennskap", "Forelskelse", "Forhold", "Kjønnsidentitet", "Skeiv",
    "Helsa di", "Kropp", "Prevensjon", "Sex", "Seksuelle overgrep", "Vold",
]

# Undertemaene ligger i JSON: {label: [nøkkelord, ...]}
PSA_SSA_TOPICS_PATH = os.getenv(
    "PSA_SSA_TOPICS_PATH", str(Path(__file__).resolve().parent / "psa_ssa_topics.json"))

# FORCE_RECATEGORIZE_PSA_SSA=1 nullstiller og re-kategoriserer alle PSA/SSA-rader
# (bruk etter endringer i psa_ssa_topics.json)
FORCE_RECATEGORIZE_PSA_SSA = os.getenv("FORCE_RECATEGORIZE_PSA_SSA", "0").strip().lower() in {"1", "true", "yes", "on"}


# ---------------- felles ----------------

@lru_cache(maxsize=1)
def get_model():
    import torch
    from sentence_transformers import SentenceTransformer
    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info("Loading %s on %s", MODEL_NAME, device)
    return SentenceTransformer(MODEL_NAME, device=device)


def build_text(question: Optional[str], answer: Optional[str]) -> str:
    q = (question or "")[:MAX_CHARS_QUESTION]
    a = (answer or "")[:MAX_CHARS_ANSWER]
    return f"Spørsmål: {q}\n\nSvar: {a}".strip()


def encode(texts: Sequence[str], batch_size: int = 32) -> np.ndarray:
    emb = get_model().encode(list(texts), batch_size=batch_size, normalize_embeddings=True, show_progress_bar=False)
    return np.asarray(emb, dtype=np.float32)


def classify(texts: List[str], topic_names: List[str], topic_embs: np.ndarray,
             always_top1: bool) -> List[Tuple[str, List[str]]]:
    """
    Returnerer (top1, multi) per tekst. Uten treff over THRESHOLD:
    always_top1=True -> beste treff brukes likevel; False -> ('', []).
    """
    sims = encode(texts, batch_size=256) @ topic_embs.T  # cos-sim siden normalisert
    out = []
    for row in sims:
        best = topic_names[int(np.argmax(row))]
        multi = [topic_names[j] for j, s in enumerate(row) if s >= THRESHOLD]
        if multi or always_top1:
            out.append((best, multi or [best]))
        else:
            out.append(("", []))
    return out


def ensure_columns(conn: pyodbc.Connection, col_top1: str, col_multi: str) -> None:
    cur = conn.cursor()
    for col, typ in ((col_top1, "NVARCHAR(256)"), (col_multi, "NVARCHAR(MAX)")):
        cur.execute(f"""
        IF COL_LENGTH('{db.SCHEMA}.{db.TABLE}', '{col}') IS NULL
            ALTER TABLE [{db.SCHEMA}].[{db.TABLE}] ADD [{col}] {typ} NULL;
        """)
    conn.commit()


def _categorize(conn, topics: Dict[str, str], col_top1: str, col_multi: str,
                always_top1: bool, count_sql: str, fetch_batch) -> None:
    topic_names = list(topics)
    topic_embs = encode([topics[t] for t in topic_names])

    sql, params = count_sql
    (total,) = conn.cursor().execute(sql, *params).fetchone()
    log.info("To categorize into %s/%s: %d rows", col_top1, col_multi, total)

    processed = pending = 0
    while True:
        rows = fetch_batch()
        if not rows:
            break
        results = classify([build_text(r[1], r[2]) for r in rows], topic_names, topic_embs, always_top1)
        updates = [(t1, json.dumps(ml, ensure_ascii=False), r[0]) for r, (t1, ml) in zip(rows, results)]

        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(
            f"UPDATE [{db.SCHEMA}].[{db.TABLE}] SET [{col_top1}] = ?, [{col_multi}] = ? WHERE url = ?",
            updates)
        pending += len(updates)
        processed += len(rows)
        if pending >= COMMIT_EVERY:
            conn.commit()
            pending = 0
        log.info("%d/%d categorized", processed, total)

    conn.commit()
    log.info("Done: %d rows written to %s/%s", processed, col_top1, col_multi)


# ---------------- steg 2: hovedtemaer ----------------

def run_main_topics() -> None:
    pending_where = f"""
        WHERE ok = 1
          AND (topic_top1 IS NULL OR topic_top1 = '')
          AND (topics_multi IS NULL OR topics_multi = '')"""

    conn = db.connect_mssql()
    try:
        ensure_columns(conn, "topic_top1", "topics_multi")

        def fetch_batch():
            # Raden faller ut av WHERE når den er oppdatert (always_top1 gir alltid et label)
            return conn.cursor().execute(f"""
                SELECT url, question, answer FROM [{db.SCHEMA}].[{db.TABLE}] {pending_where}
                ORDER BY updated_at ASC OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY;
            """, BATCH_SIZE).fetchall()

        _categorize(conn, MAIN_TOPICS, "topic_top1", "topics_multi", always_top1=True,
                    count_sql=(f"SELECT COUNT(*) FROM [{db.SCHEMA}].[{db.TABLE}] {pending_where}", ()),
                    fetch_batch=fetch_batch)
    finally:
        conn.close()


# ---------------- steg 3: PSA/SSA-undertemaer ----------------

def load_psa_ssa_topics(path: str = PSA_SSA_TOPICS_PATH) -> Dict[str, str]:
    """{label: [nøkkelord]} -> {label: "nøkkelord, nøkkelord, ..."}"""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a JSON object {{label: [keywords...]}}")
    out = {}
    for label, kws in raw.items():
        if not isinstance(kws, list) or not all(isinstance(k, str) for k in kws):
            raise ValueError(f"Value for '{label}' in {path} must be a list of strings")
        out[label] = ", ".join(kws)
    return out


def run_psa_ssa() -> None:
    col_top1, col_multi = "topic_top1_psa_ssa", "topics_multi_psa_ssa"
    topics = load_psa_ssa_topics()
    log.info("Loaded %d PSA/SSA topics from %s", len(topics), PSA_SSA_TOPICS_PATH)

    parents = ", ".join("?" for _ in PSA_SSA_PARENT_TOPICS)
    base_where = f"WHERE ok = 1 AND topic_top1 IN ({parents})"
    # Rader uten treff får ('', '[]') og regnes dermed som ferdige
    pending_where = (f"{base_where} AND ([{col_top1}] IS NULL OR [{col_top1}] = '')"
                     f" AND ([{col_multi}] IS NULL OR [{col_multi}] = '')")

    conn = db.connect_mssql()
    try:
        ensure_columns(conn, col_top1, col_multi)

        if FORCE_RECATEGORIZE_PSA_SSA:
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE [{db.SCHEMA}].[{db.TABLE}] SET [{col_top1}] = NULL, [{col_multi}] = NULL
                WHERE [{col_top1}] IS NOT NULL OR [{col_multi}] IS NOT NULL;
            """)
            log.info("FORCE: reset %d rows", cur.rowcount)
            conn.commit()
            # Alle PSA/SSA-rader er nå "pending"; vi går gjennom dem sortert på url med cursor,
            # fordi rader uten treff ('') ellers ville blitt hentet på nytt i det uendelige.
            last_url = ""

            def fetch_batch():
                nonlocal last_url
                rows = conn.cursor().execute(f"""
                    SELECT TOP (?) url, question, answer FROM [{db.SCHEMA}].[{db.TABLE}]
                    {base_where} AND url > ? ORDER BY url ASC;
                """, BATCH_SIZE, *PSA_SSA_PARENT_TOPICS, last_url).fetchall()
                if rows:
                    last_url = rows[-1][0]
                return rows
            count_where = base_where
        else:
            def fetch_batch():
                return conn.cursor().execute(f"""
                    SELECT TOP (?) url, question, answer FROM [{db.SCHEMA}].[{db.TABLE}] {pending_where};
                """, BATCH_SIZE, *PSA_SSA_PARENT_TOPICS).fetchall()
            count_where = pending_where

        _categorize(conn, topics, col_top1, col_multi, always_top1=False,
                    count_sql=(f"SELECT COUNT(*) FROM [{db.SCHEMA}].[{db.TABLE}] {count_where}",
                               PSA_SSA_PARENT_TOPICS),
                    fetch_batch=fetch_batch)
    finally:
        conn.close()
