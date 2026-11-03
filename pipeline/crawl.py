"""Steg 1: crawler nye spørsmål/svar fra ung.no (sitemap -> /oss/-sider) til dbo.qa."""
import logging
import os
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import List, Optional, Tuple

import requests
from lxml import etree
from lxml import html as lxml_html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import db

log = logging.getLogger("ung_pipeline")

SITEMAP_URL = "https://www.ung.no/sitemap.xml"
QA_URL_PREFIX = "https://www.ung.no/oss/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UngNo-QA-Scraper/threads; +contact: you@example.com)",
    "Accept": "application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8",
}

REQUEST_TIMEOUT = 30
MAX_WORKERS = 8
IN_FLIGHT = 20
MAX_URLS = int(os.getenv("MAX_URLS", "800000"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "200"))

# Retry per side (i tillegg til Retry på sitemap-sesjonen)
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 0.5

# h2-overskrifter som markerer at svaret er slutt (gammel sidelayout)
STOP_H2_TITLES = {
    "vi har valgt ut dette for deg",
    "spørsmål og svar som ligner",
    "still oss et spørsmål",
    "gå til spørsmål og svar",
    "kontakt og hjelp",
}

DATE_RE = re.compile(r"Besvart[\s\xa0]*:[\s\xa0]*([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4})", re.I)
META_RE = re.compile(r"^\s*([A-Za-zÆØÅæøå]+)\s*(?:,\s*(\d{1,2}))?\s*$")


# ---------------- parsing av Q&A-side ----------------

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def parse_gender_age(meta: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """'Jente, 14' -> ('Jente', 14)"""
    if not meta:
        return None, None
    m = META_RE.match(meta.strip())
    if not m:
        return None, None
    return m.group(1).strip(), int(m.group(2)) if m.group(2) else None


def _paragraphs_text(el) -> Optional[str]:
    """Slår sammen <p>-tekster med linjeskift (faller tilbake på elementets egen tekst)."""
    ps = el.xpath(".//p")
    parts = [clean(p.text_content()) for p in ps] if ps else [clean(el.text_content())]
    parts = [p for p in parts if p]
    return "\n".join(parts) if parts else None


def _siblings_text_until_h2(start_el) -> Optional[str]:
    parts: List[str] = []
    for sib in start_el.itersiblings():
        if getattr(sib, "tag", "").lower() == "h2":
            break
        txt = clean(sib.text_content())
        if txt:
            parts.append(txt)
    return "\n".join(parts).strip() if parts else None


def _first(doc, *xpaths):
    for xp in xpaths:
        r = doc.xpath(xp)
        if r:
            return r[0]
    return None


def extract_qa(page_html: bytes, url: str) -> dict:
    doc = lxml_html.fromstring(page_html)

    h1 = _first(doc, "//main//h1[1]", "//h1[1]")
    title = clean(h1.text_content()) if h1 is not None else None
    item = {"url": url, "title": title, "meta": None, "question": None,
            "answer": None, "answered_date": None}

    # --- Nåværende layout (2026): h1 -> div[pill "Jente, 14" + div.prose spørsmål];
    #     svar = div med <p>-er rett før div-en som holder "Besvart: <dato>" ---
    box = h1.getnext() if h1 is not None else None
    prose = box.xpath("./div[contains(@class,'prose')]") if box is not None else []
    if prose:
        pill = box.xpath("./div[contains(@class,'rounded-2xl')][1]")
        item["meta"] = clean(pill[0].text_content()) if pill else None
        item["question"] = _paragraphs_text(prose[0])

        besvart = doc.xpath("//main//strong[starts-with(normalize-space(),'Besvart')]")
        if besvart:
            date_holder = besvart[0].getparent()  # <p>Besvart: d.m.yyyy</p>
            m = DATE_RE.search(clean(date_holder.text_content()))
            if m:
                item["answered_date"] = m.group(1)
            answer_box = date_holder.getparent().getprevious()
            if answer_box is not None:
                item["answer"] = _paragraphs_text(answer_box)

    # --- Gammel layout: h1 -> h2 "Jente, 14" -> spørsmål -> h2 "Svar" -> svar ---
    else:
        first_h2 = _first(doc, "//main//h1[1]/following::h2[1]", "//h1[1]/following::h2[1]")
        if first_h2 is not None:
            item["meta"] = clean(first_h2.text_content())
            item["question"] = _siblings_text_until_h2(first_h2)

        svar_h2 = _first(doc,
                         "//main//h2[translate(normalize-space(.), 'SVAR', 'svar')='svar'][1]",
                         "//h2[translate(normalize-space(.), 'SVAR', 'svar')='svar'][1]")
        if svar_h2 is None and first_h2 is not None:
            # Uten "Svar"-overskrift: bruk innholdet etter neste h2
            svar_h2 = next((s for s in first_h2.itersiblings()
                            if getattr(s, "tag", "").lower() == "h2"), None)
        if svar_h2 is not None:
            raw = _siblings_text_until_h2(svar_h2)
            if raw:
                item["answer"] = DATE_RE.sub("", raw).strip() or None

        main_text = " ".join(doc.xpath("//main//text()")) or " ".join(doc.xpath("//text()"))
        m = DATE_RE.search(main_text)
        if m:
            item["answered_date"] = m.group(1)

    item["gender"], item["age"] = parse_gender_age(item["meta"])
    return item


# ---------------- sitemap ----------------

def build_sitemap_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "ung-crawler/1.0 (+contact: your-email@domain.no)"})
    return s


def fetch_bytes(url: str, session: requests.Session, timeout=(10, 180)) -> bytes:
    t0 = time.perf_counter()
    resp = session.get(url, timeout=timeout, allow_redirects=True)
    log.info("HTTP GET url=%s status=%s elapsed=%.2fs", url, resp.status_code, time.perf_counter() - t0)
    resp.raise_for_status()
    return resp.content


def parse_sitemap(xml_bytes: bytes, session: requests.Session) -> List[Tuple[str, Optional[str]]]:
    """Returnerer (url, lastmod) for alle URL-er. Følger sitemapindex rekursivt."""
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    root = etree.fromstring(xml_bytes)
    tag = etree.QName(root).localname

    if tag == "sitemapindex":
        out: List[Tuple[str, Optional[str]]] = []
        locs = root.xpath("//sm:sitemap/sm:loc/text()", namespaces=ns)
        log.info("Sitemapindex: found %d child sitemaps", len(locs))
        for sm_url in locs:
            try:
                child = fetch_bytes(sm_url, session)
            except requests.HTTPError as e:
                code = getattr(e.response, "status_code", None)
                if code in (404, 410):
                    log.warning("Skipping child sitemap (HTTP %s): %s", code, sm_url)
                    continue
                raise
            try:
                out.extend(parse_sitemap(child, session))
            except etree.XMLSyntaxError:
                log.exception("Skipping child sitemap due to parse error: %s", sm_url)
        return out

    if tag == "urlset":
        rows = []
        for u in root.xpath("//sm:url", namespaces=ns):
            loc = u.xpath("./sm:loc/text()", namespaces=ns)
            lastmod = u.xpath("./sm:lastmod/text()", namespaces=ns)
            if loc:
                rows.append((loc[0], lastmod[0] if lastmod else None))
        return rows

    raise ValueError(f"Unknown sitemap root tag: {tag}")


# ---------------- henting av sider (tråder) ----------------

_thread_local = threading.local()


def _thread_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return s


def fetch_and_parse(url: str) -> Tuple[Optional[dict], Optional[int], Optional[str]]:
    """Kjøres i worker-tråd. Returnerer (item, http_status, feilmelding)."""
    status: Optional[int] = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = _thread_session().get(url, timeout=REQUEST_TIMEOUT)
            status = resp.status_code
            if status == 429 or status >= 500:
                raise requests.HTTPError(f"HTTP {status}", response=resp)
            resp.raise_for_status()
            return extract_qa(resp.content, url), status, None
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
                continue
            if isinstance(e, requests.HTTPError) and e.response is not None:
                status = e.response.status_code
            return None, status, str(e)


# ---------------- main ----------------

def main() -> None:
    log.info("Starting ung.no crawl (MAX_WORKERS=%d IN_FLIGHT=%d MAX_URLS=%d)", MAX_WORKERS, IN_FLIGHT, MAX_URLS)
    cutoff = db.qa_cutoff_date()

    conn = db.connect_mssql()
    try:
        db.init_db(conn)
        existing = db.load_existing_urls(conn)
        log.info("DB: %d URLs already in %s.%s", len(existing), db.SCHEMA, db.TABLE)

        with build_sitemap_session() as s:
            all_pairs = parse_sitemap(fetch_bytes(SITEMAP_URL, s), s)
        qa_pairs = [(u, lm) for (u, lm) in all_pairs if u.startswith(QA_URL_PREFIX)]

        # Kun Q&A innenfor aldersvinduet (sitemap lastmod er ISO, f.eks. 2025-11-20T13:52:18Z)
        if cutoff is not None:
            qa_pairs = [(u, lm) for (u, lm) in qa_pairs if not lm or lm[:10] >= cutoff.isoformat()]
        qa_pairs = qa_pairs[:MAX_URLS]

        # Kun nye URL-er crawles; eksisterende rader røres ikke
        to_crawl = [(u, lm) for (u, lm) in qa_pairs if u not in existing]
        total = len(to_crawl)
        log.info("Sitemap: %d URLs, %d Q&A after filter (cutoff=%s), %d new to crawl",
                 len(all_pairs), len(qa_pairs), cutoff, total)
        if total == 0:
            log.info("Nothing to do.")
            return

        done = saved = failed = skipped = pending_db = 0

        def write(item: dict, status: Optional[int], lastmod: Optional[str]) -> bool:
            nonlocal conn, pending_db
            try:
                db.upsert_qa(conn, item, status, lastmod)
                return True
            except Exception as e:
                log.warning("DB write failed for url=%s: %s", item["url"], e)
            new_conn = db.ensure_conn(conn)
            if new_conn is not conn:
                # Tilkoblingen var død: ucommittet batch er tapt (crawles på nytt neste natt).
                conn, pending_db = new_conn, 0
                try:
                    db.upsert_qa(conn, item, status, lastmod)
                    return True
                except Exception:
                    log.exception("DB write failed again after reconnect for url=%s", item["url"])
            else:
                conn.rollback()
            return False

        def write_failed(url: str, status: Optional[int]) -> None:
            nonlocal conn
            try:
                db.upsert_failed(conn, url, status)
            except Exception:
                log.exception("DB upsert_failed failed for url=%s", url)
                conn = db.ensure_conn(conn)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            queue = iter(to_crawl)
            in_flight = {}

            def fill() -> None:
                while len(in_flight) < IN_FLIGHT:
                    nxt = next(queue, None)
                    if nxt is None:
                        return
                    in_flight[ex.submit(fetch_and_parse, nxt[0])] = nxt

            fill()
            while in_flight:
                finished, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in finished:
                    url, lastmod = in_flight.pop(fut)
                    item, status, err = fut.result()
                    done += 1

                    if err or not item:
                        failed += 1
                        log.warning("Failed url=%s status=%s err=%s", url, status, err)
                        write_failed(url, status)
                    elif cutoff is not None and (d := db.parse_answered_date(item.get("answered_date"))) \
                            and d < cutoff:
                        # sitemap-lastmod var ny, men selve svaret er for gammelt
                        skipped += 1
                    elif write(item, status, lastmod):
                        saved += 1
                    else:
                        failed += 1
                        write_failed(url, None)

                    pending_db += 1
                    if pending_db >= COMMIT_EVERY:
                        if not db.safe_commit(conn, f"batch_{done}"):
                            conn = db.ensure_conn(conn)
                        pending_db = 0

                    if done % 100 == 0 or done == total:
                        log.info("Progress %d/%d saved=%d skipped=%d failed=%d", done, total, saved, skipped, failed)
                fill()

        if pending_db and not db.safe_commit(conn, "final"):
            conn = db.ensure_conn(conn)
        log.info("Crawl done: saved=%d skipped=%d failed=%d total=%d", saved, skipped, failed, total)
    finally:
        try:
            conn.close()
        except Exception:
            pass
