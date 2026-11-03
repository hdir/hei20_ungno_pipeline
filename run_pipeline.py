"""
Nattlig jobb: crawler ung.no Q&A til Azure SQL og beriker dataene.

  1. crawl              nye /oss/-sider fra sitemap -> dbo.qa
  1b. prune             slett rader besvart før QA_MAX_AGE_MONTHS (default 12)
  2. kategorisering     hovedtema              (RUN_TOPIC_CATEGORIZATION=0 hopper over)
  3. PSA/SSA            undertemaer            (RUN_PSA_SSA_CATEGORIZATION=0 hopper over)
  4. topic discovery    ukentlige klynger+LLM  (RUN_WEEKLY_TOPIC_DISCOVERY=0 hopper over)
"""
import os
import subprocess
import sys
from pathlib import Path

from pipeline import categorize, crawl, db
from pipeline.logging_setup import setup_logging

log = setup_logging()


def enabled(env_var: str) -> bool:
    if os.getenv(env_var, "1") == "1":
        return True
    log.info("Skipping step (%s!=1)", env_var)
    return False


def prune() -> None:
    cutoff = db.qa_cutoff_date()
    if cutoff is None:
        log.info("Skipping prune (QA_MAX_AGE_MONTHS=0)")
        return
    conn = db.connect_mssql()
    try:
        deleted = db.prune_old_qa(conn, cutoff)
        log.info("Pruned %d rows answered before %s (QA_MAX_AGE_MONTHS=%d)", deleted, cutoff, db.QA_MAX_AGE_MONTHS)
    finally:
        conn.close()


def main() -> None:
    log.info("== 1/4 crawl ==")
    crawl.main()
    prune()

    if enabled("RUN_TOPIC_CATEGORIZATION"):
        log.info("== 2/4 topic categorization ==")
        categorize.run_main_topics()

    if enabled("RUN_PSA_SSA_CATEGORIZATION"):
        log.info("== 3/4 PSA/SSA sub-topic categorization ==")
        categorize.run_psa_ssa()

    if enabled("RUN_WEEKLY_TOPIC_DISCOVERY"):
        # Egen prosess så minnet fra sentence-transformers/torch frigjøres først
        log.info("== 4/4 weekly topic discovery (subprocess) ==")
        subprocess.run([sys.executable, "-u", "-m", "pipeline.discover_weekly"],
                       check=True, cwd=Path(__file__).resolve().parent)

    log.info("run_pipeline executed successfully")


if __name__ == "__main__":
    main()
