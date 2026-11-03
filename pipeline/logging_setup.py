import logging
import os
import sys
import time


def setup_logging() -> logging.Logger:
    """Container-vennlig logging til stdout med UTC-tidsstempler."""
    logger = logging.getLogger("ung_pipeline")
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)sZ | %(levelname)s | %(threadName)s | %(message)s")
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logging.getLogger("urllib3").setLevel("WARNING")
    return logger
