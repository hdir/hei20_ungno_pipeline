"""ung.no Q&A-pipeline: crawl -> prune -> kategorisering -> ukentlig topic discovery."""
from dotenv import find_dotenv, load_dotenv

# Lokalt: les .env fra prosjektroten. I Azure settes alt som env-vars/secrets på jobben.
load_dotenv(find_dotenv())
