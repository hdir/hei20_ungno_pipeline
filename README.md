# ung-crawler-job

Nattlig batchjobb som henter alle spørsmål og svar fra **ung.no/oss** inn i Azure SQL, og beriker dem med temaer:

1. **Crawl:** Leser `https://www.ung.no/sitemap.xml`, finner nye `/oss/`-sider og lagrer tittel, kjønn/alder, spørsmål, svar og besvart-dato i `dbo.qa`.
2. **Prune:** Sletter rader som ble besvart for mer enn `QA_MAX_AGE_MONTHS` måneder siden (standard 12).
3. **Kategorisering:** Gir hvert spørsmål ett hovedtema (44 faste temaer) med en lokal embedding-modell.
4. **PSA/SSA-kategorisering:** Gir spørsmål innen seksuell helse og relasjoner et undertema (definert i `psa_ssa_topics.json`).
5. **Ukentlig topic discovery:** Klynger forrige ukes spørsmål og lar Azure OpenAI navngi de 10 største temaene.

Jobben kjører som en **Azure Container Apps Job** hver natt kl. 02:00 UTC. All kode starter i `run_pipeline.py`.

---

## Innhold

- [Arkitektur](#arkitektur)
- [Prosjektstruktur](#prosjektstruktur)
- [Datamodell](#datamodell)
- [Konfigurasjon](#konfigurasjon)
- [Kjøre lokalt](#kjøre-lokalt)
- [Reetablere jobben i Azure fra bunnen](#reetablere-jobben-i-azure-fra-bunnen)
- [Deploy etter kodeendring](#deploy-etter-kodeendring)
- [Drift](#drift)
- [Vanlige oppgaver](#vanlige-oppgaver)
- [Feilsøking](#feilsøking)
- [Endringer fra ungno_utils](#endringer-fra-ungno_utils)

---

## Arkitektur

```
 ung.no sitemap ──► [1 crawl] ──► dbo.qa ◄── [1b prune]
                                    │
                                    ├──► [2 kategorisering]   topic_top1, topics_multi
                                    │        sentence-transformers (lokalt, CPU)
                                    │
                                    ├──► [3 PSA/SSA]          topic_top1_psa_ssa, topics_multi_psa_ssa
                                    │        sentence-transformers + psa_ssa_topics.json
                                    │
                                    └──► [4 topic discovery]  qa_discovered_topics_weekly
                                             Azure OpenAI       qa_discovered_topic_questions_weekly
                                             embeddings + KMeans + chat
```

| Steg | Modul | Hva skjer | Kan slås av med |
|---|---|---|---|
| 1 | `pipeline/crawl.py` | Leser sitemapen rekursivt og tar med `/oss/`-URL-er med `lastmod` innenfor aldersgrensen. **Bare URL-er som ikke allerede finnes i `dbo.qa`** blir crawlet, med 8 tråder og 20 forespørsler samtidig. Siden blir parset med lxml: først i dagens layout fra 2026, ellers i den gamle h2-layouten. Mislykkede sider blir lagret med `ok=0`. | – |
| 1b | `pipeline/db.py` → `prune_old_qa` | `DELETE` på rader der `answered_date` (eller `sitemap_lastmod` som reserve) er eldre enn cutoff. | `QA_MAX_AGE_MONTHS=0` |
| 2 | `pipeline/categorize.py` → `run_main_topics` | Gjelder rader med `ok=1` og tom `topic_top1`. Spørsmål og svar blir embeddet med `paraphrase-multilingual-MiniLM-L12-v2`. Raden får temaet med høyest cosinus-likhet, og `topics_multi` får alle temaer med likhet ≥ 0,38. Alle rader får et tema. | `RUN_TOPIC_CATEGORIZATION=0` |
| 3 | `pipeline/categorize.py` → `run_psa_ssa` | Samme metode, men bare på rader der `topic_top1` er et av 12 PSA/SSA-hovedtemaer. Rader uten treff over terskelen får `''` / `[]`, og de regnes som ferdige. | `RUN_PSA_SSA_CATEGORIZATION=0` |
| 4 | `pipeline/discover_weekly.py` | Gjelder forrige hele ISO-uke (Europe/Oslo). Spørsmålene blir embeddet med Azure OpenAI og klynget med KMeans (k ≤ 10). For hver klynge lager LLM-en `label`, `description` og `keywords`. En uke som allerede er generert blir hoppet over. Steget kjører som **egen prosess** slik at minnet fra torch-modellen blir frigjort først. | `RUN_WEEKLY_TOPIC_DISCOVERY=0` |

Alle steg er **inkrementelle** og kan trygt kjøres på nytt. Tabeller og kolonner blir opprettet automatisk første gang jobben kjører.

Feiler et steg, avsluttes prosessen med en exit-kode ulik 0, og Azure markerer kjøringen som *Failed*. Jobben prøver én gang til (`replica-retry-limit 1`).

---

## Prosjektstruktur

```
ungno_pipeline/
├── run_pipeline.py            # Entrypoint: kjører stegene i rekkefølge
├── pipeline/
│   ├── __init__.py            # Laster .env (kun lokalt)
│   ├── logging_setup.py       # Logging til stdout med UTC-tid
│   ├── db.py                  # Azure SQL: tilkobling med retry, schema, upsert, prune
│   ├── crawl.py               # Steg 1: sitemap + parsing av Q&A-sider
│   ├── categorize.py          # Steg 2 og 3: tema- og PSA/SSA-kategorisering
│   ├── discover_weekly.py     # Steg 4: ukentlig klynging + LLM-navngiving
│   └── psa_ssa_topics.json    # Undertemaer for PSA/SSA: {label: [nøkkelord, ...]}
├── requirements.txt
├── Dockerfile                 # python:3.11-slim + ODBC Driver 18 + CPU-torch + modellen
├── .env.example               # Mal for .env
├── create-job.ps1             # Oppretter jobben / setter secrets, env-vars og image
└── deploy-job.ps1             # Bygger image i ACR, oppdaterer jobben og starter en kjøring
```

---

## Datamodell

Databasen er `UngNo_QA` på `digiung.database.windows.net` (Azure SQL, serverless).

### `dbo.qa`: én rad per Q&A-side

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `url` (PK) | NVARCHAR(2048) | f.eks. `https://www.ung.no/oss/01370m7IyTQngs4is3s2Cw` |
| `title` | NVARCHAR(512) | Overskrift (h1) |
| `meta` | NVARCHAR(MAX) | Rå tekst, f.eks. `Jente, 16` |
| `gender`, `age` | NVARCHAR(64), INT | Parset fra `meta` |
| `question`, `answer` | NVARCHAR(MAX) | Spørsmål og svar som ren tekst |
| `answered_date` | NVARCHAR(64) | `d.m.yyyy` som på siden (konverter med `TRY_CONVERT(date, answered_date, 104)`) |
| `status` | INT | HTTP-status fra siste henting |
| `ok` | BIT | 1 = parset OK, 0 = henting feilet |
| `sitemap_lastmod` | NVARCHAR(64) | ISO-tid fra sitemapen |
| `content_hash` | NVARCHAR(64) | SHA-256 av innholdet |
| `updated_at` | DATETIME2 | Sist endret (UTC) |
| `topic_top1`, `topics_multi` | NVARCHAR(256), NVARCHAR(MAX) | Steg 2: hovedtema + JSON-liste |
| `topic_top1_psa_ssa`, `topics_multi_psa_ssa` | NVARCHAR(256), NVARCHAR(MAX) | Steg 3: undertema + JSON-liste |

### `dbo.qa_discovered_topics_weekly`: ett tema per uke og rang

PK er (`week_start`, `topic_rank`). Kolonnene er `week_end`, `topic_label`, `topic_description`, `keywords_json` og `question_count`. Rang 1 er den største klyngen.

### `dbo.qa_discovered_topic_questions_weekly`: hvilke spørsmål hører til hvilket tema

PK er (`week_start`, `topic_rank`, `url`). Kolonnene er `answered_date`, `answered_date_raw`, `title` og `question`.

> Andre systemer leser disse tabellene, blant annet `digiung_lab-query_server`. Ikke endre tabell- eller kolonnenavn uten å oppdatere dem.

---

## Konfigurasjon

Lokalt leses alt fra `.env` i prosjektroten (kopier `.env.example`). I Azure ligger det som secrets og env-vars på jobben, satt av `create-job.ps1`.

**Påkrevd**

| Variabel | Brukes av | Secret i Azure |
|---|---|---|
| `UNG_AZURESQL_ODBC` | alle steg | `ung-azuresql-odbc` |
| `AZURE_OPENAI_API_KEY` | steg 4 (chat) | `openai-api-key` |
| `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_DEPLOYMENT_NAME` | steg 4 (chat) | – |
| `AZURE_OPENAI_EMBEDDINGS_API_KEY` | steg 4 (embeddings) | `embeddings-api-key` |
| `AZURE_OPENAI_EMBEDDINGS_ENDPOINT`, `AZURE_OPENAI_EMBEDDINGS_API_VERSION`, `AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT` | steg 4 (embeddings) | – |

Formatet på ODBC-strengen er:

```
Driver={ODBC Driver 18 for SQL Server};Server=tcp:digiung.database.windows.net,1433;Database=UngNo_QA;Uid=digiung;Pwd=<PASSORD>;Encrypt=yes;TrustServerCertificate=no;
```

**Valgfritt**

| Variabel | Standard | Effekt |
|---|---|---|
| `QA_MAX_AGE_MONTHS` | `12` | Aldersgrense for crawl og prune. `0` = behold alt. |
| `RUN_TOPIC_CATEGORIZATION` | `1` | `0` hopper over steg 2. |
| `RUN_PSA_SSA_CATEGORIZATION` | `1` | `0` hopper over steg 3. |
| `RUN_WEEKLY_TOPIC_DISCOVERY` | `1` | `0` hopper over steg 4. |
| `FORCE_RECATEGORIZE_PSA_SSA` | `0` | `1` nullstiller og kjører steg 3 på nytt for alle rader. |
| `PSA_SSA_TOPICS_PATH` | `pipeline/psa_ssa_topics.json` | Alternativ fil med undertemaer. |
| `DISCOVERY_WEEKS` | `1` | Antall hele uker bakover som steg 4 behandler. |
| `DISCOVERY_OVERWRITE` | `0` | `1` regenererer uker som allerede finnes. |
| `COMMIT_EVERY` | `200` | Antall rader mellom hver commit i crawlen. |
| `MAX_URLS` | `800000` | Øvre grense for antall sitemap-URL-er. |
| `LOG_LEVEL` | `INFO` | |
| `HF_TOKEN` | – | Trengs bare hvis Hugging Face begrenser nedlastingen av modellen ved lokal kjøring. Modellen ligger ferdig i Docker-imaget. |

**Aldri commit `.env`.** Den er med i `.gitignore` og `.dockerignore`.

---

## Kjøre lokalt

**Forutsetninger**

- Python 3.11 eller nyere
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Azure CLI (`az login`) med tilgang til ressursgruppen `rg-webappaichatbot`
- En brannmurregel for din IP på SQL-serveren

```powershell
cd ungno_pipeline
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env      # fyll inn verdier

# Åpne SQL-brannmuren for din IP (gjenta hvis du bytter nettverk)
$MYIP = (Invoke-RestMethod https://api.ipify.org)
az sql server firewall-rule create -g rg-webappaichatbot -s digiung -n MyIP --start-ip-address $MYIP --end-ip-address $MYIP
# finnes regelen fra før: bytt "create" med "update"

python -u run_pipeline.py
```

> **NB:** Lokal kjøring skriver til **produksjonsdatabasen**. Vil du teste uten å endre data, slå av stegene du ikke trenger (`$env:RUN_TOPIC_CATEGORIZATION="0"` osv.). Du kan også peke `UNG_AZURESQL_ODBC` mot en egen testdatabase. Tabellene blir opprettet automatisk.

Første gang lastes embedding-modellen (ca. 470 MB) ned fra Hugging Face. Den første crawlen mot en tom database tar flere timer.

**Kjøre enkeltsteg**

```powershell
python -c "from pipeline import crawl; from pipeline.logging_setup import setup_logging; setup_logging(); crawl.main()"
python -c "from pipeline import categorize as c; from pipeline.logging_setup import setup_logging; setup_logging(); c.run_main_topics()"
python -c "from pipeline import categorize as c; from pipeline.logging_setup import setup_logging; setup_logging(); c.run_psa_ssa()"
python -m pipeline.discover_weekly --weeks 1 --overwrite
```

**Teste Docker-imaget lokalt**

```powershell
docker build -t ung-crawler .
docker run --rm --env-file .env ung-crawler
```

---

## Reetablere jobben i Azure fra bunnen

Abonnementet er **HDIR-DigiUngSøk** og ressursgruppen er **`rg-webappaichatbot`** (North Europe).

| Ressurs | Navn | Merknad |
|---|---|---|
| SQL-server | `digiung` | Admin-bruker `digiung` |
| Database | `UngNo_QA` | Serverless (Gen5, 1 vCore, auto-pause 60 min) |
| Container Registry | `crungcrawler` | Basic, image `ung-crawler` |
| Log Analytics | `workspace-rgwebappaichatbotk7fL` | Finnes fra før |
| Container Apps-miljø | `aca-env-ung` | |
| Container Apps Job | `ung-crawler-job` | Cron `0 2 * * *` (UTC), 2 CPU / 4 GiB, timeout 3 t, 1 retry |
| Azure OpenAI | (eksisterende) | Én chat-deployment og én embeddings-deployment |

Kjør stegene i PowerShell fra prosjektmappen etter `az login` og `az account set --subscription "HDIR-DigiUngSøk"`:

```powershell
$RG = "rg-webappaichatbot"
$LOC = "northeurope"

# 1) SQL-server + database (hopp over hvis de finnes; dataene bygges opp igjen av crawlen)
az sql server create -g $RG -n digiung -l $LOC --admin-user digiung --admin-password "<PASSORD>"
az sql server firewall-rule create -g $RG -s digiung -n AllowAzure --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
az sql db create -g $RG -s digiung -n UngNo_QA --edition GeneralPurpose --compute-model Serverless `
  --family Gen5 --capacity 1 --auto-pause-delay 60

# 2) Container Registry + første image
az acr create -g $RG -n crungcrawler --sku Basic -l $LOC
az acr build -r crungcrawler -t "ung-crawler:latest" --no-logs .

# 3) Container Apps-miljø koblet til eksisterende Log Analytics
$WS_ID  = az monitor log-analytics workspace show -g $RG -n workspace-rgwebappaichatbotk7fL --query customerId -o tsv
$WS_KEY = az monitor log-analytics workspace get-shared-keys -g $RG -n workspace-rgwebappaichatbotk7fL --query primarySharedKey -o tsv
az containerapp env create -g $RG -n aca-env-ung -l $LOC --logs-workspace-id $WS_ID --logs-workspace-key $WS_KEY

# 4) Fyll inn .env (se Konfigurasjon) og opprett jobben med secrets, env-vars og image
.\create-job.ps1 -Tag latest

# 5) Start en kjøring og følg loggen
az containerapp job start -n ung-crawler-job -g $RG
az containerapp job logs show -n ung-crawler-job -g $RG --container ung-crawler-job --tail 200 --follow
```

`create-job.ps1` gjør dette:

1. Oppretter jobben med schedule-trigger og et placeholder-image, hvis jobben ikke finnes.
2. Slår på ACR admin-bruker og gir jobben pull-tilgang. Vi har ikke rettighet til å tildele AcrPull-rollen, derfor brukes admin-bruker.
3. Setter de tre secretene.
4. Setter image og alle env-vars. De valgfrie variablene fra `.env` blir også med.

Skriptet kan kjøres på nytt når som helst, for eksempel etter at en nøkkel er byttet.

> **Nytt SQL-passord?** Oppdater `.env` her og kjør `.\create-job.ps1`. Oppdater også `.env` i `hei20_article_indexer`, `digiung_lab-query_server` og `vectorindex_editor_server`, som bruker samme database.

---

## Deploy etter kodeendring

```powershell
.\deploy-job.ps1            # bygg, oppdater jobben, start en kjøring
.\deploy-job.ps1 -NoStart   # bygg og oppdater, men vent til neste nattkjøring
```

Imaget blir bygget i ACR (`az acr build`), så du trenger ikke Docker lokalt. Det får tag med git commit-hash, eller tidsstempel hvis mappen ikke er et git-repo, i tillegg til `latest`. Skal nye filer med, legger du dem under `pipeline/`. Dockerfile kopierer hele mappen.

---

## Drift

```powershell
$RG = "rg-webappaichatbot"; $JOB = "ung-crawler-job"

az containerapp job start -n $JOB -g $RG                               # manuell kjøring
az containerapp job execution list -n $JOB -g $RG -o table             # historikk
az containerapp job logs show -n $JOB -g $RG --container $JOB --tail 200 --follow

# en bestemt kjøring
$EXEC = "ung-crawler-job-xxxxxxx"
az containerapp job logs show -n $JOB -g $RG --execution $EXEC --container $JOB --tail 200
az containerapp job execution show -n $JOB -g $RG --job-execution-name $EXEC --query properties -o json

# mer ressurser / endre tidspunkt
az containerapp job update -n $JOB -g $RG --cpu 2.0 --memory 4.0Gi
az containerapp job update -n $JOB -g $RG --cron-expression "0 2 * * *"
```

Hver kjøring logger linjer som starter med `== 1/4 crawl ==` osv., og avslutter med `run_pipeline executed successfully`.

**Nyttige kontrollspørringer**

```sql
-- Nye rader siste døgn og fordeling ok/feil
SELECT ok, COUNT(*) FROM dbo.qa WHERE updated_at > DATEADD(day, -1, SYSUTCDATETIME()) GROUP BY ok;

-- Rader som venter på kategorisering
SELECT COUNT(*) FROM dbo.qa WHERE ok = 1 AND (topic_top1 IS NULL OR topic_top1 = '');

-- Siste genererte uke
SELECT TOP 10 * FROM dbo.qa_discovered_topics_weekly ORDER BY week_start DESC, topic_rank;
```

---

## Vanlige oppgaver

**Endre PSA/SSA-undertemaer.** Rediger `pipeline/psa_ssa_topics.json`. Formatet er `{"Label": ["nøkkelord", ...]}`. Nøkkelordene blir slått sammen med komma og embeddet. Deploy, og kjør deretter én re-kategorisering slik at eksisterende rader får nye labels. Slå flagget av igjen etterpå:

```powershell
$RG = "rg-webappaichatbot"; $JOB = "ung-crawler-job"
az containerapp job update -n $JOB -g $RG --set-env-vars FORCE_RECATEGORIZE_PSA_SSA=1
az containerapp job start  -n $JOB -g $RG
# når kjøringen er ferdig:
az containerapp job update -n $JOB -g $RG --remove-env-vars FORCE_RECATEGORIZE_PSA_SSA
```

**Endre hovedtemaer.** Rediger `MAIN_TOPICS` i `pipeline/categorize.py`. Bare rader uten tema blir kategorisert automatisk. For å kategorisere alt på nytt, nullstill kolonnene først:

```sql
UPDATE dbo.qa SET topic_top1 = NULL, topics_multi = NULL;
```

Merk at PSA/SSA-steget filtrerer på `topic_top1`. Hvis du endrer navn på et hovedtema, må du også oppdatere `PSA_SSA_PARENT_TOPICS`.

**Regenerere ukentlige topics.** Kjør `python -m pipeline.discover_weekly --weeks 4 --overwrite` lokalt.

**ung.no har endret sidelayout.** Hvis mange nye rader får tom `question`/`answer`, eller loggen viser mange `Failed url=`, må du oppdatere `extract_qa()` i `pipeline/crawl.py`. Den støtter i dag 2026-layouten (h1 → div med pill + `div.prose`, svar før `Besvart:`) og den gamle h2-layouten. Rader som allerede er crawlet blir ikke hentet på nytt. Slett dem fra `dbo.qa` hvis de skal parses på nytt.

---

## Feilsøking

| Symptom | Årsak / løsning |
|---|---|
| `Azure SQL resuming (40613)` i loggen | Normalt. Den serverless databasen våkner, og tilkoblingen prøver igjen i opptil ca. 6 min. |
| `Login timeout` / `Cannot open server` lokalt | IP-en din mangler i SQL-brannmuren (se [Kjøre lokalt](#kjøre-lokalt)). |
| `Communication link failure (08S01)` | Tilkoblingen ble brutt underveis i crawlen. Koden kobler til på nytt. Rader som ikke var committet, blir crawlet igjen neste natt. |
| Kjøringen avbrytes etter 3 t | Typisk første gang mot en tom database. Kjør igjen; crawlen fortsetter der den slapp. Eventuelt øk `--replica-timeout`. |
| `OOMKilled` | Øk minnet: `az containerapp job update ... --memory 8.0Gi` (krever `--cpu 4.0`). |
| `json.decoder.JSONDecodeError` i steg 4 | LLM-en svarte ikke med gyldig JSON. Kjør steget på nytt. Får du feilen igjen, sjekk deployment-navn og modell. |
| `acr build` krasjer på æøå | Bruk `--no-logs` (som `deploy-job.ps1` gjør). Se byggeloggen med `az acr task list-runs -r crungcrawler --top 1`. |
| Jobben får ikke hentet imaget | Kjør `.\create-job.ps1` på nytt. Det setter registry-legitimasjonen igjen. |

---

