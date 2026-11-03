# Oppretter/oppdaterer Container Apps-jobben: registry-tilgang, secrets, env-vars og image.
# Forutsetter: ACR, Container Apps-miljø og image finnes, og .env ligger i denne mappen.
# Bruk: .\create-job.ps1 [-Tag latest]
param(
  [string]$ResourceGroup = "rg-webappaichatbot",
  [string]$JobName = "ung-crawler-job",
  [string]$Environment = "aca-env-ung",
  [string]$AcrName = "crungcrawler",
  [string]$ImageName = "ung-crawler",
  [string]$Tag = "latest",
  [string]$Cron = "0 2 * * *"
)
# NB: ikke $ErrorActionPreference = "Stop" - i PowerShell 5.1 blir az-advarsler på stderr da
# til fatale feil. Hvert steg sjekker $LASTEXITCODE i stedet.

# Les .env inn i en hashtable (skrives ikke ut)
if (-not (Test-Path ".env")) { throw "Mangler .env i denne mappen" }
$cfg = @{}
Get-Content .env | ForEach-Object {
  $line = $_.Trim()
  if ($line -and -not $line.StartsWith("#")) {
    $pair = $line -split "=", 2
    if ($pair.Length -eq 2) { $cfg[$pair[0].Trim()] = $pair[1].Trim().Trim('"') }
  }
}

$required = @(
  "UNG_AZURESQL_ODBC",
  "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_VERSION", "AZURE_OPENAI_DEPLOYMENT_NAME",
  "AZURE_OPENAI_EMBEDDINGS_API_KEY", "AZURE_OPENAI_EMBEDDINGS_ENDPOINT", "AZURE_OPENAI_EMBEDDINGS_API_VERSION",
  "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT"
)
foreach ($k in $required) {
  if ([string]::IsNullOrWhiteSpace($cfg[$k])) { throw "Mangler $k i .env" }
}

$loginServer = az acr show -n $AcrName --query loginServer -o tsv
if ($LASTEXITCODE -ne 0) { throw "fant ikke ACR $AcrName" }

# 1) Opprett jobben hvis den mangler (med placeholder-image; ekte image settes i steg 4)
$exists = az containerapp job list -g $ResourceGroup --query "[?name=='$JobName'].name" -o tsv
if (-not $exists) {
  az containerapp job create `
    -n $JobName -g $ResourceGroup `
    --environment $Environment `
    --trigger-type Schedule `
    --cron-expression $Cron `
    --image "mcr.microsoft.com/k8se/quickstart-jobs:latest" `
    --cpu 2.0 --memory 4.0Gi `
    --replica-timeout 10800 `
    --replica-retry-limit 1 `
    --replica-completion-count 1 `
    --parallelism 1 `
    --output none
  if ($LASTEXITCODE -ne 0) { throw "job create feilet" }
}

# 2) Registry-tilgang via ACR admin-bruker (krever ikke rettighet til å tildele AcrPull-rolle)
az acr update -n $AcrName --admin-enabled true --output none
$acrUser = az acr credential show -n $AcrName --query username -o tsv
$acrPass = az acr credential show -n $AcrName --query "passwords[0].value" -o tsv
az containerapp job registry set -n $JobName -g $ResourceGroup `
  --server $loginServer --username $acrUser --password $acrPass --output none
if ($LASTEXITCODE -ne 0) { throw "registry set feilet" }

# 3) Secrets (navn: små bokstaver + bindestrek)
az containerapp job secret set -n $JobName -g $ResourceGroup --output none --secrets `
  "ung-azuresql-odbc=$($cfg['UNG_AZURESQL_ODBC'])" `
  "openai-api-key=$($cfg['AZURE_OPENAI_API_KEY'])" `
  "embeddings-api-key=$($cfg['AZURE_OPENAI_EMBEDDINGS_API_KEY'])"
if ($LASTEXITCODE -ne 0) { throw "secret set feilet" }

# 4) Image + env-vars (hemmeligheter som secretref)
$envVars = @(
  "UNG_AZURESQL_ODBC=secretref:ung-azuresql-odbc",
  "AZURE_OPENAI_API_KEY=secretref:openai-api-key",
  "AZURE_OPENAI_EMBEDDINGS_API_KEY=secretref:embeddings-api-key",
  "AZURE_OPENAI_ENDPOINT=$($cfg['AZURE_OPENAI_ENDPOINT'])",
  "AZURE_OPENAI_API_VERSION=$($cfg['AZURE_OPENAI_API_VERSION'])",
  "AZURE_OPENAI_DEPLOYMENT_NAME=$($cfg['AZURE_OPENAI_DEPLOYMENT_NAME'])",
  "AZURE_OPENAI_EMBEDDINGS_ENDPOINT=$($cfg['AZURE_OPENAI_EMBEDDINGS_ENDPOINT'])",
  "AZURE_OPENAI_EMBEDDINGS_API_VERSION=$($cfg['AZURE_OPENAI_EMBEDDINGS_API_VERSION'])",
  "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT=$($cfg['AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT'])"
)
# Valgfrie styringsvariabler videreføres hvis de står i .env
foreach ($k in @("QA_MAX_AGE_MONTHS", "RUN_TOPIC_CATEGORIZATION", "RUN_PSA_SSA_CATEGORIZATION",
                 "RUN_WEEKLY_TOPIC_DISCOVERY", "FORCE_RECATEGORIZE_PSA_SSA", "DISCOVERY_WEEKS",
                 "DISCOVERY_OVERWRITE", "LOG_LEVEL")) {
  if ($cfg[$k]) { $envVars += "$k=$($cfg[$k])" }
}

az containerapp job update -n $JobName -g $ResourceGroup `
  --image "$loginServer/${ImageName}:$Tag" `
  --set-env-vars @envVars `
  --output none
if ($LASTEXITCODE -ne 0) { throw "job update feilet" }

az containerapp job show -n $JobName -g $ResourceGroup `
  --query "{image:properties.template.containers[0].image,cron:properties.configuration.scheduleTriggerConfig.cronExpression,state:properties.provisioningState}" -o table
