# Bygger imaget i ACR, peker jobben på det nye imaget og starter en kjøring.
# Bruk: .\deploy-job.ps1 [-NoStart]
param(
  [string]$ResourceGroup = "rg-webappaichatbot",
  [string]$JobName = "ung-crawler-job",
  [string]$AcrName = "crungcrawler",
  [string]$ImageName = "ung-crawler",
  [switch]$NoStart
)
# NB: ikke $ErrorActionPreference = "Stop" - i PowerShell 5.1 blir az-advarsler på stderr da
# til fatale feil. Hvert steg sjekker $LASTEXITCODE i stedet.

# Tag: git commit-hash hvis prosjektet er et git-repo, ellers tidsstempel
$tag = git rev-parse --short HEAD 2>$null
if ($LASTEXITCODE -ne 0 -or -not $tag) {
  $tag = Get-Date -Format "yyyyMMdd-HHmmss"
} elseif (git status --porcelain --untracked-files=no) {
  Write-Warning "Du har endringer som ikke er committet - imaget tagges likevel med HEAD ($tag)."
}

$loginServer = az acr show -n $AcrName --query loginServer -o tsv
if ($LASTEXITCODE -ne 0) { throw "fant ikke ACR $AcrName" }

# --no-logs: venter til bygget er ferdig uten å streame loggen
# (streaming krasjer på æøå i Windows-konsollen)
az acr build -r $AcrName -t "${ImageName}:$tag" -t "${ImageName}:latest" --no-logs .
if ($LASTEXITCODE -ne 0) { throw "acr build feilet (se: az acr task list-runs -r $AcrName --top 1)" }

az containerapp job update -n $JobName -g $ResourceGroup `
  --image "${loginServer}/${ImageName}:$tag" --output none
if ($LASTEXITCODE -ne 0) { throw "job update feilet" }
Write-Host "Jobben bruker nå ${loginServer}/${ImageName}:$tag"

if (-not $NoStart) {
  az containerapp job start -n $JobName -g $ResourceGroup --query name -o tsv
  if ($LASTEXITCODE -ne 0) { throw "job start feilet" }
}
