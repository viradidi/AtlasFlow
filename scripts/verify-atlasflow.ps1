param([switch]$StaticOnly)
$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
Push-Location $Root
try {
  if (-not (Test-Path '.env') -or -not (Test-Path '.venv/Scripts/python.exe')) {
    & (Join-Path $PSScriptRoot 'prepare-local-env.ps1')
    if ($LASTEXITCODE -ne 0) { throw 'Local environment preparation failed.' }
  }
  $Python = (Resolve-Path '.venv/Scripts/python.exe').Path
  & $Python '.\scripts\check-atlasflow-static.py'
  if ($LASTEXITCODE -ne 0) { throw 'Static validation failed.' }
  if ($StaticOnly) { Write-Host 'Static-only verification complete.' -ForegroundColor Green; return }
  docker compose config --quiet
  if ($LASTEXITCODE -ne 0) { throw 'docker compose config failed.' }
  & $Python '.\scripts\smoke-test-atlasflow.py'
  if ($LASTEXITCODE -ne 0) { throw 'Live smoke test failed; inspect the first failing service.' }
  Write-Host 'AtlasFlow verification passed.' -ForegroundColor Green
} catch {
  Write-Error $_
  exit 1
} finally { Pop-Location }
