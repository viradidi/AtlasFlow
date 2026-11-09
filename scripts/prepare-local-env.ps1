$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function New-RandomHex([int]$Bytes = 24) {
  $buffer = New-Object byte[] $Bytes
  $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
  try { $rng.GetBytes($buffer) } finally { $rng.Dispose() }
  return ([System.BitConverter]::ToString($buffer)).Replace('-', '').ToLowerInvariant()
}


$ExamplePath = Join-Path $Root '.env.example'
$EnvPath = Join-Path $Root '.env'
if (-not (Test-Path -LiteralPath $ExamplePath)) { throw '.env.example is missing from the project root.' }

$Existing = @{}
if (Test-Path -LiteralPath $EnvPath) {
  foreach ($line in Get-Content -LiteralPath $EnvPath) {
    if ($line.Trim() -match '^([A-Z0-9_]+)=(.*)$') { $Existing[$Matches[1]] = $Matches[2] }
  }
}

$Lines = @()
foreach ($line in Get-Content -LiteralPath $ExamplePath) {
  if ($line -notmatch '^([A-Z0-9_]+)=(.*)$') { $Lines += $line; continue }
  $Key = $Matches[1]
  $Default = $Matches[2]
  $Value = if ($Existing.ContainsKey($Key)) { [string]$Existing[$Key] } else { $Default }
  $NeedsSecret = [string]::IsNullOrWhiteSpace($Value) -or $Value -match '^change_this_'
  if ($NeedsSecret) {
    if ($Key -in @('POSTGRES_PASSWORD','CDC_PASSWORD','RUSTFS_ACCESS_KEY','RUSTFS_SECRET_KEY','SUPERSET_DB_PASSWORD','SUPERSET_SECRET_KEY','SUPERSET_ADMIN_PASSWORD','GRAFANA_ADMIN_PASSWORD')) { $Value = New-RandomHex 24 }
    else { $Value = $Default }
  }
  # SQL templates inject object-store keys directly, so require simple safe characters.
  if ($Key -in @('RUSTFS_ACCESS_KEY','RUSTFS_SECRET_KEY') -and $Value -notmatch '^[A-Za-z0-9_-]{8,80}$') { $Value = New-RandomHex 24 }
  $Lines += "$Key=$Value"
}
[System.IO.File]::WriteAllText($EnvPath, (($Lines -join "`r`n").TrimEnd() + "`r`n"), [System.Text.UTF8Encoding]::new($false))
Write-Host 'PASS: local .env is ready. Existing non-placeholder secrets were preserved.' -ForegroundColor Green

if (-not (Get-Command python -ErrorAction SilentlyContinue)) { throw 'Python 3.10+ was not found on PATH.' }
$PythonExe = Join-Path $Root '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $PythonExe)) {
  python -m venv .venv
  if ($LASTEXITCODE -ne 0) { throw 'Could not create .venv. Check your Python installation.' }
}
& $PythonExe -m pip install --disable-pip-version-check -q -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed.' }
Write-Host 'PASS: Python environment and required dependencies are ready.' -ForegroundColor Green
Write-Host 'Next command: docker compose up -d --build' -ForegroundColor Cyan
Write-Host 'Keep .env private; do not commit or share it.' -ForegroundColor Yellow
