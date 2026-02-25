# Run the NYC taxi pipeline with PyArrow timezone fix for Windows.
# PyArrow on Windows needs TZDIR to locate the IANA timezone database.
# See: https://arrow.apache.org/docs/python/install.html#tzdata-on-windows

$tzdataPath = $null
# Try to get tzdata path from ingestr's environment (after: uv tool install ingestr --with tzdata)
$ingestrRoot = "$env:APPDATA\uv\tools\ingestr"
if (Test-Path "$ingestrRoot\Lib\site-packages\tzdata") {
    $tzdataPath = "$ingestrRoot\Lib\site-packages\tzdata"
}
if (-not $tzdataPath -and (Get-Command python -ErrorAction SilentlyContinue)) {
    $tzdataPath = python -c "import tzdata, os; print(os.path.dirname(tzdata.__file__))" 2>$null
}
if ($tzdataPath) {
    $env:TZDIR = $tzdataPath
    Write-Host "TZDIR set to: $tzdataPath"
} else {
    Write-Host "Warning: tzdata not found. Install with: uv tool install ingestr --with tzdata --force"
    Write-Host "Or: pip install tzdata"
}

bruin run ./pipeline/pipeline.yml @args
