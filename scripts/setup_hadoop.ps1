# Telecharge winutils + hadoop.dll pour PySpark sur Windows
$ErrorActionPreference = "Stop"
$binDir = Join-Path $PSScriptRoot "..\tools\hadoop\bin"
New-Item -ItemType Directory -Force -Path $binDir | Out-Null

$base = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.0.0/bin"
curl.exe -L "$base/winutils.exe" -o (Join-Path $binDir "winutils.exe")
curl.exe -L "$base/hadoop.dll" -o (Join-Path $binDir "hadoop.dll")

Write-Host "Hadoop Windows binaries instalés dans tools/hadoop/bin"
