param(
    [string]$Database = "data/local/scei.db",
    [string]$Destination = "backups"
)

$destinationPath = New-Item -ItemType Directory -Force -Path $Destination
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$target = Join-Path $destinationPath "scei-$timestamp.db"
Copy-Item -LiteralPath $Database -Destination $target
Write-Output "Backup created: $target"
