$hist = "C:\Users\FrancisWynne\AppData\Roaming\Microsoft\Windows\PowerShell\PSReadLine\ConsoleHost_history.txt"
$cfg  = "C:\Users\FrancisWynne\Desktop\.exe\navman-dashboard\config.json"

$lines = @()
if (Test-Path $hist) { $lines += Get-Content $hist }
if (Test-Path $cfg)  { $lines += Get-Content $cfg }

$candidates = $lines |
  Where-Object { $_ -match 'SessionId|NAVMAN_SESSION' } |
  ForEach-Object { [regex]::Matches($_,'(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b') } |
  ForEach-Object { $_.Value.ToLower() } |
  Where-Object { $_ -ne '00000000-0000-0000-0000-000000000000' } |
  Sort-Object -Unique

$candidates
