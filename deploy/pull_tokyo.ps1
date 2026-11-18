# Download the daily Parquet exports from the Tokyo recorder (hl-tokyo) to this PC.
#
# The server packs each finished UTC day into /home/ubuntu/export/<day>.tar at 00:30 UTC
# (/usr/local/bin/hl-export.sh, /etc/cron.d/hl-export). This script fetches every day it has not
# imported yet and unpacks it into <Dest>\parquet\<channel>\<coin>\<day>\<HH>.parquet, the same
# layout as the local recorder, so research code can point ALGO_DATA_ROOT at <Dest>.
#
#   .\deploy\pull_tokyo.ps1
#
# The SSH key is Lightsail's default key for Tokyo. If it is still in Downloads, the script moves it
# to ~\.ssh and restricts its permissions (OpenSSH refuses keys other users can read).
param(
    [string]$Key = "$HOME\.ssh\lightsail-tokyo.pem",
    [string]$Server = "ubuntu@52.195.230.182",
    [string]$Dest = "$HOME\short-horizon-trading\data-tokyo"
)
$ErrorActionPreference = "Stop"

if (-not (Test-Path $Key)) {
    $alt = "$HOME\Downloads\LightsailDefaultKey-ap-northeast-1.pem"
    if (-not (Test-Path $alt)) { throw "SSH key not found at $Key or $alt" }
    New-Item -ItemType Directory -Force "$HOME\.ssh" | Out-Null
    Move-Item $alt $Key
    Write-Host "moved the key to $Key"
}
# grant read access to the current user only, by SID: a name like "David" can resolve to the
# computer itself when the PC has the same name
$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
icacls $Key /reset | Out-Null
icacls $Key /inheritance:r /grant:r "*${sid}:(R)" | Out-Null

New-Item -ItemType Directory -Force "$Dest\parquet" | Out-Null
$log = "$Dest\imported.txt"
if (-not (Test-Path $log)) { New-Item -ItemType File $log | Out-Null }
$imported = @(Get-Content $log)

$remote = @(ssh -i $Key -o StrictHostKeyChecking=accept-new $Server "ls /home/ubuntu/export" |
            Where-Object { $_ -like "*.tar" })
if ($LASTEXITCODE -ne 0) { throw "ssh failed (exit $LASTEXITCODE)" }

$new = 0
foreach ($f in $remote) {
    if ($imported -contains $f) { continue }
    Write-Host "downloading $f ..."
    scp -q -i $Key "${Server}:/home/ubuntu/export/$f" "$Dest\$f"
    if ($LASTEXITCODE -ne 0) { throw "scp failed for $f" }
    tar -xf "$Dest\$f" -C "$Dest\parquet"
    if ($LASTEXITCODE -ne 0) { throw "unpacking $f failed" }
    Remove-Item "$Dest\$f"
    Add-Content $log $f
    $new++
    Write-Host "imported $f"
}
$size = "{0:N1} GB" -f ((Get-ChildItem "$Dest\parquet" -Recurse -File | Measure-Object Length -Sum).Sum / 1GB)
Write-Host "done: $new new day(s); $($remote.Count) on the server; local Tokyo data $size in $Dest\parquet"
