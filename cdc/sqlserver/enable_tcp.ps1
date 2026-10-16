# enable_tcp.ps1 - Enable TCP/IP on port 1433 for the default SQL Server instance (MSSQLSERVER)
# so Debezium (running in Docker) can connect. Must run as Administrator.
#
# Same changes as SQL Server Configuration Manager:
#   Protocols for MSSQLSERVER > TCP/IP > Enabled = Yes; IPAll TCP Port = 1433; TCP Dynamic Ports = blank
# Then restarts SQL Server and starts SQL Server Agent again (the restart stops it; CDC needs it).
# No firewall change: other machines stay blocked; Docker Desktop connects via the local host.

$ErrorActionPreference = 'Stop'
$log = Join-Path $PSScriptRoot 'enable_tcp.log'
Start-Transcript -Path $log -Force | Out-Null
try {
    $root = 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server'
    $instanceId = (Get-ItemProperty "$root\Instance Names\SQL").MSSQLSERVER
    if (-not $instanceId) { throw 'Default instance MSSQLSERVER not found.' }
    $tcp = "$root\$instanceId\MSSQLServer\SuperSocketNetLib\Tcp"

    Set-ItemProperty -Path $tcp -Name Enabled -Value 1
    Set-ItemProperty -Path "$tcp\IPAll" -Name TcpPort -Value '1433'
    Set-ItemProperty -Path "$tcp\IPAll" -Name TcpDynamicPorts -Value ''
    Write-Output "TCP/IP enabled on port 1433 for $instanceId"

    Restart-Service -Name MSSQLSERVER -Force
    Start-Service -Name SQLSERVERAGENT
    Get-Service MSSQLSERVER, SQLSERVERAGENT | ForEach-Object { Write-Output "$($_.Name): $($_.Status)" }
    Write-Output 'DONE'
}
catch {
    Write-Output "FAILED: $_"
}
finally {
    Stop-Transcript | Out-Null
}
