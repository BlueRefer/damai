[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
$ErrorActionPreference = "SilentlyContinue"

$composeFile = Join-Path $PSScriptRoot "docker-compose.infra.yml"

Write-Host "`n========== Docker 状态 ==========" -ForegroundColor Cyan
docker compose -f $composeFile ps

$ports = @(
    @{ Name = "MySQL";        Port = 3306 },
    @{ Name = "Redis";        Port = 6379 },
    @{ Name = "Nacos";        Port = 8848 },
    @{ Name = "Kafka";        Port = 9092 },
    @{ Name = "Elasticsearch";Port = 9200 },
    @{ Name = "User";         Port = 6082 },
    @{ Name = "BaseData";     Port = 6083 },
    @{ Name = "Customize";    Port = 6084 },
    @{ Name = "Gateway";      Port = 6085 },
    @{ Name = "Program";      Port = 6086 },
    @{ Name = "Pay";          Port = 6087 },
    @{ Name = "Order";        Port = 8081 },
    @{ Name = "Admin";        Port = 10082 },
    @{ Name = "Vue";          Port = 5173 }
)

Write-Host "`n========== 端口状态 ==========" -ForegroundColor Cyan

$results = foreach ($item in $ports) {
    $client = New-Object System.Net.Sockets.TcpClient

    try {
        $connect = $client.BeginConnect(
            "127.0.0.1",
            $item.Port,
            $null,
            $null
        )

        $success = $connect.AsyncWaitHandle.WaitOne(500)

        if ($success) {
            $client.EndConnect($connect)
        }

        [PSCustomObject]@{
            Service = $item.Name
            Port    = $item.Port
            Running = $success
        }
    }
    catch {
        [PSCustomObject]@{
            Service = $item.Name
            Port    = $item.Port
            Running = $false
        }
    }
    finally {
        $client.Close()
    }
}

$results | Format-Table -AutoSize

Write-Host "`n========== 系统内存 ==========" -ForegroundColor Cyan

$os = Get-CimInstance Win32_OperatingSystem

[PSCustomObject]@{
    TotalGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
    FreeGB  = [math]::Round($os.FreePhysicalMemory / 1MB, 2)
    UsedPct = [math]::Round(
        (1 - $os.FreePhysicalMemory / $os.TotalVisibleMemorySize) * 100,
        2
    )
} | Format-Table -AutoSize