param(
  [string]$MosquittoDirectory = 'C:\Program Files\mosquitto'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$brokerExe = Join-Path $MosquittoDirectory 'mosquitto.exe'
$publisherExe = Join-Path $MosquittoDirectory 'mosquitto_pub.exe'
$subscriberExe = Join-Path $MosquittoDirectory 'mosquitto_sub.exe'
foreach ($exe in @($brokerExe, $publisherExe, $subscriberExe)) {
  if (-not (Test-Path -LiteralPath $exe)) { throw "Missing Mosquitto executable: $exe" }
}

Push-Location $projectRoot
try {
  moon build --target native
  if ($LASTEXITCODE -ne 0) { throw 'moon build failed' }

  $artifactDir = Join-Path $projectRoot '.tools\interop'
  New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
  $brokerLog = Join-Path $artifactDir 'broker.out.log'
  $brokerError = Join-Path $artifactDir 'broker.err.log'
  $cliMessage = Join-Path $artifactDir 'cli-subscriber.txt'
  $cliError = Join-Path $artifactDir 'cli-subscriber.err.log'
  $moonMessage = Join-Path $artifactDir 'moon-subscriber.txt'
  $moonError = Join-Path $artifactDir 'moon-subscriber.err.log'
  $moonPublisher = Join-Path $projectRoot '_build\native\debug\build\examples\interop_pub\interop_pub.exe'
  $moonSubscriber = Join-Path $projectRoot '_build\native\debug\build\examples\interop_sub\interop_sub.exe'
  $moonBasic = Join-Path $projectRoot '_build\native\debug\build\examples\basic\basic.exe'

  $brokerProcess = $null
  $cliProcess = $null
  $moonProcess = $null
  try {
    $brokerProcess = Start-Process -FilePath $brokerExe -ArgumentList @('-p', '1883', '-v') `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $brokerLog `
      -RedirectStandardError $brokerError
    Start-Sleep -Milliseconds 500
    if ($brokerProcess.HasExited) { throw "Mosquitto exited: $(Get-Content -Raw $brokerError)" }

    $basicOutput = & $moonBasic
    if ($LASTEXITCODE -ne 0 -or $basicOutput -notcontains 'moonmqtt/example: hello') {
      throw "Basic library example failed: $basicOutput"
    }

    $cliProcess = Start-Process -FilePath $subscriberExe `
      -ArgumentList @('-h', '127.0.0.1', '-p', '1883', '-t', 'moonmqtt/interop/out0', '-t', 'moonmqtt/interop/out', '-q', '1', '-C', '2', '-v') `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $cliMessage `
      -RedirectStandardError $cliError
    Start-Sleep -Milliseconds 500
    if ($cliProcess.HasExited) { throw "mosquitto_sub exited: $(Get-Content -Raw $cliError)" }
    & $moonPublisher
    if ($LASTEXITCODE -ne 0) { throw 'MoonBit QoS 1 publisher failed' }
    if (-not $cliProcess.WaitForExit(10000)) { throw 'mosquitto_sub timed out' }
    $cliOutput = Get-Content -Raw $cliMessage
    if ($cliOutput -notmatch 'moonmqtt/interop/out0 from MoonBit QoS0' -or
        $cliOutput -notmatch 'moonmqtt/interop/out from MoonBit') {
      throw "Mosquitto subscriber got unexpected data: $(Get-Content -Raw $cliMessage)"
    }

    $moonProcess = Start-Process -FilePath $moonSubscriber -WorkingDirectory $projectRoot `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $moonMessage `
      -RedirectStandardError $moonError
    $ready = $false
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
      if ($moonProcess.HasExited) { throw "MoonBit subscriber exited: $(Get-Content -Raw $moonError)" }
      $brokerOutput = Get-Content -Raw $brokerError
      if (([regex]::Matches($brokerOutput, 'Sending SUBACK to moonmqtt-interop-subscriber')).Count -ge 2) {
        $ready = $true
        break
      }
      Start-Sleep -Milliseconds 100
    }
    if (-not $ready) { throw 'MoonBit subscriber did not become ready' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/in0 -q 0 -m 'from Mosquitto QoS0'
    if ($LASTEXITCODE -ne 0) { throw 'mosquitto_pub QoS 0 failed' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/in -q 1 -m 'from Mosquitto'
    if ($LASTEXITCODE -ne 0) { throw 'mosquitto_pub QoS 1 failed' }
    if (-not $moonProcess.WaitForExit(10000)) { throw 'MoonBit subscriber timed out' }
    $moonOutput = Get-Content -Raw $moonMessage
    if ($moonOutput -notmatch 'moonmqtt/interop/in0: from Mosquitto QoS0' -or
        $moonOutput -notmatch 'moonmqtt/interop/in: from Mosquitto') {
      throw "MoonBit subscriber got unexpected data: $(Get-Content -Raw $moonMessage)"
    }
    Write-Output 'PASS: basic example and two-way MoonBit/Mosquitto CLI exchange (QoS 0 and 1)'
  }
  finally {
    foreach ($childProcess in @($moonProcess, $cliProcess, $brokerProcess)) {
      if ($null -ne $childProcess -and -not $childProcess.HasExited) {
        Stop-Process -Id $childProcess.Id -Force
      }
    }
  }
}
finally { Pop-Location }
