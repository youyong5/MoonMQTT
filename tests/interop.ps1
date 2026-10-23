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
  $moonResume = Join-Path $projectRoot '_build\native\debug\build\examples\session_resume\session_resume.exe'
  $processResume = Join-Path $projectRoot '_build\native\debug\build\examples\process_resume\process_resume.exe'
  $processSnapshot = Join-Path $artifactDir 'process-session.bin'
  $processSeedOutput = Join-Path $artifactDir 'process-seed.txt'
  $processSeedError = Join-Path $artifactDir 'process-seed.err.log'
  $processNewOutput = Join-Path $artifactDir 'process-new.txt'
  $processNewError = Join-Path $artifactDir 'process-new.err.log'
  $resumeOutput = Join-Path $artifactDir 'session-resume.txt'
  $resumeError = Join-Path $artifactDir 'session-resume.err.log'

  $brokerProcess = $null
  $cliProcess = $null
  $moonProcess = $null
  $resumeProcess = $null
  $processSeed = $null
  $processNew = $null
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
      -ArgumentList @('-h', '127.0.0.1', '-p', '1883', '-t', 'moonmqtt/interop/out0', '-t', 'moonmqtt/interop/out', '-t', 'moonmqtt/interop/out2', '-q', '2', '-C', '3', '-v') `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $cliMessage `
      -RedirectStandardError $cliError
    Start-Sleep -Milliseconds 500
    if ($cliProcess.HasExited) { throw "mosquitto_sub exited: $(Get-Content -Raw $cliError)" }
    & $moonPublisher
    if ($LASTEXITCODE -ne 0) { throw 'MoonBit QoS 1 publisher failed' }
    if (-not $cliProcess.WaitForExit(10000)) { throw 'mosquitto_sub timed out' }
    $cliOutput = Get-Content -Raw $cliMessage
    if ($cliOutput -notmatch 'moonmqtt/interop/out0 from MoonBit QoS0' -or
        $cliOutput -notmatch 'moonmqtt/interop/out from MoonBit' -or
        $cliOutput -notmatch 'moonmqtt/interop/out2 from MoonBit QoS2') {
      throw "Mosquitto subscriber got unexpected data: $(Get-Content -Raw $cliMessage)"
    }

    $moonProcess = Start-Process -FilePath $moonSubscriber -WorkingDirectory $projectRoot `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $moonMessage `
      -RedirectStandardError $moonError
    $ready = $false
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
      if ($moonProcess.HasExited) { throw "MoonBit subscriber exited: $(Get-Content -Raw $moonError)" }
      $brokerOutput = Get-Content -Raw $brokerError
      if (([regex]::Matches($brokerOutput, 'Sending SUBACK to moonmqtt-interop-subscriber')).Count -ge 3) {
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
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/in2 -q 2 -m 'from Mosquitto QoS2'
    if ($LASTEXITCODE -ne 0) { throw 'mosquitto_pub QoS 2 failed' }
    if (-not $moonProcess.WaitForExit(10000)) { throw 'MoonBit subscriber timed out' }
    $moonOutput = Get-Content -Raw $moonMessage
    if ($moonOutput -notmatch 'moonmqtt/interop/in0: from Mosquitto QoS0' -or
        $moonOutput -notmatch 'moonmqtt/interop/in: from Mosquitto' -or
        $moonOutput -notmatch 'moonmqtt/interop/in2: from Mosquitto QoS2') {
      throw "MoonBit subscriber got unexpected data: $(Get-Content -Raw $moonMessage)"
    }

    $resumeProcess = Start-Process -FilePath $moonResume -WorkingDirectory $projectRoot `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $resumeOutput `
      -RedirectStandardError $resumeError
    $offline = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
      if ($resumeProcess.HasExited) { throw "MoonBit resume example exited: $(Get-Content -Raw $resumeError)" }
      $brokerText = Get-Content -Raw $brokerError
      if ($brokerText -match 'Client moonmqtt-interop-resume disconnected') {
        $offline = $true
        break
      }
      Start-Sleep -Milliseconds 100
    }
    if (-not $offline) { throw 'MoonBit resume example did not become ready' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/resume/one -q 1 -m 'offline QoS1'
    if ($LASTEXITCODE -ne 0) { throw 'offline QoS 1 publish failed' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/resume/two -q 2 -m 'offline QoS2'
    if ($LASTEXITCODE -ne 0) { throw 'offline QoS 2 publish failed' }
    $control = $null
    for ($attempt = 0; $attempt -lt 50; $attempt++) {
      try {
        $control = [System.Net.Sockets.TcpClient]::new('127.0.0.1', 18884)
        break
      } catch {
        if ($resumeProcess.HasExited) { throw "MoonBit resume example exited: $(Get-Content -Raw $resumeError)" }
        Start-Sleep -Milliseconds 100
      }
    }
    if ($null -eq $control) { throw 'MoonBit resume control socket unavailable' }
    try { $control.GetStream().WriteByte(1) } finally { $control.Dispose() }
    if (-not $resumeProcess.WaitForExit(10000)) { throw 'MoonBit resume example timed out' }
    $resumed = Get-Content -Raw $resumeOutput
    $resumeStderr = Get-Content -Raw $resumeError
    if ($resumeStderr -or
        $resumed -notmatch 'moonmqtt/interop/resume/one: offline QoS1' -or
        $resumed -notmatch 'moonmqtt/interop/resume/two: offline QoS2') {
      throw "MoonBit resume example got unexpected data: $resumed $resumeStderr"
    }

    foreach ($path in @($processSnapshot, "$processSnapshot.tmp", "$processSnapshot.ready")) {
      if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force }
    }
    $processSeed = Start-Process -FilePath $processResume -WorkingDirectory $projectRoot `
      -ArgumentList @('hold', $processSnapshot) -WindowStyle Hidden -PassThru `
      -RedirectStandardOutput $processSeedOutput -RedirectStandardError $processSeedError
    $seedReady = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
      if ($processSeed.HasExited) { throw "Process seed exited: $(Get-Content -Raw $processSeedError)" }
      if (Test-Path -LiteralPath "$processSnapshot.ready") {
        $seedReady = $true
        break
      }
      Start-Sleep -Milliseconds 100
    }
    if (-not $seedReady) { throw 'Process seed did not persist subscriptions' }
    Stop-Process -Id $processSeed.Id -Force
    if (-not $processSeed.WaitForExit(5000)) { throw 'Process seed did not stop' }
    Start-Sleep -Milliseconds 200
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/process/one -q 1 -m 'restart QoS1'
    if ($LASTEXITCODE -ne 0) { throw 'restart QoS 1 publish failed' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/process/two -q 2 -m 'restart QoS2'
    if ($LASTEXITCODE -ne 0) { throw 'restart QoS 2 publish failed' }
    $restarted = & $processResume resume $processSnapshot
    if ($LASTEXITCODE -ne 0 -or $restarted -notcontains 'SESSION_PRESENT=1' -or
        ($restarted -join "`n") -notmatch 'process/one: restart QoS1' -or
        ($restarted -join "`n") -notmatch 'process/two: restart QoS2') {
      throw "Process restart resume failed: $restarted"
    }

    Stop-Process -Id $brokerProcess.Id -Force
    if (-not $brokerProcess.WaitForExit(5000)) { throw 'Mosquitto did not stop' }
    $brokerProcess = Start-Process -FilePath $brokerExe -ArgumentList @('-p', '1883', '-v') `
      -WindowStyle Hidden -PassThru -RedirectStandardOutput $brokerLog `
      -RedirectStandardError $brokerError
    Start-Sleep -Milliseconds 500
    if ($brokerProcess.HasExited) { throw "Mosquitto restart failed: $(Get-Content -Raw $brokerError)" }
    $processNew = Start-Process -FilePath $processResume -WorkingDirectory $projectRoot `
      -ArgumentList @('new', $processSnapshot) -WindowStyle Hidden -PassThru `
      -RedirectStandardOutput $processNewOutput -RedirectStandardError $processNewError
    $newReady = $false
    for ($attempt = 0; $attempt -lt 100; $attempt++) {
      if ($processNew.HasExited) { throw "Process new-session example exited: $(Get-Content -Raw $processNewError)" }
      $brokerText = Get-Content -Raw $brokerError
      if (([regex]::Matches($brokerText, 'Sending SUBACK to moonmqtt-interop-process-resume')).Count -ge 2) {
        $newReady = $true
        break
      }
      Start-Sleep -Milliseconds 100
    }
    if (-not $newReady) { throw 'Process new-session example did not resubscribe' }
    & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/process/one -q 1 -m 'after broker reset'
    if ($LASTEXITCODE -ne 0) { throw 'post-reset publish failed' }
    if (-not $processNew.WaitForExit(10000)) { throw 'Process new-session example timed out' }
    $newResult = Get-Content -Raw $processNewOutput
    $newError = Get-Content -Raw $processNewError
    if ($newError -or $newResult -notmatch 'SESSION_PRESENT=0' -or
        $newResult -notmatch 'process/one: after broker reset') {
      throw "Process new-session example failed: $newResult $newError"
    }
    Write-Output 'PASS: QoS 0/1/2 both ways, in-process resume, process restart (SP=1), broker reset (SP=0)'
  }
  finally {
    foreach ($childProcess in @($processNew, $processSeed, $resumeProcess, $moonProcess, $cliProcess, $brokerProcess)) {
      if ($null -ne $childProcess -and -not $childProcess.HasExited) {
        Stop-Process -Id $childProcess.Id -Force
      }
    }
  }
}
finally { Pop-Location }
