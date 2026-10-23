param(
  [string]$MosquittoDirectory = 'C:\Program Files\mosquitto'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$brokerExe = Join-Path $MosquittoDirectory 'mosquitto.exe'
$subscriberExe = Join-Path $MosquittoDirectory 'mosquitto_sub.exe'
$publisherExe = Join-Path $MosquittoDirectory 'mosquitto_pub.exe'
$probeExe = Join-Path $projectRoot '_build\native\debug\build\examples\will_probe\will_probe.exe'
foreach ($exe in @($brokerExe, $subscriberExe, $publisherExe, $probeExe)) {
  if (-not (Test-Path -LiteralPath $exe)) { throw "Missing executable: $exe" }
}

$artifactDir = Join-Path $projectRoot '.tools\interop'
New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
$brokerError = Join-Path $artifactDir 'will-broker.err.log'
$brokerOutput = Join-Path $artifactDir 'will-broker.out.log'
$subscriberOutput = Join-Path $artifactDir 'will-subscriber.txt'
$subscriberError = Join-Path $artifactDir 'will-subscriber.err.log'
$holdOutput = Join-Path $artifactDir 'will-hold.txt'
$holdError = Join-Path $artifactDir 'will-hold.err.log'
$autoOutput = Join-Path $artifactDir 'will-auto.txt'
$autoError = Join-Path $artifactDir 'will-auto.err.log'
$broker = $null
$subscriber = $null
$hold = $null
$auto = $null
$bridge = $null

function Wait-Text {
  param([string]$Path, [string]$Pattern, [string]$What)
  for ($attempt = 0; $attempt -lt 100; $attempt++) {
    if ((Test-Path -LiteralPath $Path) -and (Get-Content -LiteralPath $Path -Raw) -match $Pattern) { return }
    Start-Sleep -Milliseconds 100
  }
  throw "Timed out waiting for $What in $Path"
}

Push-Location $projectRoot
try {
  $broker = Start-Process -FilePath $brokerExe -ArgumentList @('-p', '1883', '-v') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $brokerOutput `
    -RedirectStandardError $brokerError
  Start-Sleep -Milliseconds 500
  if ($broker.HasExited) { throw "Mosquitto exited: $(Get-Content -Raw $brokerError)" }

  # Clear a retained value left by an earlier failed run.
  & $publisherExe -h 127.0.0.1 -p 1883 -t moonmqtt/interop/will/close -r -n
  if ($LASTEXITCODE -ne 0) { throw 'Failed to clear retained Will test topic' }

  $subscriber = Start-Process -FilePath $subscriberExe `
    -ArgumentList @('-h', '127.0.0.1', '-p', '1883', '-V', 'mqttv311',
      '-i', 'moonmqtt-will-watcher', '-t', 'moonmqtt/interop/will/#',
      '-q', '2', '-C', '4', '-F', '%t|%q|%r|%x') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $subscriberOutput `
    -RedirectStandardError $subscriberError
  Wait-Text $brokerError 'Sending SUBACK to moonmqtt-will-watcher' 'Will subscriber'

  foreach ($mode in @('disconnect', 'stop')) {
    $result = & $probeExe $mode
    if ($LASTEXITCODE -ne 0 -or $result -notcontains 'READY') {
      throw "Normal $mode failed: $result"
    }
  }
  Start-Sleep -Milliseconds 300
  if ((Get-Content -Raw $subscriberOutput) -match 'will/(disconnect|stop)') {
    throw 'Normal DISCONNECT or stop published a Will'
  }

  $closed = & $probeExe close
  if ($LASTEXITCODE -ne 0 -or $closed -notcontains 'READY') { throw "Direct close failed: $closed" }
  $closed0 = & $probeExe close0
  if ($LASTEXITCODE -ne 0 -or $closed0 -notcontains 'READY') { throw "QoS 0 direct close failed: $closed0" }
  $retained = & $subscriberExe -h 127.0.0.1 -p 1883 -V mqttv311 `
    -t moonmqtt/interop/will/close -q 2 -C 1 -W 3 -F '%t|%q|%r|%x'
  if ($LASTEXITCODE -ne 0 -or
      $retained -notcontains 'moonmqtt/interop/will/close|2|1|00ff41') {
    throw "Retained QoS 2 binary Will missing: $retained"
  }

  $hold = Start-Process -FilePath $probeExe -ArgumentList @('hold') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $holdOutput `
    -RedirectStandardError $holdError
  Wait-Text $brokerError ' as moonmqtt-will-hold ' 'held Will client'
  Stop-Process -Id $hold.Id -Force
  if (-not $hold.WaitForExit(5000)) { throw 'Forced client did not exit' }

  Add-Type -Path (Join-Path $PSScriptRoot 'WillBridge.cs')
  $bridge = [WillBridge]::new(18885, '127.0.0.1', 1883)
  $bridge.Start()
  $auto = Start-Process -FilePath $probeExe -ArgumentList @('auto') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $autoOutput `
    -RedirectStandardError $autoError
  Wait-Text $brokerError 'Sending CONNACK to moonmqtt-will-auto' 'automatic Will connection'
  Start-Sleep -Milliseconds 200
  $bridge.Drop()
  if (-not $auto.WaitForExit(15000)) { throw 'Automatic client did not reconnect' }
  $autoResult = Get-Content -Raw $autoOutput
  $autoErrorText = Get-Content -Raw $autoError
  if ($autoResult -notmatch 'AUTO_RECONNECTED' -or
      $autoResult -notmatch 'WILL_UNCERTAIN' -or $autoErrorText) {
    throw "Automatic Will reconnect failed: $autoResult $autoErrorText"
  }
  Wait-Text $brokerError 'Received DISCONNECT from moonmqtt-will-auto' 'clean automatic stop'

  if (-not $subscriber.WaitForExit(10000)) { throw 'Will subscriber did not receive four Wills' }
  $messages = @(Get-Content -LiteralPath $subscriberOutput)
  $expected = @(
    'moonmqtt/interop/will/close|2|0|00ff41',
    'moonmqtt/interop/will/close0|0|0|5a',
    'moonmqtt/interop/will/hold|1|0|4b',
    'moonmqtt/interop/will/auto|2|0|41'
  )
  if ($messages.Count -ne 4) { throw "Unexpected Will count: $($messages -join '; ')" }
  foreach ($line in $expected) {
    if ($messages -notcontains $line) { throw "Missing Will '$line': $($messages -join '; ')" }
  }
  Write-Output 'PASS: Will QoS 0/1/2 CONNECT, clean disconnect/stop, close, forced exit, retained binary payload, auto reconnect'
}
finally {
  if ($null -ne $bridge) { $bridge.Dispose() }
  foreach ($child in @($auto, $hold, $subscriber, $broker)) {
    if ($null -ne $child -and -not $child.HasExited) { Stop-Process -Id $child.Id -Force }
  }
  Pop-Location
}
