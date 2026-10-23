param(
  [string]$MosquittoDirectory = 'C:\Program Files\mosquitto',
  [string]$OpenSslFile = ''
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$brokerExe = Join-Path $MosquittoDirectory 'mosquitto.exe'
$pubExe = Join-Path $MosquittoDirectory 'mosquitto_pub.exe'
$subExe = Join-Path $MosquittoDirectory 'mosquitto_sub.exe'
$probeExe = Join-Path $root '_build\native\debug\build\examples\tls_probe\tls_probe.exe'
if (-not $OpenSslFile) {
  $gitCmd = (Get-Command git -ErrorAction Stop).Source
  $OpenSslFile = Join-Path (Split-Path (Split-Path $gitCmd -Parent) -Parent) 'usr\bin\openssl.exe'
}
foreach ($exe in @($brokerExe, $pubExe, $subExe, $probeExe, $OpenSslFile)) {
  if (-not (Test-Path -LiteralPath $exe)) { throw "Missing executable: $exe" }
}

$dir = Join-Path $root '.tools\interop\tls'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$ca = Join-Path $dir 'ca.pem'
$caKey = Join-Path $dir 'ca.key'
$wrongCa = Join-Path $dir 'wrong-ca.pem'
$wrongKey = Join-Path $dir 'wrong-ca.key'
$serverKey = Join-Path $dir 'server.key'
$serverCsr = Join-Path $dir 'server.csr'
$serverCert = Join-Path $dir 'server.pem'
$extFile = Join-Path $dir 'server.ext'
$brokerConfig = Join-Path $dir 'mosquitto.conf'
$brokerOut = Join-Path $dir 'broker.out.log'
$brokerLog = Join-Path $dir 'broker.err.log'
$subscriberOut = Join-Path $dir 'subscriber.txt'
$subscriberErr = Join-Path $dir 'subscriber.err.log'
$roundtripOut = Join-Path $dir 'roundtrip.txt'
$roundtripErr = Join-Path $dir 'roundtrip.err.log'
$resumeOut = Join-Path $dir 'resume.txt'
$resumeErr = Join-Path $dir 'resume.err.log'
$autoOut = Join-Path $dir 'auto.txt'
$autoErr = Join-Path $dir 'auto.err.log'
$snapshot = Join-Path $dir 'session.bin'

function Check-Exit { if ($LASTEXITCODE -ne 0) { throw "Command exited $LASTEXITCODE" } }
function Wait-Text {
  param([string]$Path, [string]$Pattern, [string]$What)
  for ($attempt = 0; $attempt -lt 100; $attempt++) {
    if ((Test-Path -LiteralPath $Path) -and
        (Get-Content -LiteralPath $Path -Raw) -match $Pattern) { return }
    Start-Sleep -Milliseconds 100
  }
  throw "Timed out waiting for $What in $Path"
}

$broker = $null
$subscriber = $null
$roundtrip = $null
$resume = $null
$auto = $null
$bridge = $null
Push-Location $root
try {
  # OpenSSL writes key generation progress to stderr even on success.
  $ErrorActionPreference = 'Continue'
  & $OpenSslFile req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 `
    -subj '/CN=MoonMQTT Test CA' -addext 'basicConstraints=critical,CA:TRUE' `
    -keyout $caKey -out $ca 2>$null
  Check-Exit
  & $OpenSslFile req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 `
    -subj '/CN=Wrong MoonMQTT CA' -addext 'basicConstraints=critical,CA:TRUE' `
    -keyout $wrongKey -out $wrongCa 2>$null
  Check-Exit
  & $OpenSslFile req -newkey rsa:2048 -nodes -sha256 `
    -subj '/CN=localhost' -keyout $serverKey -out $serverCsr 2>$null
  Check-Exit
  @'
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
'@ | Set-Content -LiteralPath $extFile -Encoding Ascii
  & $OpenSslFile x509 -req -in $serverCsr -CA $ca -CAkey $caKey `
    -CAcreateserial -out $serverCert -days 2 -sha256 -extfile $extFile 2>$null
  Check-Exit
  $ErrorActionPreference = 'Stop'
  @"
listener 18886 127.0.0.1
allow_anonymous true
certfile $($serverCert.Replace('\', '/'))
keyfile $($serverKey.Replace('\', '/'))
"@ | Set-Content -LiteralPath $brokerConfig -Encoding Ascii
  if (Test-Path -LiteralPath $snapshot) { Remove-Item -LiteralPath $snapshot }
  $broker = Start-Process -FilePath $brokerExe -ArgumentList @('-c', $brokerConfig, '-v') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $brokerOut `
    -RedirectStandardError $brokerLog
  Wait-Text $brokerLog 'Opening ipv4 listen socket on port 18886' 'TLS listener'
  if ($broker.HasExited) { throw "TLS broker exited: $(Get-Content -Raw $brokerLog)" }

  foreach ($mode in @('badca', 'badhost', 'baddefault')) {
    $trust = if ($mode -eq 'badca') { $wrongCa } else { $ca }
    $result = & $probeExe $mode $trust $snapshot
    if ($LASTEXITCODE -ne 0 -or $result -notcontains 'TLS_REJECTED') {
      throw "Expected $mode TLS rejection: $result"
    }
  }
  $result = & $probeExe badca_auto $wrongCa $snapshot
  if ($LASTEXITCODE -ne 0 -or $result -notcontains 'TLS_PERMANENT_FAILURE') {
    throw "Automatic TLS validation did not fail permanently: $result"
  }
  if ((Get-Content -Raw $brokerLog) -match 'New client connected .*moonmqtt-tls-bad') {
    throw 'MQTT CONNECT reached broker before certificate validation'
  }

  $subscriber = Start-Process -FilePath $subExe -ArgumentList @(
    '-h', 'localhost', '-p', '18886', '--cafile', $ca,
    '-V', 'mqttv311', '-t', 'moonmqtt/tls/from-client/#', '-q', '2',
    '-C', '3', '-F', '%t|%q|%x'
  ) -WindowStyle Hidden -PassThru -RedirectStandardOutput $subscriberOut `
    -RedirectStandardError $subscriberErr
  Wait-Text $brokerLog 'Received SUBSCRIBE from auto-' 'TLS CLI subscriber'
  $roundtrip = Start-Process -FilePath $probeExe -ArgumentList @('roundtrip', $ca, $snapshot) `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $roundtripOut `
    -RedirectStandardError $roundtripErr
  Wait-Text $brokerLog 'Sending SUBACK to moonmqtt-tls-roundtrip' 'TLS MoonBit subscription'
  for ($q = 0; $q -le 2; $q++) {
    & $pubExe -h localhost -p 18886 --cafile $ca -V mqttv311 `
      -t "moonmqtt/tls/to-client/$q" -q $q -m "cli-$q"
    Check-Exit
  }
  if (-not $roundtrip.WaitForExit(10000)) { throw 'TLS roundtrip timed out' }
  $roundtripText = Get-Content -Raw $roundtripOut
  if ((Get-Content -Raw $roundtripErr) -or $roundtripText -notmatch 'ROUNDTRIP_READY') {
    throw "TLS roundtrip failed: $roundtripText $(Get-Content -Raw $roundtripErr)"
  }
  for ($q = 0; $q -le 2; $q++) {
    if ($roundtripText -notmatch "RECEIVED=moonmqtt/tls/to-client/$q`:cli-$q") {
      throw "MoonBit missed TLS QoS $q publication: $roundtripText"
    }
  }
  if (-not $subscriber.WaitForExit(10000)) { throw 'TLS CLI subscriber timed out' }
  $messages = @(Get-Content -LiteralPath $subscriberOut)
  if ($messages.Count -ne 3) { throw "TLS CLI received $($messages.Count) messages" }
  for ($q = 0; $q -le 2; $q++) {
    $hex = [BitConverter]::ToString([Text.Encoding]::ASCII.GetBytes("moon-$q")).Replace('-', '').ToLowerInvariant()
    if ($messages -notcontains "moonmqtt/tls/from-client/$q|$q|$hex") {
      throw "CLI missed TLS QoS $q publication: $($messages -join '; ')"
    }
  }

  $seed = & $probeExe seed $ca $snapshot
  if ($LASTEXITCODE -ne 0 -or $seed -notcontains 'SEED_SP0') { throw "TLS seed failed: $seed" }
  $resume = Start-Process -FilePath $probeExe -ArgumentList @('resume', $ca, $snapshot) `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $resumeOut `
    -RedirectStandardError $resumeErr
  Wait-Text $brokerLog 'Sending CONNACK to moonmqtt-tls-resume \(1, 0\)' 'TLS SP1 recovery'
  & $pubExe -h localhost -p 18886 --cafile $ca -V mqttv311 `
    -t moonmqtt/tls/resume -q 1 -m 'tls-resumed'
  Check-Exit
  if (-not $resume.WaitForExit(10000)) { throw 'TLS resume timed out' }
  $resumeText = Get-Content -Raw $resumeOut
  if ((Get-Content -Raw $resumeErr) -or $resumeText -notmatch 'RESUME_SP1' -or
      $resumeText -notmatch 'RESUME_RECEIVED=tls-resumed') {
    throw "TLS persistent resume failed: $resumeText $(Get-Content -Raw $resumeErr)"
  }

  Add-Type -Path (Join-Path $PSScriptRoot 'WillBridge.cs')
  $bridge = [WillBridge]::new(18887, '127.0.0.1', 18886)
  $bridge.Start()
  $auto = Start-Process -FilePath $probeExe -ArgumentList @('auto', $ca, $snapshot) `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $autoOut `
    -RedirectStandardError $autoErr
  Wait-Text $brokerLog 'Sending SUBACK to moonmqtt-tls-auto' 'TLS automatic subscription'
  Start-Sleep -Milliseconds 200
  $bridge.Drop()
  Wait-Text $brokerLog 'Sending CONNACK to moonmqtt-tls-auto \(1, 0\)' 'TLS automatic SP1 reconnect'
  & $pubExe -h localhost -p 18886 --cafile $ca -V mqttv311 `
    -t moonmqtt/tls/auto -q 1 -m 'tls-auto-resumed'
  Check-Exit
  if (-not $auto.WaitForExit(10000)) { throw 'TLS automatic client timed out' }
  $autoText = Get-Content -Raw $autoOut
  if ((Get-Content -Raw $autoErr) -or $autoText -notmatch 'AUTO_RECONNECTED' -or
      $autoText -notmatch 'AUTO_RECEIVED=tls-auto-resumed') {
    throw "TLS automatic reconnect failed: $autoText $(Get-Content -Raw $autoErr)"
  }
  Write-Output 'PASS: TLS default/custom CA and hostname rejection, QoS 0/1/2 both ways, SP1 process resume, automatic TLS reconnect'
}
finally {
  if ($null -ne $bridge) { $bridge.Dispose() }
  foreach ($child in @($auto, $resume, $roundtrip, $subscriber, $broker)) {
    if ($null -ne $child -and -not $child.HasExited) { Stop-Process -Id $child.Id -Force }
  }
  Pop-Location
}
