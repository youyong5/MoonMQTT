param(
  [string]$MosquittoDirectory = 'C:\Program Files\mosquitto'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$brokerExe = Join-Path $MosquittoDirectory 'mosquitto.exe'
$passwdExe = Join-Path $MosquittoDirectory 'mosquitto_passwd.exe'
$probeExe = Join-Path $root '_build\native\debug\build\examples\auth_probe\auth_probe.exe'
foreach ($exe in @($brokerExe, $passwdExe, $probeExe)) {
  if (-not (Test-Path -LiteralPath $exe)) { throw "Missing executable: $exe" }
}

# Keep credentials, the password database and broker logs outside the checkout
# so the workflow's diagnostics artifact cannot collect them.
$tempRoot = [IO.Path]::GetFullPath($env:TEMP).TrimEnd('\') + '\'
$workDir = [IO.Path]::GetFullPath(
  (Join-Path $tempRoot ('moonmqtt-auth-' + [guid]::NewGuid().ToString('N')))
)
if (-not $workDir.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
  throw 'Authentication test directory escaped the temporary root'
}
New-Item -ItemType Directory -Path $workDir | Out-Null
$passwordFile = Join-Path $workDir 'password.db'
$configFile = Join-Path $workDir 'mosquitto.conf'
$brokerOut = Join-Path $workDir 'broker.out.log'
$brokerError = Join-Path $workDir 'broker.err.log'
$username = 'auth-' + [guid]::NewGuid().ToString('N')
$password = [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')
$wrongPassword = [guid]::NewGuid().ToString('N')
$broker = $null
$probe = $null
$rejections = @{}

try {
  & $passwdExe -b -c $passwordFile $username $password
  if ($LASTEXITCODE -ne 0) { throw 'mosquitto_passwd failed' }
  @"
listener 18891 127.0.0.1
allow_anonymous false
password_file $($passwordFile.Replace('\', '/'))
"@ | Set-Content -LiteralPath $configFile -Encoding Ascii

  $broker = Start-Process -FilePath $brokerExe -ArgumentList @('-c', $configFile, '-v') `
    -WindowStyle Hidden -PassThru -RedirectStandardOutput $brokerOut `
    -RedirectStandardError $brokerError
  $ready = $false
  for ($attempt = 0; $attempt -lt 100; $attempt++) {
    if ($broker.HasExited) { throw 'Authentication broker exited before listening' }
    if ((Test-Path -LiteralPath $brokerError) -and
        (Get-Content -LiteralPath $brokerError -Raw) -match
          'Opening ipv4 listen socket on port 18891') {
      $ready = $true
      break
    }
    Start-Sleep -Milliseconds 100
  }
  if (-not $ready) { throw 'Authentication listener did not become ready' }

  foreach ($mode in @('good', 'bad', 'missing')) {
    $candidate = if ($mode -eq 'bad') { $wrongPassword } else { $password }
    $start = [Diagnostics.ProcessStartInfo]::new(
      $probeExe, "$mode $username $candidate"
    )
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $probe = [Diagnostics.Process]::new()
    $probe.StartInfo = $start
    if (-not $probe.Start()) { throw "Authentication $mode probe did not start" }
    $stdout = $probe.StandardOutput.ReadToEndAsync()
    $stderr = $probe.StandardError.ReadToEndAsync()
    if (-not $probe.WaitForExit(10000)) {
      $probe.Kill()
      throw "Authentication $mode probe timed out"
    }
    $result = $stdout.GetAwaiter().GetResult()
    $errorText = $stderr.GetAwaiter().GetResult()
    if ($probe.ExitCode -ne 0 -or $errorText.Trim()) {
      throw "Authentication $mode probe failed (exit $($probe.ExitCode)): $result $errorText"
    }
    if ($mode -eq 'good') {
      if ($result -notmatch '(?m)^AUTH_QOS1_OK\r?$') {
        throw "Authenticated QoS 1 roundtrip missing: $result"
      }
    } else {
      if ($result -notmatch '(?m)^AUTH_REJECTED=(NotAuthorized|BadUserNameOrPassword)\r?$') {
        throw "Authentication $mode did not return a MQTT rejection: $result"
      }
      $rejections[$mode] = $Matches[1]
    }
    $probe.Dispose()
    $probe = $null
  }
  Write-Output "PASS: authenticated QoS 1 roundtrip; bad=$($rejections.bad); missing=$($rejections.missing)"
}
finally {
  if ($null -ne $probe) {
    if (-not $probe.HasExited) {
      $probe.Kill()
      $probe.WaitForExit(5000) | Out-Null
    }
    $probe.Dispose()
  }
  if ($null -ne $broker -and -not $broker.HasExited) {
    Stop-Process -Id $broker.Id -Force
    $broker.WaitForExit(5000) | Out-Null
  }
  if (Test-Path -LiteralPath $workDir) {
    Remove-Item -LiteralPath $workDir -Recurse -Force
  }
}
