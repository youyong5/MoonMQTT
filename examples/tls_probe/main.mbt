///|
/// Local Mosquitto TLS demonstration and interoperability probe.
async fn main {
  let args = @argparse.Command(
    "tls_probe",
    about="MQTT 3.1.1 over verified native TLS",
    positionals=[
      PositionArg(
        "mode",
        about="roundtrip, badca, badhost, baddefault, seed, resume, auto, badca_auto",
      ),
      PositionArg("ca", about="trusted PEM CA file"),
      PositionArg("snapshot", about="session file or unused placeholder"),
    ],
  ).parse()
  guard args.values
    is { "mode": [mode], "ca": [ca], "snapshot": [snapshot], .. } else {
    fail("usage: tls_probe <mode> <ca.pem> <snapshot-path>")
  }
  let client_id = if mode == "seed" || mode == "resume" {
    "moonmqtt-tls-resume"
  } else if mode == "auto" {
    "moonmqtt-tls-auto"
  } else {
    "moonmqtt-tls-\{mode}"
  }
  let host = if mode == "badhost" { "127.0.0.1" } else { "localhost" }
  let port = if mode == "auto" { 18887 } else { 18886 }
  let config = @client.Config::new(host, client_id, port~)
  config.keep_alive_seconds = 1
  config.timeout_ms = 1500
  config.tls = Some(@client.TlsConfig::new(ca_file=ca))
  if mode == "baddefault" {
    config.tls = Some(@client.TlsConfig::new())
  }
  if mode == "seed" || mode == "resume" {
    config.clean_session = false
    config.session_store = Some(@client.SessionStore::file(snapshot))
  }
  if mode == "auto" {
    config.clean_session = false
    @async.with_task_group(group => {
      let auto = @client.AutoClient::start(
        config,
        group,
        policy=@client.ReconnectPolicy::new(
          initial_delay_ms=100,
          max_delay_ms=500,
        ),
      )
      auto.wait_connected()
      let _ = auto.subscribe("moonmqtt/tls/auto", @packet.QoS1)
      println("AUTO_READY")
      @async.with_timeout(10000, () => {
        while !(auto.status() is @client.Connected(true)) {
          @async.sleep(10)
        }
      })
      println("AUTO_RECONNECTED")
      let message = @async.with_timeout(5000, () => auto.receive())
      println("AUTO_RECEIVED=\{@utf8.decode(message.payload)}")
      auto.stop()
    })
  } else if mode == "badca_auto" {
    @async.with_task_group(group => {
      let auto = @client.AutoClient::start(config, group)
      @async.with_timeout(5000, () => {
        while !(auto.status() is @client.Failed) {
          @async.sleep(10)
        }
      })
      guard auto.last_error() is Some(@client.TlsValidation(_)) else {
        fail("expected permanent TLS validation failure")
      }
      println("TLS_PERMANENT_FAILURE")
      auto.stop()
    })
  } else if mode == "badca" || mode == "badhost" || mode == "baddefault" {
    @async.with_task_group(group => {
      let rejected = try {
        let client = @client.Client::connect(config, group)
        client.disconnect()
        false
      } catch {
        @client.TlsValidation(_) => true
        _ => false
      }
      if !rejected {
        fail("expected TLS certificate validation failure")
      }
      println("TLS_REJECTED")
    })
  } else {
    @async.with_task_group(group => {
      let client = @client.Client::connect(config, group)
      match mode {
        "roundtrip" => {
          let _ = client.subscribe("moonmqtt/tls/to-client/#", @packet.QoS2)
          for qos in [@packet.QoS0, @packet.QoS1, @packet.QoS2] {
            let label = match qos {
              @packet.QoS0 => "0"
              @packet.QoS1 => "1"
              @packet.QoS2 => "2"
            }
            client.publish(
              "moonmqtt/tls/from-client/\{label}",
              b"moon-\{label}",
              qos~,
            )
          }
          println("ROUNDTRIP_READY")
          for _ in 0..<3 {
            let message = @async.with_timeout(5000, () => client.receive())
            println(
              "RECEIVED=\{message.topic}:\{@utf8.decode(message.payload)}",
            )
          }
        }
        "seed" => {
          if client.session_present() {
            fail("seed expected SP=0")
          }
          let _ = client.subscribe("moonmqtt/tls/resume", @packet.QoS1)
          println("SEED_SP0")
        }
        "resume" => {
          if !client.session_present() {
            fail("resume expected SP=1")
          }
          println("RESUME_SP1")
          let message = @async.with_timeout(5000, () => client.receive())
          println("RESUME_RECEIVED=\{@utf8.decode(message.payload)}")
        }
        _ => fail("unknown mode")
      }
      client.disconnect()
    })
  }
}
