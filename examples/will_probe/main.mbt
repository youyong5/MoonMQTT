///|
/// Run against Mosquitto; the interoperability script controls the connection.
async fn main {
  let args = @argparse.Command(
    "will_probe",
    about="Observe MQTT 3.1.1 Will behavior",
    positionals=[
      PositionArg(
        "mode",
        about="disconnect, stop, close, close0, hold, or auto",
      ),
    ],
  ).parse()
  guard args.values is { "mode": [mode], .. } else {
    fail("usage: will_probe <disconnect|stop|close|close0|hold|auto>")
  }
  let (qos, retain, payload) = match mode {
    "disconnect" => (@packet.QoS0, false, b"D")
    "stop" => (@packet.QoS1, false, b"S")
    "close" => (@packet.QoS2, true, Bytes::from_array([0x00, 0xff, 0x41]))
    "close0" => (@packet.QoS0, false, b"Z")
    "hold" => (@packet.QoS1, false, b"K")
    "auto" => (@packet.QoS2, false, b"A")
    _ => fail("unknown mode")
  }
  let port = if mode == "auto" { 18885 } else { 1883 }
  let config = @client.Config::new("127.0.0.1", "moonmqtt-will-\{mode}", port~)
  config.keep_alive_seconds = 1
  config.timeout_ms = 1000
  config.will = Some(
    @client.Will::new("moonmqtt/interop/will/\{mode}", payload, qos~, retain~),
  )
  if mode == "auto" || mode == "stop" {
    config.clean_session = mode != "auto"
    @async.with_task_group(group => {
      let client = @client.AutoClient::start(
        config,
        group,
        policy=@client.ReconnectPolicy::new(
          initial_delay_ms=100,
          max_delay_ms=500,
        ),
      )
      client.wait_connected()
      println("READY")
      if mode == "auto" {
        @async.with_timeout(10000, () => {
          while !(client.status() is @client.Connected(true)) {
            @async.sleep(10)
          }
        })
        println("AUTO_RECONNECTED")
      }
      if mode == "auto" || mode == "stop" {
        let uncertain = try {
          client.stop()
          false
        } catch {
          @client.DisconnectUncertain => true
          _ => false
        }
        if uncertain {
          println("WILL_UNCERTAIN")
        }
      } else {
        client.stop()
      }
    })
  } else {
    @async.with_task_group(group => {
      let client = @client.Client::connect(config, group)
      println("READY")
      match mode {
        "disconnect" => client.disconnect()
        "close" | "close0" => client.close()
        "hold" => @async.sleep(60000)
        _ => ()
      }
    })
  }
}
