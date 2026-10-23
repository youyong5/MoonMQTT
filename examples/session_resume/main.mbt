///|
/// One process spans both TCP connections; the control socket lets the
/// interoperability script publish while this MQTT session is offline.
async fn main {
  let config = @client.Config::new("127.0.0.1", "moonmqtt-interop-resume")
  config.clean_session = false
  config.keep_alive_seconds = 0
  @async.with_task_group(group => {
    let first = @client.Client::connect(config, group)
    if first.session_present() {
      abort("unexpected previous broker session")
    }
    let _ = first.subscribe("moonmqtt/interop/resume/one", @packet.QoS1)
    let _ = first.subscribe("moonmqtt/interop/resume/two", @packet.QoS2)
    first.disconnect()

    let control = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:18884"))
    defer control.close()
    println("OFFLINE:\{control.addr.port()}")
    let (gate, _) = control.accept()
    let _ = gate.read_exactly(1)
    gate.close()

    let second = first.reconnect(group)
    if !second.session_present() {
      abort("Mosquitto did not resume session")
    }
    for _ in 0..<2 {
      let message = @async.with_timeout(5000, () => second.receive())
      println("\{message.topic}: \{@utf8.decode(message.payload)}")
    }
    second.disconnect()
  })
}
