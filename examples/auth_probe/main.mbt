///|
/// Probe the public authentication API against a password-protected broker.
async fn main {
  let args = @argparse.Command("auth_probe", positionals=[
    PositionArg("mode", about="good, bad, or missing"),
    PositionArg("username", about="ephemeral test username"),
    PositionArg("password", about="ephemeral test password"),
  ]).parse()
  guard args.values
    is { "mode": [mode], "username": [username], "password": [password], .. } else {
    fail("usage: auth_probe <good|bad|missing> <username> <password>")
  }
  let config = @client.Config::new(
    "127.0.0.1",
    "moonmqtt-auth-\{mode}",
    port=18891,
  )
  config.timeout_ms = 2000
  match mode {
    "good" | "bad" => {
      config.username = Some(username)
      config.password = Some(@utf8.encode(password))
    }
    "missing" => ()
    _ => fail("unknown authentication mode")
  }
  if mode == "good" {
    @async.with_task_group(group => {
      let client = @client.Client::connect(config, group)
      let _ = client.subscribe("moonmqtt/interop/auth", @packet.QoS1)
      client.publish("moonmqtt/interop/auth", b"auth-ok", qos=@packet.QoS1)
      let message = @async.with_timeout(5000, () => client.receive())
      if message.topic != "moonmqtt/interop/auth" ||
        @utf8.decode(message.payload) != "auth-ok" {
        fail("authenticated QoS 1 roundtrip returned unexpected message")
      }
      client.disconnect()
      println("AUTH_QOS1_OK")
    })
  } else {
    @async.with_task_group(group => {
      let rejection = try {
        let client = @client.Client::connect(config, group)
        client.disconnect()
        None
      } catch {
        @client.Rejected(@packet.NotAuthorized) => Some("NotAuthorized")
        @client.Rejected(@packet.BadUserNameOrPassword) =>
          Some("BadUserNameOrPassword")
        _ => None
      }
      guard rejection is Some(reason) else {
        fail("expected explicit MQTT authentication rejection")
      }
      println("AUTH_REJECTED=\{reason}")
    })
  }
}
