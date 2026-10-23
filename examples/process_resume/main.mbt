///|
/// Run each mode as a separate native process against the same snapshot path.
async fn main {
  let args = @argparse.Command(
    "process_resume",
    about="Demonstrate durable MQTT 3.1.1 session recovery",
    positionals=[
      PositionArg("mode", about="seed, hold, resume, or new"),
      PositionArg("snapshot", about="path to the session snapshot"),
    ],
  ).parse()
  guard args.values is { "mode": [mode], "snapshot": [snapshot], .. } else {
    fail("usage: process_resume <seed|hold|resume|new> <snapshot>")
  }
  let config = @client.Config::new(
    "127.0.0.1", "moonmqtt-interop-process-resume",
  )
  config.clean_session = false
  config.session_store = Some(@client.SessionStore::file(snapshot))
  config.keep_alive_seconds = 0
  @async.with_task_group(group => {
    let client = @client.Client::connect(config, group)
    match mode {
      "seed" => {
        if client.session_present() {
          fail("seed expected a new broker session")
        }
        let _ = client.subscribe("moonmqtt/interop/process/one", @packet.QoS1)
        let _ = client.subscribe("moonmqtt/interop/process/two", @packet.QoS2)
        println("SESSION_PRESENT=0")
      }
      "hold" => {
        if client.session_present() {
          fail("hold expected a new broker session")
        }
        let _ = client.subscribe("moonmqtt/interop/process/one", @packet.QoS1)
        let _ = client.subscribe("moonmqtt/interop/process/two", @packet.QoS2)
        @fs.write_file(snapshot + ".ready", b"ready")
        @async.sleep(60000)
      }
      "resume" => {
        if !client.session_present() {
          fail("resume expected a broker session")
        }
        println("SESSION_PRESENT=1")
        for _ in 0..<2 {
          let message = @async.with_timeout(5000, () => client.receive())
          println("\{message.topic}: \{@utf8.decode(message.payload)}")
        }
      }
      "new" => {
        if client.session_present() {
          fail("new expected broker session loss")
        }
        println("SESSION_PRESENT=0")
        let message = @async.with_timeout(5000, () => client.receive())
        println("\{message.topic}: \{@utf8.decode(message.payload)}")
      }
      _ => fail("mode must be seed, hold, resume, or new")
    }
    client.disconnect()
  })
}
