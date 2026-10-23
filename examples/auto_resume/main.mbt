///|
/// Stable AutoClient handle survives a broker restart and rebuilds its subscription.
async fn main {
  let config = @client.Config::new("127.0.0.1", "moonmqtt-interop-auto")
  config.clean_session = false
  config.keep_alive_seconds = 1
  config.timeout_ms = 500
  @async.with_task_group(group => {
    let client = @client.AutoClient::start(
      config,
      group,
      policy=@client.ReconnectPolicy::new(
        initial_delay_ms=100,
        max_delay_ms=1000,
      ),
    )
    client.wait_connected()
    let _ = client.subscribe("moonmqtt/interop/auto", @packet.QoS1)
    println("AUTO_READY")
    let message = @async.with_timeout(15000, () => client.receive())
    println("AUTO_RECEIVED=\{message.topic}: \{@utf8.decode(message.payload)}")
    client.stop()
  })
}
