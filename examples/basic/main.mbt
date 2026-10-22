///|
/// A small consumer of the public library API.
async fn main {
  let config = @client.Config::new("127.0.0.1", "moonmqtt-basic")
  @client.with_client(config, client => {
    let _ = client.subscribe("moonmqtt/example", @packet.QoS2)
    client.publish("moonmqtt/example", b"hello", qos=@packet.QoS2)
    let message = @async.with_timeout(5000, () => client.receive())
    println("\{message.topic}: \{@utf8.decode(message.payload)}")
    client.unsubscribe("moonmqtt/example")
  })
}
