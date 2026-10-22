///|
async fn main {
  let config = @client.Config::new("127.0.0.1", "moonmqtt-interop-subscriber")
  @client.with_client(config, client => {
    let _ = client.subscribe("moonmqtt/interop/in0", @packet.QoS0)
    let _ = client.subscribe("moonmqtt/interop/in", @packet.QoS1)
    let _ = client.subscribe("moonmqtt/interop/in2", @packet.QoS2)
    for _ in 0..<3 {
      let message = @async.with_timeout(10000, () => client.receive())
      println("\{message.topic}: \{@utf8.decode(message.payload)}")
    }
    client.unsubscribe("moonmqtt/interop/in0")
    client.unsubscribe("moonmqtt/interop/in")
    client.unsubscribe("moonmqtt/interop/in2")
  })
}
