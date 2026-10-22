///|
async fn main {
  let config = @client.Config::new("127.0.0.1", "moonmqtt-interop-publisher")
  @client.with_client(config, client => {
    client.publish("moonmqtt/interop/out0", b"from MoonBit QoS0")
    client.publish("moonmqtt/interop/out", b"from MoonBit", qos=@packet.QoS1)
    client.publish(
      "moonmqtt/interop/out2",
      b"from MoonBit QoS2",
      qos=@packet.QoS2,
    )
  })
}
