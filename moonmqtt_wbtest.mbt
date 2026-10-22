///|
async test "fragmented CONNACK and coalesced SUBACK/PUBLISH; QoS 1 both ways" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let port = server.addr.port()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let connect = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(connect) is Ok(ConnectPacket(_)))
      conn.write(b"\x20")
      @async.sleep(10)
      conn.write(b"\x02\x00\x00")
      let subscribe = read_packet(conn, 4096)
      let sub_id = match @mqtt.decode_packet(subscribe) {
        Ok(SubscribePacket(packet)) => packet.message_id
        _ => abort("expected SUBSCRIBE")
      }
      assert_eq(sub_id, 1)
      let combined : Array[Byte] = []
      for
        b in encoded(
          @mqtt.encode_suback({
            message_id: sub_id,
            return_codes: [SuccessQoS1],
          }),
        ) {
        combined.push(b)
      }
      for
        b in encoded(
          @mqtt.encode_publish({
            topic: "test/in",
            payload: b"from broker",
            qos: @mqtt.QoS1,
            message_id: Some(77),
            retain: false,
            dup: false,
          }),
        ) {
        combined.push(b)
      }
      for
        b in encoded(
          @mqtt.encode_publish({
            topic: "test/zero",
            payload: b"zero",
            qos: @mqtt.QoS0,
            message_id: None,
            retain: false,
            dup: false,
          }),
        ) {
        combined.push(b)
      }
      conn.write(Bytes::from_array(combined))
      let ack = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(ack) is Ok(PubackPacket(_)))
      let zero_publish = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(zero_publish) is Ok(PublishPacket(_)))
      let publish = read_packet(conn, 4096)
      let pub_id = match @mqtt.decode_packet(publish) {
        Ok(PublishPacket(packet)) => {
          assert_eq(packet.payload, b"from client")
          match packet.message_id {
            Some(id) => id
            None => abort("missing ID")
          }
        }
        _ => abort("expected PUBLISH")
      }
      assert_eq(pub_id, 2)
      conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(pub_id))))
      let unsubscribe = read_packet(conn, 4096)
      let unsub_id = match @mqtt.decode_packet(unsubscribe) {
        Ok(UnsubscribePacket(packet)) => packet.message_id
        _ => abort("expected UNSUBSCRIBE")
      }
      assert_eq(unsub_id, 3)
      conn.write(Bytes::from_array(encoded(@mqtt.encode_unsuback(unsub_id))))
      let disconnect = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(disconnect) is Ok(DisconnectPacket))
    }
    let config = Config::new("127.0.0.1", "fragment-test", port~)
    config.keep_alive_seconds = 0
    config.timeout_ms = 500
    let client = Client::connect(config, group)
    assert_eq(client.subscribe("test/in", @mqtt.QoS1), @mqtt.QoS1)
    let message = @async.with_timeout(500, () => client.receive())
    assert_eq(message.topic, "test/in")
    assert_eq(message.payload, b"from broker")
    let zero = client.receive()
    assert_eq(zero.topic, "test/zero")
    assert_eq(zero.qos, @mqtt.QoS0)
    client.publish("test/zero", b"zero", qos=@mqtt.QoS0)
    client.publish("test/out", b"from client", qos=@mqtt.QoS1)
    client.unsubscribe("test/in")
    client.disconnect()
  }
}

///|
async test "CONNACK rejection is surfaced" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x05")
    }
    let config = Config::new("127.0.0.1", "rejected", port=server.addr.port())
    config.keep_alive_seconds = 0
    let result = try {
      let _ = Client::connect(config, group)
      false
    } catch {
      Rejected(NotAuthorized) => true
      _ => false
    }
    assert_true(result)
  }
}

///|
async test "malformed packet closes pending receive" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      conn.write(b"\xF0\x00")
    }
    let config = Config::new("127.0.0.1", "bad-packet", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let failed = try {
      let _ = client.receive()
      false
    } catch {
      Protocol(_) => true
      _ => false
    }
    assert_true(failed)
    client.close()
  }
}

///|
async test "unexpected disconnect clears pending SUBSCRIBE" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let _ = read_packet(conn, 4096)
    }
    let config = Config::new("127.0.0.1", "close-test", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let failed = try {
      let _ = client.subscribe("test/#", @mqtt.QoS0)
      false
    } catch {
      _ => true
    }
    assert_true(failed)
    client.close()
  }
}

///|
async test "minimal clean connection and disconnect" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg(no_wait=true) <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let packet = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(packet) is Ok(DisconnectPacket))
    }
    let config = Config::new("127.0.0.1", "minimal", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    client.disconnect()
  }
}

///|
async test "heartbeat sends PINGREQ and accepts PINGRESP" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let ping = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(ping) is Ok(PingreqPacket))
      conn.write(b"\xD0\x00")
      let disconnect = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(disconnect) is Ok(DisconnectPacket))
    }
    let config = Config::new("127.0.0.1", "ping-test", port=server.addr.port())
    config.keep_alive_seconds = 1
    config.timeout_ms = 400
    let client = Client::connect(config, group)
    @async.sleep(1100)
    client.disconnect()
  }
}

///|
async test "SUBACK timeout terminates connection and pending operation" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let _ = read_packet(conn, 4096)
      @async.sleep(300)
    }
    let config = Config::new(
      "127.0.0.1",
      "timeout-test",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.timeout_ms = 100
    let client = Client::connect(config, group)
    let timed_out = try {
      let _ = client.subscribe("test/timeout", @mqtt.QoS1)
      false
    } catch {
      TimedOut => true
      _ => false
    }
    assert_true(timed_out)
    client.close()
  }
}

///|
async test "oversized Remaining Length closes connection" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      conn.write(b"\x30\xFF\xFF\xFF\xFF")
    }
    let config = Config::new(
      "127.0.0.1",
      "length-test",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let invalid = try {
      let _ = client.receive()
      false
    } catch {
      Protocol(_) => true
      _ => false
    }
    assert_true(invalid)
    client.close()
  }
}

///|
async test "wrong PUBACK identifier cannot complete QoS 1 publish" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let published = read_packet(conn, 4096)
      let id = match @mqtt.decode_packet(published) {
        Ok(PublishPacket(packet)) => packet.message_id.unwrap()
        _ => abort("expected PUBLISH")
      }
      conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(id + 1))))
      @async.sleep(300)
    }
    let config = Config::new("127.0.0.1", "wrong-ack", port=server.addr.port())
    config.keep_alive_seconds = 0
    config.timeout_ms = 100
    let client = Client::connect(config, group)
    let rejected = try {
      client.publish("test/ack", b"payload", qos=@mqtt.QoS1)
      false
    } catch {
      Protocol(_) => true
      _ => false
    }
    assert_true(rejected)
    client.close()
  }
}

///|
async test "partial packet body times out" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      conn.write(b"\x30\x0A\x00")
      @async.sleep(300)
    }
    let config = Config::new(
      "127.0.0.1",
      "partial-body",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.timeout_ms = 100
    let client = Client::connect(config, group)
    let timed_out = try {
      let _ = client.receive()
      false
    } catch {
      TimedOut => true
      _ => false
    }
    assert_true(timed_out)
    client.close()
  }
}
