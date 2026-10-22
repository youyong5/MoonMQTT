///|
async test "outgoing QoS 2 keeps ID until PUBCOMP and repeats PUBREL only" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let first = read_packet(conn, 4096)
      let id = match @mqtt.decode_packet(first) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.qos, @mqtt.QoS2)
          p.message_id.unwrap()
        }
        _ => abort("expected QoS 2 PUBLISH")
      }
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      let release = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(release) is Ok(PubrelPacket(_)))
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      let repeated = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(repeated) is Ok(PubrelPacket(_)))
      let second = read_packet(conn, 4096)
      let second_id = match @mqtt.decode_packet(second) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected second PUBLISH")
      }
      assert_true(id != second_id)
      conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(second_id))))
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      let end = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(end) is Ok(DisconnectPacket))
    }
    let config = Config::new("127.0.0.1", "qos2-out", port=server.addr.port())
    config.keep_alive_seconds = 0
    config.timeout_ms = 1000
    let client = Client::connect(config, group)
    let task = group.spawn(no_wait=true, allow_failure=true) <| () => {
      client.publish("test/two", b"first", qos=@mqtt.QoS2)
    }
    @async.sleep(30)
    client.publish("test/one", b"second", qos=@mqtt.QoS1)
    task.wait()
    client.disconnect()
  }
}

///|
async test "incoming QoS 2 duplicate PUBLISH and PUBREL deliver once" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let first = encoded(
        @mqtt.encode_publish({
          topic: "test/in",
          payload: b"one",
          qos: @mqtt.QoS2,
          message_id: Some(77),
          retain: false,
          dup: false,
        }),
      )
      conn.write(Bytes::from_array(first))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubrecPacket(_)),
      )
      let duplicate = encoded(
        @mqtt.encode_publish({
          topic: "test/in",
          payload: b"one",
          qos: @mqtt.QoS2,
          message_id: Some(77),
          retain: false,
          dup: true,
        }),
      )
      conn.write(Bytes::from_array(duplicate))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubrecPacket(_)),
      )
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubcompPacket(_)),
      )
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubcompPacket(_)),
      )
      let next = encoded(
        @mqtt.encode_publish({
          topic: "test/in",
          payload: b"two",
          qos: @mqtt.QoS2,
          message_id: Some(77),
          retain: false,
          dup: true,
        }),
      )
      conn.write(Bytes::from_array(next))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubrecPacket(_)),
      )
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubcompPacket(_)),
      )
      conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/done",
              payload: b"done",
              qos: @mqtt.QoS0,
              message_id: None,
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      let end = read_packet(conn, 4096)
      assert_true(@mqtt.decode_packet(end) is Ok(DisconnectPacket))
    }
    let config = Config::new("127.0.0.1", "qos2-in", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let first = @async.with_timeout(1000, () => client.receive())
    assert_eq(first.payload, b"one")
    let second = @async.with_timeout(1000, () => client.receive())
    assert_eq(second.payload, b"two")
    let done = @async.with_timeout(1000, () => client.receive())
    assert_eq(done.topic, "test/done")
    client.disconnect()
  }
}

///|
async test "bounded inbox closes before acknowledging overflow" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      for id in 1..<=2 {
        conn.write(
          Bytes::from_array(
            encoded(
              @mqtt.encode_publish({
                topic: "test/full",
                payload: b"x",
                qos: @mqtt.QoS1,
                message_id: Some(id),
                retain: false,
                dup: false,
              }),
            ),
          ),
        )
      }
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubackPacket(_)),
      )
      @async.sleep(200)
    }
    let config = Config::new("127.0.0.1", "inbox-full", port=server.addr.port())
    config.keep_alive_seconds = 0
    config.inbox_capacity = 1
    let client = Client::connect(config, group)
    @async.sleep(50)
    let full = try {
      client.publish("test/out", b"x")
      false
    } catch {
      InboxFull => true
      _ => false
    }
    assert_true(full)
    client.close()
  }
}

///|
async test "concurrent requests use distinct IDs and match reordered acknowledgements" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let mut pub_id = 0
      let mut sub_id = 0
      let mut unsub_id = 0
      for _ in 0..<3 {
        match @mqtt.decode_packet(read_packet(conn, 4096)) {
          Ok(PublishPacket(p)) => pub_id = p.message_id.unwrap()
          Ok(SubscribePacket(p)) => sub_id = p.message_id
          Ok(UnsubscribePacket(p)) => unsub_id = p.message_id
          _ => abort("unexpected request")
        }
      }
      assert_true(pub_id != sub_id && sub_id != unsub_id && pub_id != unsub_id)
      conn.write(Bytes::from_array(encoded(@mqtt.encode_unsuback(unsub_id))))
      conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(pub_id))))
      conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({
              message_id: sub_id,
              return_codes: [SuccessQoS2],
            }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "concurrent", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let publish = group.spawn(no_wait=true, allow_failure=true) <| () => {
      client.publish("test/out", b"x", qos=@mqtt.QoS1)
    }
    let subscribe = group.spawn(no_wait=true, allow_failure=true) <| () => {
      assert_eq(client.subscribe("test/#", @mqtt.QoS2), @mqtt.QoS2)
    }
    let unsubscribe = group.spawn(no_wait=true, allow_failure=true) <| () => {
      client.unsubscribe("test/old")
    }
    publish.wait()
    subscribe.wait()
    unsubscribe.wait()
    client.disconnect()
  }
}

///|
async test "wrong QoS 2 acknowledgement order closes and clears request" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let packet = read_packet(conn, 4096)
      let id = match @mqtt.decode_packet(packet) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected publish")
      }
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      @async.sleep(100)
    }
    let config = Config::new("127.0.0.1", "bad-qos2", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let bad = try {
      client.publish("test/qos2", b"x", qos=@mqtt.QoS2)
      false
    } catch {
      Protocol(_) => true
      _ => false
    }
    assert_true(bad)
    assert_true(client.closed)
    assert_true(!client.pending.contains(1))
    client.close()
  }
}

///|
async test "cancelled QoS 1 publish closes connection and clears ID" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let _ = read_packet(conn, 4096)
      @async.sleep(150)
    }
    let config = Config::new("127.0.0.1", "cancelled", port=server.addr.port())
    config.keep_alive_seconds = 0
    config.timeout_ms = 1000
    let client = Client::connect(config, group)
    let cancelled = try {
      @async.with_timeout(30, () => {
        client.publish("test/cancel", b"x", qos=@mqtt.QoS1)
      })
      false
    } catch {
      _ => true
    }
    assert_true(cancelled)
    assert_true(client.closed)
    assert_true(!client.pending.contains(1))
    client.close()
  }
}

///|
async test "disconnect while waiting for PUBCOMP wakes QoS 2 publisher" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let packet = read_packet(conn, 4096)
      let id = match @mqtt.decode_packet(packet) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected publish")
      }
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PubrelPacket(_)),
      )
      conn.close()
    }
    let config = Config::new("127.0.0.1", "drop-qos2", port=server.addr.port())
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    let failed = try {
      client.publish("test/drop", b"x", qos=@mqtt.QoS2)
      false
    } catch {
      _ => true
    }
    assert_true(failed)
    assert_true(client.closed)
    assert_true(!client.pending.contains(1))
    client.close()
  }
}

///|
async test "late acknowledgement on old TCP connection cannot complete new publish" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (old_conn, _) = server.accept()
      defer old_conn.close()
      let _ = read_packet(old_conn, 4096)
      old_conn.write(b"\x20\x02\x00\x00")
      let old_packet = read_packet(old_conn, 4096)
      let old_id = match @mqtt.decode_packet(old_packet) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected old publish")
      }
      let (new_conn, _) = server.accept()
      defer new_conn.close()
      let _ = read_packet(new_conn, 4096)
      new_conn.write(b"\x20\x02\x00\x00")
      let new_packet = read_packet(new_conn, 4096)
      let new_id = match @mqtt.decode_packet(new_packet) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected new publish")
      }
      assert_eq(old_id, new_id)
      old_conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(old_id))))
      @async.sleep(200)
    }
    let old_config = Config::new("127.0.0.1", "old", port=server.addr.port())
    old_config.keep_alive_seconds = 0
    old_config.timeout_ms = 50
    let old_client = Client::connect(old_config, group)
    let old_timed_out = try {
      old_client.publish("test/old", b"x", qos=@mqtt.QoS1)
      false
    } catch {
      TimedOut => true
      _ => false
    }
    assert_true(old_timed_out)
    let new_config = Config::new("127.0.0.1", "new", port=server.addr.port())
    new_config.keep_alive_seconds = 0
    new_config.timeout_ms = 100
    let new_client = Client::connect(new_config, group)
    let new_timed_out = try {
      new_client.publish("test/new", b"y", qos=@mqtt.QoS1)
      false
    } catch {
      TimedOut => true
      _ => false
    }
    assert_true(new_timed_out)
    old_client.close()
    new_client.close()
  }
}

///|
async test "unknown PUBREL receives idempotent PUBCOMP" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(55))))
      let answer = read_packet(conn, 4096)
      match @mqtt.decode_packet(answer) {
        Ok(PubcompPacket(id)) => assert_eq(id.message_id, 55)
        _ => abort("expected PUBCOMP")
      }
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "unknown-rel",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    @async.sleep(30)
    client.disconnect()
  }
}
