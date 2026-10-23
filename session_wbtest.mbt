///|
async test "persistent QoS 1 replays same ID with DUP and reserves it" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let connect = read_packet(first_conn, 4096)
      match @mqtt.decode_packet(connect) {
        Ok(ConnectPacket(packet)) => {
          assert_eq(packet.client_id, "resume-qos1")
          assert_true(!packet.flags.clean_session)
        }
        _ => abort("expected CONNECT")
      }
      first_conn.write(b"\x20\x02\x00\x00")
      let original = read_packet(first_conn, 4096)
      let id = match @mqtt.decode_packet(original) {
        Ok(PublishPacket(p)) => {
          assert_true(!p.dup)
          p.message_id.unwrap()
        }
        _ => abort("expected PUBLISH")
      }
      first_conn.close()
      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let reconnect = read_packet(second_conn, 4096)
      assert_true(@mqtt.decode_packet(reconnect) is Ok(ConnectPacket(_)))
      second_conn.write(b"\x20\x02\x01\x00")
      let repeated = read_packet(second_conn, 4096)
      match @mqtt.decode_packet(repeated) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_true(p.dup)
          assert_eq(p.payload, b"first")
        }
        _ => abort("expected replayed PUBLISH")
      }
      let fresh = read_packet(second_conn, 4096)
      let new_id = match @mqtt.decode_packet(fresh) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected fresh PUBLISH")
      }
      assert_true(new_id != id)
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(new_id))))
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "resume-qos1",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    assert_true(!first.session_present())
    let dropped = try {
      first.publish("test/resume", b"first", qos=@mqtt.QoS1)
      false
    } catch {
      _ => true
    }
    assert_true(dropped)
    let second = first.reconnect(group)
    assert_true(second.session_present())
    let duplicate_connection = try {
      let _ = first.reconnect(group)
      false
    } catch {
      InvalidConfig(_) => true
      _ => false
    }
    assert_true(duplicate_connection)
    second.publish("test/resume", b"second", qos=@mqtt.QoS1)
    @async.sleep(30)
    assert_true(second.session.outgoing.is_empty())
    second.disconnect()
  }
}

///|
async test "persistent QoS 2 replays PUBLISH then PUBREL across two drops" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      let first_publish = read_packet(first_conn, 4096)
      let id = match @mqtt.decode_packet(first_publish) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected first PUBLISH")
      }
      first_conn.close()

      let (second_conn, _) = server.accept()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x01\x00")
      let replayed = read_packet(second_conn, 4096)
      match @mqtt.decode_packet(replayed) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_eq(p.qos, @mqtt.QoS2)
          assert_true(p.dup)
        }
        _ => abort("expected duplicate PUBLISH")
      }
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      let release = read_packet(second_conn, 4096)
      assert_true(@mqtt.decode_packet(release) is Ok(PubrelPacket(_)))
      second_conn.close()

      let (third_conn, _) = server.accept()
      defer third_conn.close()
      let _ = read_packet(third_conn, 4096)
      third_conn.write(b"\x20\x02\x01\x00")
      let replayed_release = read_packet(third_conn, 4096)
      match @mqtt.decode_packet(replayed_release) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, id)
        _ => abort("must replay PUBREL, not PUBLISH")
      }
      third_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(third_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "resume-qos2",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let first_failed = try {
      first.publish("test/qos2", b"once", qos=@mqtt.QoS2)
      false
    } catch {
      _ => true
    }
    assert_true(first_failed)
    let second = first.reconnect(group)
    let second_failed = try {
      let _ = @async.with_timeout(1000, () => second.receive())
      false
    } catch {
      _ => true
    }
    assert_true(second_failed)
    let third = second.reconnect(group)
    assert_true(third.session_present())
    @async.sleep(30)
    assert_true(third.session.outgoing.is_empty())
    third.disconnect()
  }
}

///|
async test "persistent incoming QoS 2 keeps undelivered message and dedup ID" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      first_conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/in",
              payload: b"once",
              qos: @mqtt.QoS2,
              message_id: Some(77),
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(first_conn, 4096))
        is Ok(PubrecPacket(_)),
      )
      first_conn.close()

      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x01\x00")
      second_conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/in",
              payload: b"once",
              qos: @mqtt.QoS2,
              message_id: Some(77),
              retain: false,
              dup: true,
            }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(PubrecPacket(_)),
      )
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(PubcompPacket(_)),
      )
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(PubcompPacket(_)),
      )
      second_conn.write(
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
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "resume-in", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    @async.with_timeout(1000, () => {
      while !first.closed {
        @async.sleep(10)
      }
    })
    assert_eq(first.session.backlog.length(), 1)
    let second = first.reconnect(group)
    assert_true(second.session_present())
    assert_eq(second.receive().payload, b"once")
    assert_eq(second.receive().topic, "test/done")
    second.disconnect()
  }
}

///|
async test "missing broker session drops old in-flight state and resubscribes" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      let subscribe = read_packet(first_conn, 4096)
      let sub_id = match @mqtt.decode_packet(subscribe) {
        Ok(SubscribePacket(p)) => {
          assert_eq(p.topics[0].qos, @mqtt.QoS2)
          p.message_id
        }
        _ => abort("expected first SUBSCRIBE")
      }
      first_conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({
              message_id: sub_id,
              return_codes: [SuccessQoS1],
            }),
          ),
        ),
      )
      first_conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/sub",
              payload: b"old session",
              qos: @mqtt.QoS2,
              message_id: Some(77),
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      let mut saw_rec = false
      let mut saw_publish = false
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(first_conn, 4096)) {
          Ok(PubrecPacket(_)) => saw_rec = true
          Ok(PublishPacket(_)) => saw_publish = true
          _ => abort("expected PUBREC and PUBLISH")
        }
      }
      assert_true(saw_rec && saw_publish)
      first_conn.close()

      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x00\x00")
      let restored = read_packet(second_conn, 4096)
      let id = match @mqtt.decode_packet(restored) {
        Ok(SubscribePacket(p)) => {
          assert_eq(p.topics[0].topic, "test/sub")
          assert_eq(p.topics[0].qos, @mqtt.QoS2)
          p.message_id
        }
        _ => abort("expected resubscribe, not replayed publish")
      }
      second_conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({ message_id: id, return_codes: [SuccessQoS1], }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "lost-session",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    assert_eq(first.subscribe("test/sub", @mqtt.QoS2), @mqtt.QoS1)
    let dropped = try {
      first.publish("test/out", b"lost", qos=@mqtt.QoS1)
      false
    } catch {
      _ => true
    }
    assert_true(dropped)
    let second = first.reconnect(group)
    assert_true(!second.session_present())
    assert_true(second.session.outgoing.is_empty())
    assert_true(second.session.incoming_qos2.is_empty())
    assert_true(second.session.backlog.is_empty())
    assert_true(second.session.subscriptions.contains("test/sub"))
    second.disconnect()
  }
}

///|
async test "disconnect after PUBREC but before PUBREL write replays PUBREL" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let sent : @async.Queue[Int32] = @async.Queue::Queue(kind=Blocking(1))
    let allow_rec : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      let published = read_packet(first_conn, 4096)
      let id = match @mqtt.decode_packet(published) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected PUBLISH")
      }
      sent.put(id)
      allow_rec.get()
      first_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      @async.sleep(100)
      first_conn.close()
      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x01\x00")
      let retry = read_packet(second_conn, 4096)
      match @mqtt.decode_packet(retry) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, id)
        _ => abort("expected PUBREL after acknowledged PUBLISH")
      }
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "between-rec-rel",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let publishing = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/between", b"x", qos=@mqtt.QoS2) catch {
        _ => ()
      }
    }
    let id = sent.get()
    first.write_lock.acquire()
    allow_rec.put(())
    @async.with_timeout(500, () => {
      while first.session.outgoing.get(id).unwrap().stage is WaitPubrec {
        @async.sleep(5)
      }
    })
    assert_true(first.session.outgoing.get(id).unwrap().stage is WaitPubcomp)
    first.close()
    first.write_lock.release()
    publishing.wait()
    let second = first.reconnect(group)
    @async.sleep(30)
    assert_true(second.session.outgoing.is_empty())
    second.disconnect()
  }
}

///|
async test "cancelled persistent publish retains ID until recovered PUBACK" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      let first_packet = read_packet(first_conn, 4096)
      let id = match @mqtt.decode_packet(first_packet) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected PUBLISH")
      }
      @async.sleep(80)
      first_conn.close()
      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second_conn, 4096)) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_true(p.dup)
        }
        _ => abort("expected cancelled publication to replay")
      }
      second_conn.write(Bytes::from_array(encoded(@mqtt.encode_puback(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "cancel-resume",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    config.timeout_ms = 1000
    let first = Client::connect(config, group)
    let cancelled = try {
      @async.with_timeout(30, () => {
        first.publish("test/cancel", b"x", qos=@mqtt.QoS1)
      })
      false
    } catch {
      _ => true
    }
    assert_true(cancelled)
    assert_true(first.closed)
    assert_true(first.pending.is_empty())
    assert_true(first.session.outgoing.contains(1))
    let second = first.reconnect(group)
    @async.sleep(30)
    assert_true(second.session.outgoing.is_empty())
    second.disconnect()
  }
}

///|
async test "persistent connect rejects broker session without local state" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let packet = read_packet(conn, 4096)
      match @mqtt.decode_packet(packet) {
        Ok(ConnectPacket(p)) => assert_true(!p.flags.clean_session)
        _ => abort("expected CONNECT")
      }
      conn.write(b"\x20\x02\x01\x00")
    }
    let config = Config::new("127.0.0.1", "orphaned", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    let refused = try {
      let _ = Client::connect(config, group)
      false
    } catch {
      Protocol(_) => true
      _ => false
    }
    assert_true(refused)
  }
}

///|
async test "concurrent persistent publications replay in original order" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first_conn, _) = server.accept()
      let _ = read_packet(first_conn, 4096)
      first_conn.write(b"\x20\x02\x00\x00")
      let ids : Array[Int32] = []
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(first_conn, 4096)) {
          Ok(PublishPacket(p)) => ids.push(p.message_id.unwrap())
          _ => abort("expected original PUBLISH")
        }
      }
      assert_true(ids[0] != ids[1])
      first_conn.close()
      let (second_conn, _) = server.accept()
      defer second_conn.close()
      let _ = read_packet(second_conn, 4096)
      second_conn.write(b"\x20\x02\x01\x00")
      for expected in ids {
        match @mqtt.decode_packet(read_packet(second_conn, 4096)) {
          Ok(PublishPacket(p)) => {
            assert_eq(p.message_id.unwrap(), expected)
            assert_true(p.dup)
          }
          _ => abort("expected ordered replay")
        }
        second_conn.write(
          Bytes::from_array(encoded(@mqtt.encode_puback(expected))),
        )
      }
      assert_true(
        @mqtt.decode_packet(read_packet(second_conn, 4096))
        is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "ordered", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let one = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/order", b"one", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    let two = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/order", b"two", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    one.wait()
    two.wait()
    let second = first.reconnect(group)
    @async.sleep(30)
    assert_true(second.session.outgoing.is_empty())
    second.disconnect()
  }
}

///|
async test "finish drains unread in memory session and clears clean session" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      for _ in 0..<2 {
        let (conn, _) = server.accept()
        let _ = read_packet(conn, 4096)
        conn.write(b"\x20\x02\x00\x00")
        assert_true(
          try {
            let _ = read_packet(conn, 4096)
            false
          } catch {
            _ => true
          },
        )
        conn.close()
      }
    }
    let config = Config::new(
      "127.0.0.1",
      "finish-memory",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.keep_alive_seconds = 0
    let persistent = Client::connect(config, group)
    let message : Message = {
      topic: "test/in",
      payload: b"queued",
      qos: @mqtt.QoS2,
      retain: false,
      dup: false,
    }
    assert_true(persistent.messages.try_put(message))
    persistent.session.incoming_qos2[77] = false
    persistent.session.subscriptions["test/#"] = @mqtt.QoS1
    persistent.close()
    assert_eq(persistent.session.backlog.length(), 1)
    assert_eq(persistent.session.backlog[0].payload, b"queued")
    assert_true(persistent.session.incoming_qos2.get(77) is Some(false))
    assert_true(persistent.session.subscriptions.contains("test/#"))

    let clean_config = Config::new(
      "127.0.0.1",
      "finish-clean",
      port=server.addr.port(),
    )
    clean_config.keep_alive_seconds = 0
    let clean = Client::connect(clean_config, group)
    assert_true(clean.messages.try_put(message))
    clean.session.backlog.push(message)
    clean.session.incoming_qos2[77] = false
    clean.session.subscriptions["test/#"] = @mqtt.QoS1
    clean.session.outgoing[12] = {
      topic: "test/out",
      payload: b"pending",
      qos: @mqtt.QoS1,
      retain: false,
      stage: WaitPuback,
      pubrel_sent: false,
    }
    clean.session.order.push(12)
    clean.close()
    assert_true(clean.session.backlog.is_empty())
    assert_true(clean.session.incoming_qos2.is_empty())
    assert_true(clean.session.subscriptions.is_empty())
    assert_true(clean.session.outgoing.is_empty())
    assert_true(clean.session.order.is_empty())
  }
}
