///|
priv struct FaultStoreState {
  mut data : Bytes?
  mut writes : Int32
  mut fail_at : Int32
}

///|
fn fault_store() -> (SessionStore, FaultStoreState) {
  let state = { data: None, writes: 0, fail_at: -1, }
  let store = SessionStore::new(() => state.data, bytes => {
    state.writes += 1
    if state.writes == state.fail_at {
      raise Storage("injected write failure")
    }
    state.data = Some(bytes)
  })
  (store, state)
}

///|
async test "versioned session file rejects corruption version and client ID" {
  let directory = @fs.tmpdir(prefix="moonmqtt-store")
  defer @fs.rmdir(directory, recursive=true)
  let store = SessionStore::file(directory + "/session.bin")
  assert_true((store.load_data)() is None)
  let session = Session::new("stable", Some(store))
  session.ever_connected = true
  session.next_id = 17
  session.outgoing[7] = {
    topic: "test/q2",
    payload: b"payload",
    qos: @mqtt.QoS2,
    retain: false,
    stage: WaitPubcomp,
    pubrel_sent: true,
  }
  session.order.push(7)
  session.incoming_qos2[19] = false
  session.subscriptions["test/#"] = @mqtt.QoS1
  session.backlog.push({
    topic: "test/in",
    payload: b"queued",
    qos: @mqtt.QoS2,
    retain: false,
    dup: false,
  })
  session.persist()
  let bytes = (store.load_data)().unwrap()
  let recovered = decode_session(bytes, "stable", store)
  assert_eq(recovered.next_id, 17)
  assert_true(recovered.outgoing.get(7).unwrap().stage is WaitPubcomp)
  assert_true(recovered.incoming_qos2.get(19) is Some(false))
  assert_eq(recovered.backlog[0].payload, b"queued")
  assert_true(
    try {
      let _ = decode_session(bytes, "other", store)
      false
    } catch {
      SessionClientIdMismatch(_) => true
      _ => false
    },
  )
  let version = bytes.to_array()
  version[4] = 4
  assert_true(
    try {
      let _ = decode_session(Bytes::from_array(version), "stable", store)
      false
    } catch {
      IncompatibleSessionVersion(4) => true
      _ => false
    },
  )
  let damaged = bytes.to_array()
  damaged[damaged.length() - 5] = damaged[damaged.length() - 5] ^ 1
  assert_true(
    try {
      let _ = decode_session(Bytes::from_array(damaged), "stable", store)
      false
    } catch {
      CorruptSession(_) => true
      _ => false
    },
  )
}

///|
async test "store failure before outgoing PUBLISH prevents network send" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let next = try {
        let _ = read_packet(conn, 4096)
        true
      } catch {
        _ => false
      }
      assert_true(!next)
    }
    let config = Config::new(
      "127.0.0.1",
      "write-fault",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    assert_eq(state.writes, 2)
    state.fail_at = 3
    let failed = try {
      client.publish("test/no-send", b"x", qos=@mqtt.QoS1)
      false
    } catch {
      Storage(_) => true
      _ => false
    }
    assert_true(failed)
    assert_true(client.closed)
    let persisted = decode_session(state.data.unwrap(), "write-fault", store)
    assert_true(persisted.outgoing.is_empty())
    client.close()
  }
}

///|
async test "restart keeps outgoing QoS 2 PUBREL stage and original ID" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let published : @async.Queue[Int32] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected initial PUBLISH")
      }
      published.put(id)
      first.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      @async.sleep(80)
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, id)
        _ => abort("restart must retransmit PUBREL")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "restart-q2", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let pub_task = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/q2", b"once", qos=@mqtt.QoS2) catch {
        _ => ()
      }
    }
    let id = published.get()
    @async.with_timeout(1000, () => {
      while !(first.session.outgoing.get(id).unwrap().stage is WaitPubcomp) {
        @async.sleep(5)
      }
    })
    first.close()
    pub_task.wait()
    let disk = decode_session(state.data.unwrap(), "restart-q2", store)
    assert_true(disk.outgoing.get(id).unwrap().stage is WaitPubcomp)
    let fresh = Client::connect(config, group)
    assert_true(fresh.session_present())
    @async.sleep(30)
    assert_true(fresh.session.outgoing.is_empty())
    fresh.disconnect()
  }
}

///|
async test "incoming QoS 2 is durable before PUBREC and not delivered twice after restart" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let first_ack : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let second_ack : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let packet = encoded(
      @mqtt.encode_publish({
        topic: "test/in",
        payload: b"once",
        qos: @mqtt.QoS2,
        message_id: Some(44),
        retain: false,
        dup: false,
      }),
    )
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      first.write(Bytes::from_array(packet))
      assert_true(
        @mqtt.decode_packet(read_packet(first, 4096)) is Ok(PubrecPacket(_)),
      )
      first_ack.put(())
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      let duplicate = encoded(
        @mqtt.encode_publish({
          topic: "test/in",
          payload: b"once",
          qos: @mqtt.QoS2,
          message_id: Some(44),
          retain: false,
          dup: true,
        }),
      )
      second.write(Bytes::from_array(duplicate))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubrecPacket(_)),
      )
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(44))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubcompPacket(_)),
      )
      second_ack.put(())
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "restart-in", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    first_ack.get()
    first.close()
    assert_eq(
      decode_session(state.data.unwrap(), "restart-in", store).backlog.length(),
      1,
    )
    let second = Client::connect(config, group)
    assert_true(second.session_present())
    assert_eq(second.receive().payload, b"once")
    second_ack.get()
    assert_true(second.session.backlog.is_empty())
    second.disconnect()
    assert_true(
      decode_session(state.data.unwrap(), "restart-in", store).backlog.is_empty(),
    )
  }
}

///|
async test "restart replays QoS 1 PUBLISH with original ID and DUP" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let sent : @async.Queue[Int32] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected QoS 1 PUBLISH")
      }
      sent.put(id)
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_eq(p.qos, @mqtt.QoS1)
          assert_true(p.dup)
        }
        _ => abort("expected replayed QoS 1 PUBLISH")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_puback(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "restart-q1", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let job = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/q1", b"once", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    let id = sent.get()
    job.wait()
    assert_true(
      decode_session(state.data.unwrap(), "restart-q1", store).outgoing.contains(
        id,
      ),
    )
    let second = Client::connect(config, group)
    assert_true(second.session_present())
    @async.with_timeout(1000, () => {
      while !second.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    second.disconnect()
    assert_true(
      decode_session(state.data.unwrap(), "restart-q1", store).outgoing.is_empty(),
    )
  }
}

///|
async test "failed durable receive prevents PUBREC and broker retry is delivered" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let first_done : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let second_done : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let packet = encoded(
      @mqtt.encode_publish({
        topic: "test/fault",
        payload: b"retry",
        qos: @mqtt.QoS2,
        message_id: Some(38),
        retain: false,
        dup: false,
      }),
    )
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      first.write(Bytes::from_array(packet))
      let got_ack = try {
        let _ = read_packet(first, 4096)
        true
      } catch {
        _ => false
      }
      assert_true(!got_ack)
      first_done.put(())
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      second.write(Bytes::from_array(packet))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubrecPacket(_)),
      )
      second_done.put(())
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "fault-in", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    state.fail_at = 3
    let first = Client::connect(config, group)
    @async.with_timeout(1000, () => {
      while !first.closed {
        @async.sleep(5)
      }
    })
    first.close()
    first_done.get()
    assert_true(first.closed)
    let cannot_reuse = try {
      let _ = first.reconnect(group)
      false
    } catch {
      Storage(_) => true
      _ => false
    }
    assert_true(cannot_reuse)
    let disk = decode_session(state.data.unwrap(), "fault-in", store)
    assert_true(disk.incoming_qos2.is_empty())
    assert_true(disk.backlog.is_empty())
    state.fail_at = -1
    let second = Client::connect(config, group)
    second_done.get()
    assert_eq(second.receive().payload, b"retry")
    second.disconnect()
  }
}

///|
async test "failed PUBCOMP checkpoint leaves inbound QoS 2 state for restart" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let got_rec : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let allow_rel : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let first_done : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let second_done : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      first.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/pubcomp",
              payload: b"once",
              qos: @mqtt.QoS2,
              message_id: Some(41),
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(first, 4096)) is Ok(PubrecPacket(_)),
      )
      got_rec.put(())
      allow_rel.get()
      first.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(41))))
      let got_comp = try {
        let _ = read_packet(first, 4096)
        true
      } catch {
        _ => false
      }
      assert_true(!got_comp)
      first_done.put(())
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(41))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubcompPacket(_)),
      )
      second_done.put(())
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "fault-pubcomp",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    got_rec.get()
    state.fail_at = state.writes + 1
    allow_rel.put(())
    @async.with_timeout(1000, () => {
      while !first.closed {
        @async.sleep(5)
      }
    })
    first.close()
    first_done.get()
    assert_true(first.closed)
    let disk = decode_session(state.data.unwrap(), "fault-pubcomp", store)
    assert_true(disk.incoming_qos2.get(41) is Some(false))
    assert_eq(disk.backlog.length(), 1)
    state.fail_at = -1
    let second = Client::connect(config, group)
    second_done.get()
    assert_eq(second.receive().payload, b"once")
    second.disconnect()
  }
}

///|
async test "new broker session clears durable flows and rebuilds subscriptions" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let old = Session::new("restart-new", Some(store))
    old.ever_connected = true
    old.outgoing[12] = {
      topic: "test/old",
      payload: b"stale",
      qos: @mqtt.QoS2,
      retain: false,
      stage: WaitPubcomp,
      pubrel_sent: true,
    }
    old.order.push(12)
    old.incoming_qos2[30] = false
    old.subscriptions["test/#"] = @mqtt.QoS2
    old.backlog.push({
      topic: "test/old",
      payload: b"stale",
      qos: @mqtt.QoS2,
      retain: false,
      dup: false,
    })
    old.persist()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      let id = match @mqtt.decode_packet(read_packet(conn, 4096)) {
        Ok(SubscribePacket(p)) => {
          assert_eq(p.topics[0].topic, "test/#")
          assert_eq(p.topics[0].qos, @mqtt.QoS2)
          p.message_id
        }
        _ => abort("expected restored subscription, not old PUBREL")
      }
      conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({ message_id: id, return_codes: [SuccessQoS2], }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "restart-new",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    assert_true(!client.session_present())
    assert_true(client.session.outgoing.is_empty())
    assert_true(client.session.incoming_qos2.is_empty())
    assert_true(client.session.backlog.is_empty())
    client.disconnect()
    let disk = decode_session(state.data.unwrap(), "restart-new", store)
    assert_true(disk.outgoing.is_empty())
    assert_true(disk.incoming_qos2.is_empty())
    assert_true(disk.backlog.is_empty())
    assert_true(disk.subscriptions.contains("test/#"))
  }
}

///|
async test "uncertain CONNECT checkpoint rejects SP1 and clears on SP0" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let old = Session::new("uncertain", Some(store))
    old.ever_connected = true
    old.connecting = true
    old.outgoing[9] = {
      topic: "test/old",
      payload: b"old",
      qos: @mqtt.QoS2,
      retain: false,
      stage: WaitPubcomp,
      pubrel_sent: false,
    }
    old.order.push(9)
    old.persist()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x01\x00")
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x00\x00")
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "uncertain", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let refused = try {
      let _ = Client::connect(config, group)
      false
    } catch {
      RecoveryUncertain => true
      _ => false
    }
    assert_true(refused)
    let second = Client::connect(config, group)
    assert_true(!second.session_present())
    second.disconnect()
    let disk = decode_session(state.data.unwrap(), "uncertain", store)
    assert_true(!disk.connecting)
    assert_true(disk.outgoing.is_empty())
  }
}

///|
async test "failed PUBREC checkpoint sends no PUBREL and retries stored PUBLISH" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    state.fail_at = 4
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(PublishPacket(p)) => p.message_id.unwrap()
        _ => abort("expected PUBLISH")
      }
      first.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      let got_rel = try {
        let _ = read_packet(first, 4096)
        true
      } catch {
        _ => false
      }
      assert_true(!got_rel)
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_true(p.dup)
        }
        _ => abort("uncheckpointed PUBREC requires retrying PUBLISH")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubrelPacket(_)),
      )
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "fault-pubrec",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    let failed = try {
      first.publish("test/pubrec", b"x", qos=@mqtt.QoS2)
      false
    } catch {
      Storage(_) => true
      _ => false
    }
    assert_true(failed)
    first.close()
    let disk = decode_session(state.data.unwrap(), "fault-pubrec", store)
    assert_true(disk.outgoing.get(1).unwrap().stage is WaitPubrec)
    state.fail_at = -1
    let second = Client::connect(config, group)
    @async.with_timeout(1000, () => {
      while !second.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    second.disconnect()
  }
}

///|
async test "restart completes subscription restoration left by SP0" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let saved = Session::new("restore-intent", Some(store))
    saved.ever_connected = true
    saved.resubscribe_needed = true
    saved.subscriptions["test/restore"] = @mqtt.QoS2
    saved.persist()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x01\x00")
      let id = match @mqtt.decode_packet(read_packet(conn, 4096)) {
        Ok(SubscribePacket(p)) => {
          assert_eq(p.topics[0].topic, "test/restore")
          assert_eq(p.topics[0].qos, @mqtt.QoS2)
          p.message_id
        }
        _ => abort("expected resumed SUBSCRIBE")
      }
      conn.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({ message_id: id, return_codes: [SuccessQoS2], }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "restore-intent",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    assert_true(client.session_present())
    assert_true(!client.session.resubscribe_needed)
    client.disconnect()
    assert_true(
      !decode_session(state.data.unwrap(), "restore-intent", store).resubscribe_needed,
    )
  }
}

///|
async test "failed SP0 reset checkpoint cannot revive stale flows on SP1" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let saved = Session::new("reset-fault", Some(store))
    saved.ever_connected = true
    saved.outgoing[51] = {
      topic: "test/stale",
      payload: b"stale",
      qos: @mqtt.QoS2,
      retain: false,
      stage: WaitPubcomp,
      pubrel_sent: false,
    }
    saved.order.push(51)
    saved.persist()
    state.fail_at = 3
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      first.close()
      let (second, _) = server.accept()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      second.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "reset-fault",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let failed = try {
      let _ = Client::connect(config, group)
      false
    } catch {
      Storage(_) => true
      _ => false
    }
    assert_true(failed)
    let disk = decode_session(state.data.unwrap(), "reset-fault", store)
    assert_true(disk.connecting)
    assert_true(disk.outgoing.contains(51))
    state.fail_at = -1
    let refused = try {
      let _ = Client::connect(config, group)
      false
    } catch {
      RecoveryUncertain => true
      _ => false
    }
    assert_true(refused)
  }
}

///|
async test "old stored client reconnects after close with QoS 1 and inbound QoS 2" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let ready : @async.Queue[Int32] = @async.Queue::Queue(kind=Blocking(1))
    let inbound = encoded(
      @mqtt.encode_publish({
        topic: "test/in",
        payload: b"once",
        qos: @mqtt.QoS2,
        message_id: Some(77),
        retain: false,
        dup: false,
      }),
    )
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let sub_id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(SubscribePacket(p)) => p.message_id
        _ => abort("expected SUBSCRIBE")
      }
      first.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({
              message_id: sub_id,
              return_codes: [SuccessQoS2],
            }),
          ),
        ),
      )
      first.write(Bytes::from_array(inbound))
      let mut id = 0
      let mut saw_rec = false
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(first, 4096)) {
          Ok(PubrecPacket(p)) => {
            assert_eq(p.message_id, 77)
            saw_rec = true
          }
          Ok(PublishPacket(p)) => {
            assert_eq(p.qos, @mqtt.QoS1)
            id = p.message_id.unwrap()
          }
          _ => abort("expected PUBREC or QoS 1 PUBLISH")
        }
      }
      assert_true(saw_rec && id != 0)
      ready.put(id)
      let closed = try {
        let _ = read_packet(first, 4096)
        false
      } catch {
        _ => true
      }
      assert_true(closed)
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PublishPacket(p)) => {
          assert_eq(p.message_id.unwrap(), id)
          assert_true(p.dup)
        }
        _ => abort("expected replayed QoS 1 PUBLISH")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_puback(id))))
      let duplicate = encoded(
        @mqtt.encode_publish({
          topic: "test/in",
          payload: b"once",
          qos: @mqtt.QoS2,
          message_id: Some(77),
          retain: false,
          dup: true,
        }),
      )
      second.write(Bytes::from_array(duplicate))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubrecPacket(_)),
      )
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(77))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubcompPacket(_)),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "old-close", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    assert_eq(first.subscribe("test/#", @mqtt.QoS2), @mqtt.QoS2)
    let job = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/out", b"pending", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    let id = ready.get()
    first.close()
    job.wait()
    let disk = decode_session(state.data.unwrap(), "old-close", store)
    assert_true(disk.outgoing.contains(id))
    assert_true(disk.incoming_qos2.get(77) is Some(false))
    assert_true(disk.subscriptions.contains("test/#"))
    assert_eq(disk.backlog.length(), 1)
    assert_true(first.session.outgoing.contains(id))
    assert_true(first.session.incoming_qos2.get(77) is Some(false))
    assert_true(first.session.subscriptions.contains("test/#"))
    assert_eq(first.session.backlog.length(), 1)
    let second = first.reconnect(group)
    assert_true(second.session_present())
    assert_eq(second.receive().payload, b"once")
    @async.with_timeout(1000, () => {
      while !second.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    assert_true(second.session.backlog.is_empty())
    assert_true(second.session.subscriptions.contains("test/#"))
    second.disconnect()
  }
}

///|
async test "old stored client reconnects after drop in QoS 2 PUBREL stage" {
  @async.with_task_group() <| group => {
    let (store, state) = fault_store()
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let sent : @async.Queue[Int32] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let sub_id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(SubscribePacket(p)) => p.message_id
        _ => abort("expected SUBSCRIBE")
      }
      first.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({
              message_id: sub_id,
              return_codes: [SuccessQoS2],
            }),
          ),
        ),
      )
      first.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/in",
              payload: b"once",
              qos: @mqtt.QoS2,
              message_id: Some(88),
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      let mut id = 0
      let mut saw_rec = false
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(first, 4096)) {
          Ok(PubrecPacket(p)) => {
            assert_eq(p.message_id, 88)
            saw_rec = true
          }
          Ok(PublishPacket(p)) => {
            assert_eq(p.qos, @mqtt.QoS2)
            id = p.message_id.unwrap()
          }
          _ => abort("expected PUBREC or QoS 2 PUBLISH")
        }
      }
      assert_true(saw_rec && id != 0)
      first.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(id))))
      match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, id)
        _ => abort("expected PUBREL")
      }
      sent.put(id)
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, id)
        _ => abort("expected replayed PUBREL")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(id))))
      second.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/in",
              payload: b"once",
              qos: @mqtt.QoS2,
              message_id: Some(88),
              retain: false,
              dup: true,
            }),
          ),
        ),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubrecPacket(_)),
      )
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubrel(88))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PubcompPacket(_)),
      )
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "old-drop", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    assert_eq(first.subscribe("test/#", @mqtt.QoS2), @mqtt.QoS2)
    let job = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/out", b"pending", qos=@mqtt.QoS2) catch {
        _ => ()
      }
    }
    let id = @async.with_timeout(2000, () => sent.get())
    @async.with_timeout(1000, () => {
      while !first.closed {
        @async.sleep(5)
      }
    })
    job.wait()
    let disk = decode_session(state.data.unwrap(), "old-drop", store)
    assert_true(disk.outgoing.get(id).unwrap().stage is WaitPubcomp)
    assert_true(disk.incoming_qos2.get(88) is Some(false))
    assert_true(disk.subscriptions.contains("test/#"))
    assert_eq(disk.backlog.length(), 1)
    assert_true(first.session.outgoing.contains(id))
    assert_true(first.session.outgoing.get(id).unwrap().stage is WaitPubcomp)
    assert_true(first.session.incoming_qos2.get(88) is Some(false))
    assert_true(first.session.subscriptions.contains("test/#"))
    assert_eq(first.session.backlog.length(), 1)
    let second = first.reconnect(group)
    assert_true(second.session_present())
    assert_eq(second.receive().payload, b"once")
    @async.with_timeout(1000, () => {
      while !second.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    assert_true(second.session.subscriptions.contains("test/#"))
    second.disconnect()
  }
}

///|
async test "closing during durable publish save keeps snapshot and old client aligned" {
  @async.with_task_group() <| group => {
    let mut snapshot : Bytes? = None
    let mut writes = 0
    let entered : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let release : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let store = SessionStore::new(() => snapshot, bytes => {
      writes += 1
      if writes == 3 {
        entered.put(())
        release.get()
      }
      snapshot = Some(bytes)
    })
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      assert_true(
        try {
          let _ = read_packet(first, 4096)
          false
        } catch {
          _ => true
        },
      )
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      let id = match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(PublishPacket(p)) => {
          assert_true(p.dup)
          assert_eq(p.payload, b"pending")
          p.message_id.unwrap()
        }
        _ => abort("expected replayed PUBLISH after delayed save")
      }
      second.write(Bytes::from_array(encoded(@mqtt.encode_puback(id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(DisconnectPacket),
      )
    }
    let config = Config::new("127.0.0.1", "save-close", port=server.addr.port())
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let first = Client::connect(config, group)
    assert_eq(writes, 2)
    let publish_job = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.publish("test/out", b"pending", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    @async.with_timeout(1000, () => entered.get())
    let close_job = group.spawn(no_wait=true, allow_failure=true) <| () => {
      first.close()
    }
    @async.with_timeout(1000, () => {
      while !first.closed {
        @async.sleep(5)
      }
    })
    release.put(())
    publish_job.wait()
    close_job.wait()
    let disk = decode_session(snapshot.unwrap(), "save-close", store)
    assert_eq(writes, 3)
    assert_eq(disk.order.length(), 1)
    let id = disk.order[0]
    assert_true(disk.outgoing.get(id).unwrap().stage is WaitPuback)
    assert_true(first.session.outgoing.contains(id))
    assert_eq(first.session.order, disk.order)
    let second = first.reconnect(group)
    assert_true(second.session_present())
    @async.with_timeout(1000, () => {
      while !second.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    second.disconnect()
  }
}
