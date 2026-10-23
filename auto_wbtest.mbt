///|
async test "automatic reconnect caps backoff and recovers after three failures" {
  let odd_cap = ReconnectPolicy::new(initial_delay_ms=1, max_delay_ms=3)
  assert_eq(odd_cap.next_delay(1), 2)
  assert_eq(odd_cap.next_delay(2), 3)
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let attempts : @async.Queue[Int64] = @async.Queue::Queue(kind=Blocking(4))
    group.spawn_bg() <| () => {
      for _ in 0..<3 {
        let (conn, _) = server.accept()
        attempts.put(@async.now())
        let _ = read_packet(conn, 4096)
        conn.close()
      }
      let (conn, _) = server.accept()
      defer conn.close()
      attempts.put(@async.now())
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x00")
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(PublishPacket(_)),
      )
      assert_true(
        try {
          let _ = read_packet(conn, 4096)
          false
        } catch {
          _ => true
        },
      )
    }
    let config = Config::new(
      "127.0.0.1",
      "auto-backoff",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=20, max_delay_ms=40),
    )
    @async.with_timeout(2000, () => auto.wait_connected())
    assert_true(auto.status() is Connected(false))
    auto.publish("test/out", b"after recovery")
    let t0 = attempts.get()
    let t1 = attempts.get()
    let t2 = attempts.get()
    let t3 = attempts.get()
    assert_true(t1 - t0 >= 15)
    assert_true(t2 - t1 >= 30)
    assert_true(t3 - t2 >= 30)
    auto.stop()
    assert_true(auto.status() is Stopped)
  }
}

///|
async test "automatic mode retries TCP connect failure until listener appears" {
  @async.with_task_group() <| group => {
    let temporary = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    let port = temporary.addr.port()
    temporary.close()
    let config = Config::new("127.0.0.1", "auto-late-listener", port~)
    config.keep_alive_seconds = 0
    config.timeout_ms = 100
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=50, max_delay_ms=100),
    )
    @async.with_timeout(1000, () => {
      while !(auto.status() is Retrying(_, _)) {
        @async.sleep(5)
      }
    })
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:\{port}"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      defer conn.close()
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
    }
    @async.with_timeout(2000, () => auto.wait_connected())
    assert_true(auto.status() is Connected(false))
    auto.stop()
  }
}

///|
async test "stop and cancellation interrupt automatic backoff" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.close()
      let (second, _) = server.accept()
      let _ = read_packet(second, 4096)
      second.close()
    }
    let config = Config::new("127.0.0.1", "auto-stop", port=server.addr.port())
    config.keep_alive_seconds = 0
    let policy = ReconnectPolicy::new(initial_delay_ms=500, max_delay_ms=500)
    let first = AutoClient::start(config, group, policy~)
    @async.with_timeout(1000, () => {
      while !(first.status() is Retrying(1, 500)) {
        @async.sleep(5)
      }
    })
    let waiter = group.spawn(no_wait=true, allow_failure=true) <| () => {
      let stopped = try {
        first.wait_connected()
        false
      } catch {
        Closed => true
        _ => false
      }
      assert_true(stopped)
    }
    @async.with_timeout(200, () => first.stop())
    @async.with_timeout(200, () => waiter.wait())
    assert_true(first.status() is Stopped)
    let second = AutoClient::start(config, group, policy~)
    @async.with_timeout(1000, () => {
      while !(second.status() is Retrying(1, 500)) {
        @async.sleep(5)
      }
    })
    second.task.unwrap().cancel()
    second.task.unwrap().wait() catch {
      _ => ()
    }
    assert_true(second.status() is Stopped)
  }
}

///|
async test "automatic mode stops on rejected connection and permanent errors" {
  assert_true(!retryable(InvalidConfig("bad")))
  assert_true(!retryable(Protocol("bad")))
  assert_true(!retryable(RecoveryUncertain))
  assert_true(!retryable(Storage("bad")))
  assert_true(!retryable(InboxFull))
  assert_true(retryable(TimedOut))
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x00\x05")
      conn.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "auto-reject",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=10, max_delay_ms=20),
    )
    @async.with_timeout(1000, () => {
      while !(auto.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_true(auto.last_error() is Some(Rejected(_)))
    let refused = try {
      auto.wait_connected()
      false
    } catch {
      Rejected(_) => true
      _ => false
    }
    assert_true(refused)
    auto.stop()
  }
}

///|
async test "automatic SP1 resumes QoS 1 PUBLISH and QoS 2 PUBREL without reissuing calls" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let sent : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let mut qos1_id = 0
      let mut qos2_id = 0
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(first, 4096)) {
          Ok(PublishPacket(p)) if p.qos == @mqtt.QoS1 =>
            qos1_id = p.message_id.unwrap()
          Ok(PublishPacket(p)) if p.qos == @mqtt.QoS2 =>
            qos2_id = p.message_id.unwrap()
          _ => abort("expected two initial publications")
        }
      }
      assert_true(qos1_id != 0 && qos2_id != 0 && qos1_id != qos2_id)
      first.write(Bytes::from_array(encoded(@mqtt.encode_pubrec(qos2_id))))
      match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(PubrelPacket(p)) => assert_eq(p.message_id, qos2_id)
        _ => abort("expected first PUBREL")
      }
      sent.put(())
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x01\x00")
      let mut replayed_qos1 = false
      let mut replayed_qos2 = false
      for _ in 0..<2 {
        match @mqtt.decode_packet(read_packet(second, 4096)) {
          Ok(PublishPacket(p)) => {
            assert_eq(p.message_id.unwrap(), qos1_id)
            assert_eq(p.qos, @mqtt.QoS1)
            assert_true(p.dup)
            replayed_qos1 = true
          }
          Ok(PubrelPacket(p)) => {
            assert_eq(p.message_id, qos2_id)
            replayed_qos2 = true
          }
          _ => abort("expected protocol replay only")
        }
      }
      assert_true(replayed_qos1 && replayed_qos2)
      second.write(Bytes::from_array(encoded(@mqtt.encode_puback(qos1_id))))
      second.write(Bytes::from_array(encoded(@mqtt.encode_pubcomp(qos2_id))))
      assert_true(
        @mqtt.decode_packet(read_packet(second, 4096)) is Ok(PublishPacket(_)),
      )
      assert_true(
        try {
          let _ = read_packet(second, 4096)
          false
        } catch {
          _ => true
        },
      )
    }
    let config = Config::new("127.0.0.1", "auto-sp1", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=20, max_delay_ms=40),
    )
    auto.wait_connected()
    let first = group.spawn(no_wait=true, allow_failure=true) <| () => {
      auto.publish("test/q1", b"one", qos=@mqtt.QoS1) catch {
        _ => ()
      }
    }
    let second = group.spawn(no_wait=true, allow_failure=true) <| () => {
      auto.publish("test/q2", b"two", qos=@mqtt.QoS2) catch {
        _ => ()
      }
    }
    @async.with_timeout(1000, () => sent.get())
    first.wait()
    second.wait()
    @async.with_timeout(2000, () => {
      while !(auto.status() is Connected(true)) {
        @async.sleep(5)
      }
    })
    @async.with_timeout(1000, () => {
      while !auto.session.outgoing.is_empty() {
        @async.sleep(5)
      }
    })
    auto.publish("test/fresh", b"fresh")
    auto.stop()
  }
}

///|
async test "automatic SP0 rebuilds subscriptions before becoming connected" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let subscribed : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    let release : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let id = match @mqtt.decode_packet(read_packet(first, 4096)) {
        Ok(SubscribePacket(p)) => p.message_id
        _ => abort("expected original SUBSCRIBE")
      }
      first.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({ message_id: id, return_codes: [SuccessQoS1], }),
          ),
        ),
      )
      subscribed.put(())
      release.get()
      first.close()
      let (second, _) = server.accept()
      defer second.close()
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x00\x00")
      let id2 = match @mqtt.decode_packet(read_packet(second, 4096)) {
        Ok(SubscribePacket(p)) => {
          assert_eq(p.topics[0].topic, "test/#")
          p.message_id
        }
        _ => abort("expected rebuilt SUBSCRIBE")
      }
      second.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_suback({
              message_id: id2,
              return_codes: [SuccessQoS1],
            }),
          ),
        ),
      )
      second.write(
        Bytes::from_array(
          encoded(
            @mqtt.encode_publish({
              topic: "test/in",
              payload: b"after reset",
              qos: @mqtt.QoS0,
              message_id: None,
              retain: false,
              dup: false,
            }),
          ),
        ),
      )
      assert_true(
        try {
          let _ = read_packet(second, 4096)
          false
        } catch {
          _ => true
        },
      )
    }
    let config = Config::new("127.0.0.1", "auto-sp0", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=20, max_delay_ms=40),
    )
    auto.wait_connected()
    assert_eq(auto.subscribe("test/#", @mqtt.QoS1), @mqtt.QoS1)
    subscribed.get()
    release.put(())
    assert_eq(
      @async.with_timeout(2000, () => auto.receive()).payload,
      b"after reset",
    )
    assert_true(auto.status() is Connected(false))
    assert_true(auto.session.subscriptions.contains("test/#"))
    auto.stop()
  }
}

///|
async test "automatic mode does not retry runtime InboxFull or protocol errors" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      let packet = encoded(
        @mqtt.encode_publish({
          topic: "test/full",
          payload: b"x",
          qos: @mqtt.QoS0,
          message_id: None,
          retain: false,
          dup: false,
        }),
      )
      first.write(Bytes::from_array(packet))
      first.write(Bytes::from_array(packet))
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
      let _ = read_packet(second, 4096)
      second.write(b"\x20\x02\x00\x00")
      second.write(b"\xff\x00")
      assert_true(
        try {
          let _ = read_packet(second, 4096)
          false
        } catch {
          _ => true
        },
      )
      second.close()
    }
    let first_config = Config::new(
      "127.0.0.1",
      "auto-full",
      port=server.addr.port(),
    )
    first_config.inbox_capacity = 1
    first_config.keep_alive_seconds = 0
    let first = AutoClient::start(first_config, group)
    @async.with_timeout(1000, () => {
      while !(first.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_true(first.last_error() is Some(InboxFull))
    first.stop()
    let second_config = Config::new(
      "127.0.0.1",
      "auto-protocol",
      port=server.addr.port(),
    )
    second_config.keep_alive_seconds = 0
    let second = AutoClient::start(second_config, group)
    @async.with_timeout(1000, () => {
      while !(second.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_true(second.last_error() is Some(Protocol(_)))
    second.stop()
  }
}

///|
async test "heartbeat timeout enters automatic retry and reconnects" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let retried : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (first, _) = server.accept()
      let _ = read_packet(first, 4096)
      first.write(b"\x20\x02\x00\x00")
      assert_true(
        @mqtt.decode_packet(read_packet(first, 4096)) is Ok(PingreqPacket),
      )
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
      retried.put(())
      assert_true(
        try {
          let _ = read_packet(second, 4096)
          false
        } catch {
          _ => true
        },
      )
    }
    let config = Config::new("127.0.0.1", "auto-ping", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 1
    config.timeout_ms = 100
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=20, max_delay_ms=40),
    )
    @async.with_timeout(3000, () => retried.get())
    @async.with_timeout(1000, () => {
      while !(auto.status() is Connected(true)) {
        @async.sleep(5)
      }
    })
    auto.stop()
  }
}

///|
async test "automatic storage failure is terminal" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let mut writes = 0
    let store = SessionStore::new(() => None, _ => {
      writes += 1
      raise Storage("injected failure")
    })
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
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
    let config = Config::new(
      "127.0.0.1",
      "auto-storage",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=10, max_delay_ms=20),
    )
    @async.with_timeout(1000, () => {
      while !(auto.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_eq(writes, 1)
    assert_true(auto.last_error() is Some(Storage(_)))
    auto.stop()
  }
}

///|
async test "automatic recovery uncertainty is terminal on SP1" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let mut snapshot : Bytes? = None
    let store = SessionStore::new(() => snapshot, bytes => {
      snapshot = Some(bytes)
    })
    let old = Session::new("auto-uncertain", Some(store))
    old.ever_connected = true
    old.connecting = true
    old.persist()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let _ = read_packet(conn, 4096)
      conn.write(b"\x20\x02\x01\x00")
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
    let config = Config::new(
      "127.0.0.1",
      "auto-uncertain",
      port=server.addr.port(),
    )
    config.clean_session = false
    config.session_store = Some(store)
    config.keep_alive_seconds = 0
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=10, max_delay_ms=20),
    )
    @async.with_timeout(1000, () => {
      while !(auto.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_true(auto.last_error() is Some(RecoveryUncertain))
    auto.stop()
  }
}
