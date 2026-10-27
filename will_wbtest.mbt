///|
async test "will configuration rejects invalid topic and oversized binary payload" {
  @async.with_task_group() <| group => {
    let config = Config::new("127.0.0.1", "will-invalid", port=1)
    for topic in ["", "bad/#", "bad/+", "bad\u{0000}name"] {
      config.will = Some(Will::new(topic, b"x"))
      assert_true(
        try {
          let _ = Client::connect(config, group)
          false
        } catch {
          InvalidConfig(_) => true
          _ => false
        },
      )
    }
    let too_big : Array[Byte] = []
    for _ in 0..<65536 {
      too_big.push(0)
    }
    config.will = Some(Will::new("test/will", Bytes::from_array(too_big)))
    assert_true(
      try {
        let _ = AutoClient::start(config, group)
        false
      } catch {
        InvalidConfig(_) => true
        _ => false
      },
    )
  }
}

///|
async test "CONNECT encodes binary Will with QoS 0 1 2 and retain" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let qoses = [@mqtt.QoS0, @mqtt.QoS1, @mqtt.QoS2]
    let payload = Bytes::from_array([0x00, 0xff, 0x10])
    group.spawn_bg() <| () => {
      for i in 0..<3 {
        let (conn, _) = server.accept()
        let packet = read_packet(conn, 4096)
        match @mqtt.decode_packet(packet) {
          Ok(ConnectPacket(p)) => {
            assert_eq(p.client_id, "will-manual-\{i}")
            assert_true(p.flags.will_flag)
            assert_eq(p.flags.will_qos, qoses[i])
            assert_eq(p.flags.will_retain, i == 2)
            assert_eq(p.will_topic, Some("test/will"))
            assert_eq(p.will_message, Some(payload))
          }
          _ => abort("expected CONNECT with Will")
        }
        conn.write(b"\x20\x02\x00\x00")
        assert_true(
          @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
        )
        conn.close()
      }
    }
    for i in 0..<3 {
      let config = Config::new(
        "127.0.0.1",
        "will-manual-\{i}",
        port=server.addr.port(),
      )
      config.keep_alive_seconds = 0
      config.will = Some(
        Will::new("test/will", payload, qos=qoses[i], retain=i == 2),
      )
      let client = Client::connect(config, group)
      client.disconnect()
    }
  }
}

///|
async test "automatic reconnect repeats Will in each CONNECT and stop sends DISCONNECT" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      for generation in 0..<2 {
        let (conn, _) = server.accept()
        match @mqtt.decode_packet(read_packet(conn, 4096)) {
          Ok(ConnectPacket(p)) => {
            assert_eq(p.client_id, "will-auto")
            assert_eq(p.will_topic, Some("test/auto-will"))
            assert_eq(p.will_message, Some(b"offline"))
            assert_true(p.flags.will_flag)
            assert_true(p.flags.will_retain)
            assert_eq(p.flags.will_qos, @mqtt.QoS2)
          }
          _ => abort("expected Will on every CONNECT")
        }
        if generation == 0 {
          conn.write(b"\x20\x02\x00\x00")
          conn.close()
        } else {
          conn.write(b"\x20\x02\x01\x00")
          assert_true(
            @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
          )
          conn.close()
        }
      }
    }
    let config = Config::new("127.0.0.1", "will-auto", port=server.addr.port())
    config.clean_session = false
    config.keep_alive_seconds = 0
    config.will = Some(
      Will::new("test/auto-will", b"offline", qos=@mqtt.QoS2, retain=true),
    )
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=20, max_delay_ms=40),
    )
    @async.with_timeout(2000, () => {
      while !(auto.status() is Connected(true)) {
        @async.sleep(5)
      }
    })
    assert_true(
      try {
        auto.stop()
        false
      } catch {
        DisconnectUncertain => true
        _ => false
      },
    )
    assert_true(auto.status() is Stopped)
  }
}

///|
async test "failed automatic DISCONNECT still cleans up and reports uncertainty" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
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
    let config = Config::new(
      "127.0.0.1",
      "will-stop-fail",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.will = Some(Will::new("test/will", b"offline"))
    let auto = AutoClient::start(config, group)
    auto.wait_connected()
    let uncertain = try {
      auto.stop_with_disconnect(_ => raise Closed)
      false
    } catch {
      DisconnectUncertain => true
      _ => false
    }
    assert_true(uncertain)
    assert_true(auto.status() is Stopped)
    assert_true(auto.current is None)
  }
}

///|
async test "successful local DISCONNECT write cannot prove Will was discarded" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(ConnectPacket(_)),
      )
      conn.write(b"\x20\x02\x00\x00")
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(DisconnectPacket),
      )
      conn.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "will-clean-stop-result",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.will = Some(Will::new("test/clean-stop", b"offline"))
    let auto = AutoClient::start(config, group)
    auto.wait_connected()
    assert_true(
      try {
        auto.stop()
        false
      } catch {
        DisconnectUncertain => true
        _ => false
      },
    )
    assert_true(
      try {
        auto.stop()
        false
      } catch {
        DisconnectUncertain => true
        _ => false
      },
    )
    assert_true(auto.status() is Stopped)
  }
}

///|
async test "stop during CONNACK wait reports possible Will publication" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let connected : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(ConnectPacket(_)),
      )
      connected.put(())
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
      "will-stop-handshake",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.will = Some(Will::new("test/handshake", b"offline"))
    let auto = AutoClient::start(config, group)
    @async.with_timeout(2000, () => connected.get())
    assert_true(auto.status() is Connecting)
    let uncertain = try {
      auto.stop()
      false
    } catch {
      DisconnectUncertain => true
      _ => false
    }
    assert_true(uncertain)
    assert_true(auto.status() is Stopped)
  }
}

///|
async test "stop in retry after accepted Will reports uncertainty" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      assert_true(
        @mqtt.decode_packet(read_packet(conn, 4096)) is Ok(ConnectPacket(_)),
      )
      conn.write(b"\x20\x02\x00\x00")
      conn.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "will-stop-retry",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    config.will = Some(Will::new("test/retry", b"offline"))
    let auto = AutoClient::start(
      config,
      group,
      policy=ReconnectPolicy::new(initial_delay_ms=500, max_delay_ms=500),
    )
    @async.with_timeout(2000, () => {
      while !(auto.status() is Retrying(_, _)) {
        @async.sleep(5)
      }
    })
    let uncertain = try {
      auto.stop()
      false
    } catch {
      DisconnectUncertain => true
      _ => false
    }
    assert_true(uncertain)
    assert_true(auto.status() is Stopped)
  }
}
