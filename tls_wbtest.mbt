///|
async test "TLS handshake times out before MQTT CONNECT" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let header = conn.read_exactly(5)
      assert_eq(header[0], 0x16)
      let length = header[3].to_int() * 256 + header[4].to_int()
      let hello = conn.read_exactly(length)
      let host = b"localhost"
      let mut sni = false
      for i in 2..<(hello.length() - host.length()) {
        if hello[i - 2] == 0 && hello[i - 1] == 9 {
          let mut matches = true
          for j in 0..<host.length() {
            if hello[i + j] != host[j] {
              matches = false
            }
          }
          if matches {
            sni = true
            break
          }
        }
      }
      assert_true(sni)
      @async.sleep(700)
      conn.close()
    }
    let config = Config::new(
      "localhost",
      "tls-timeout",
      port=server.addr.port(),
    )
    config.tls = Some(TlsConfig::new())
    config.timeout_ms = 500
    assert_true(
      try {
        let _ = Client::connect(config, group)
        false
      } catch {
        TimedOut => true
        _ => false
      },
    )
  }
}

///|
async test "TCP closes even when TLS close reports failure" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let peer_closed : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
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
      peer_closed.put(())
    }
    let config = Config::new(
      "127.0.0.1",
      "tls-close-fault",
      port=server.addr.port(),
    )
    config.keep_alive_seconds = 0
    let client = Client::connect(config, group)
    assert_true(
      try {
        client.close_with_tls_close(() => {
          Err(Protocol("injected TLS close failure"))
        })
        false
      } catch {
        Protocol("injected TLS close failure") => true
        _ => false
      },
    )
    @async.with_timeout(1000, () => peer_closed.get())
    assert_true(client.closed)
    assert_true(client.disposed)
  }
}

///|
async test "stop during TLS handshake has no MQTT Will to revoke" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    let hello : @async.Queue[Unit] = @async.Queue::Queue(kind=Blocking(1))
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let _ = conn.read_exactly(1)
      hello.put(())
      @async.sleep(500)
      conn.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "tls-stop-handshake",
      port=server.addr.port(),
    )
    config.tls = Some(TlsConfig::new())
    config.will = Some(Will::new("test/tls-handshake", b"offline"))
    config.timeout_ms = 2000
    let auto = AutoClient::start(config, group)
    @async.with_timeout(1000, () => hello.get())
    auto.stop()
    assert_true(auto.status() is Stopped)
  }
}

///|
async test "unreadable TLS CA is a permanent configuration error" {
  @async.with_task_group() <| group => {
    let config = Config::new("127.0.0.1", "tls-missing-ca", port=1)
    config.tls = Some(
      TlsConfig::new(ca_file="./this-moonmqtt-ca-file-does-not-exist.pem"),
    )
    assert_true(
      try {
        let _ = Client::connect(config, group)
        false
      } catch {
        InvalidConfig(_) => true
        _ => false
      },
    )
    let auto = AutoClient::start(config, group)
    @async.with_timeout(1000, () => {
      while !(auto.status() is Failed) {
        @async.sleep(5)
      }
    })
    assert_true(auto.last_error() is Some(InvalidConfig(_)))
    auto.stop()
  }
}

///|
async test "temporary TCP loss during TLS handshake enters retry" {
  @async.with_task_group() <| group => {
    let server = @socket.TcpServer(@socket.Addr::parse("127.0.0.1:0"))
    defer server.close()
    group.spawn_bg() <| () => {
      let (conn, _) = server.accept()
      let header = conn.read_exactly(5)
      let length = header[3].to_int() * 256 + header[4].to_int()
      let _ = conn.read_exactly(length)
      conn.close()
    }
    let config = Config::new(
      "127.0.0.1",
      "tls-network-retry",
      port=server.addr.port(),
    )
    config.tls = Some(TlsConfig::new())
    config.timeout_ms = 1000
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
    auto.stop()
  }
}
