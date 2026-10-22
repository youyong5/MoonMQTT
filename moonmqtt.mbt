///|
/// Plain TCP MQTT 3.1.1 client. The caller owns the task group.
pub(all) struct Config {
  host : String
  port : Int32
  client_id : String
  mut username : String?
  mut password : Bytes?
  mut keep_alive_seconds : Int32
  mut timeout_ms : Int32
  mut max_packet_size : Int32
}

///|
pub fn Config::new(
  host : String,
  client_id : String,
  port~ : Int32 = 1883,
) -> Config {
  {
    host,
    port,
    client_id,
    username: None,
    password: None,
    keep_alive_seconds: 30,
    timeout_ms: 5000,
    max_packet_size: 1024 * 1024,
  }
}

///|
pub(all) struct Message {
  topic : String
  payload : Bytes
  qos : @mqtt.QoS
  retain : Bool
  dup : Bool
}

///|
pub suberror MqttError {
  InvalidConfig(String)
  Codec(String)
  Protocol(String)
  Rejected(@mqtt.ConnackReturnCode)
  Closed
  TimedOut
} derive(Debug)

///|
pub extend MqttError with @debug.Debug::{to_repr}

///|
pub struct Client {
  config : Config
  tcp : @socket.Tcp
  write_lock : @async.Mutex
  messages : @async.Queue[Message]
  pending : Map[Int32, @async.Queue[@mqtt.Packet]]
  ping_reply : @async.Queue[Unit]
  mut next_id : Int32
  mut closed : Bool
  mut last_error : Error?
  mut disposed : Bool
  mut reader_task : @async.Task[Unit]?
  mut heartbeat_task : @async.Task[Unit]?
}

///|
fn[X] encoded(result : Result[X, String]) -> X raise MqttError {
  match result {
    Ok(value) => value
    Err(reason) => raise Codec(reason)
  }
}

///|
fn validate(config : Config) -> Unit raise MqttError {
  if config.host == "" ||
    config.client_id == "" ||
    config.port < 1 ||
    config.port > 65535 ||
    config.keep_alive_seconds < 0 ||
    config.keep_alive_seconds > 65535 ||
    config.timeout_ms <= 0 ||
    config.max_packet_size < 4 ||
    config.max_packet_size > 268435460 {
    raise InvalidConfig(
      "invalid host, client ID, port, keep alive, timeout or packet limit",
    )
  }
  if config.password is Some(_) && config.username is None {
    raise InvalidConfig("password requires username")
  }
}

///|
fn Client::ensure_open(self : Client) -> Unit raise {
  if self.closed {
    match self.last_error {
      Some(e) => raise e
      None => raise Closed
    }
  }
}

///|
fn Client::finish(self : Client, reason : Error) -> Unit {
  if !self.closed {
    self.closed = true
    self.last_error = Some(reason)
    match self.reader_task {
      Some(task) => task.cancel()
      None => ()
    }
    match self.heartbeat_task {
      Some(task) => task.cancel()
      None => ()
    }
    self.messages.close(error=reason)
    self.ping_reply.close(error=reason)
    for _, queue in self.pending {
      queue.close(error=reason)
    }
    self.pending.clear()
  }
}

///|
pub async fn Client::close(self : Client) -> Unit {
  self.finish(Closed)
  if !self.disposed {
    self.disposed = true
    @async.protect_from_cancel(() => {
      match self.reader_task {
        Some(task) => task.wait() catch { _ => () }
        None => ()
      }
      match self.heartbeat_task {
        Some(task) => task.wait() catch { _ => () }
        None => ()
      }
      self.tcp.close()
    })
  }
}

///|
/// Read exactly one frame, including a variable length header and fragmented body.
async fn read_rest(
  tcp : @socket.Tcp,
  length : Int32,
  timeout_ms : Int32,
) -> Bytes {
  if timeout_ms == 0 {
    return tcp.read_exactly(length)
  }
  @async.with_timeout(
    timeout_ms,
    () => tcp.read_exactly(length),
    error=TimedOut,
  )
}

///|
async fn read_packet(
  tcp : @socket.Tcp,
  max_size : Int32,
  timeout_ms~ : Int32 = 0,
) -> Array[Byte] {
  let first = tcp.read_exactly(1)
  let packet : Array[Byte] = [first[0]]
  let mut multiplier = 1
  let mut remaining = 0
  let mut finished = false
  for _ in 0..<4 {
    let byte = read_rest(tcp, 1, timeout_ms)[0]
    packet.push(byte)
    remaining += (byte.to_int() & 127) * multiplier
    if (byte.to_int() & 128) == 0 {
      finished = true
      break
    }
    multiplier *= 128
  }
  if !finished {
    raise Protocol("remaining length exceeds four bytes")
  }
  if packet.length() + remaining > max_size {
    raise Protocol("packet exceeds configured limit")
  }
  for byte in read_rest(tcp, remaining, timeout_ms) {
    packet.push(byte)
  }
  packet
}

///|
async fn Client::send(self : Client, bytes : Array[Byte]) -> Unit {
  self.ensure_open()
  let outcome = try {
    @async.with_timeout(
      self.config.timeout_ms,
      () => {
        self.write_lock.acquire()
        defer self.write_lock.release()
        self.ensure_open()
        self.tcp.write(Bytes::from_array(bytes))
      },
      error=TimedOut,
    )
    Ok(())
  } catch {
    e => Err(e)
  }
  match outcome {
    Ok(_) => ()
    Err(e) => {
      self.finish(e)
      raise e
    }
  }
}

///|
fn Client::allocate_id(self : Client) -> Int32 raise MqttError {
  for _ in 0..<65535 {
    let id = self.next_id
    self.next_id = if id == 65535 { 1 } else { id + 1 }
    if !self.pending.contains(id) {
      return id
    }
  }
  raise Protocol("all packet identifiers are in use")
}

///|
fn Client::resolve(self : Client, id : Int32, packet : @mqtt.Packet) -> Unit {
  match self.pending.get(id) {
    Some(queue) => {
      let _ = queue.try_put(packet) catch { _ => false }
    }
    None => ()
  }
}

///|
async fn Client::reader(self : Client) -> Unit {
  while !self.closed {
    let bytes = read_packet(
      self.tcp,
      self.config.max_packet_size,
      timeout_ms=self.config.timeout_ms,
    ) catch {
      e => {
        self.finish(e)
        return
      }
    }
    let packet = match @mqtt.decode_packet(bytes) {
      Ok(value) => value
      Err(reason) => {
        self.finish(Protocol(reason))
        return
      }
    }
    match packet {
      PublishPacket(publish) => {
        if publish.qos == @mqtt.QoS2 {
          self.finish(Protocol("incoming QoS 2 is unsupported"))
          return
        }
        if publish.qos == @mqtt.QoS1 {
          let id = match publish.message_id {
            Some(id) => id
            None => {
              self.finish(Protocol("QoS 1 PUBLISH has no ID"))
              return
            }
          }
          self.send(encoded(@mqtt.encode_puback(id))) catch {
            e => {
              self.finish(e)
              return
            }
          }
        }
        self.messages.put({
          topic: publish.topic,
          payload: publish.payload,
          qos: publish.qos,
          retain: publish.retain,
          dup: publish.dup,
        }) catch {
          e => {
            self.finish(e)
            return
          }
        }
      }
      PubackPacket(id) => self.resolve(id.message_id, packet)
      SubackPacket(ack) => self.resolve(ack.message_id, packet)
      UnsubackPacket(ack) => self.resolve(ack.message_id, packet)
      PingrespPacket => {
        let _ = self.ping_reply.try_put(()) catch { _ => false }
      }
      _ => {
        self.finish(Protocol("unexpected server packet"))
        return
      }
    }
  }
}

///|
async fn Client::heartbeat(self : Client) -> Unit {
  let interval = self.config.keep_alive_seconds * 1000
  if interval == 0 {
    return
  }
  while !self.closed {
    @async.sleep(interval)
    if self.closed {
      return
    }
    self.send(encoded(@mqtt.encode_pingreq())) catch {
      e => {
        self.finish(e)
        return
      }
    }
    let reply = @async.with_timeout_opt(self.config.timeout_ms, () => {
      self.ping_reply.get()
    }) catch {
      e => {
        self.finish(e)
        return
      }
    }
    if reply is None {
      self.finish(TimedOut)
      return
    }
  }
}

///| Start one reader and one optional heartbeat task in `group`.

///|
/// The group must outlive every operation. Call disconnect or close.
pub async fn[X] Client::connect(
  config : Config,
  group : @async.TaskGroup[X],
) -> Client {
  validate(config)
  let tcp = @async.with_timeout(
    config.timeout_ms,
    () => @socket.Tcp::connect_to_host(config.host, port=config.port),
    error=TimedOut,
  )
  let client = {
    config,
    tcp,
    write_lock: @async.Mutex::Mutex(),
    messages: @async.Queue::Queue(kind=Unbounded),
    pending: Map([]),
    ping_reply: @async.Queue::Queue(kind=DiscardOldest(1)),
    next_id: 1,
    closed: false,
    last_error: None,
    disposed: false,
    reader_task: None,
    heartbeat_task: None,
  }
  let flags = @mqtt.ConnectFlags::{
    username: config.username is Some(_),
    password: config.password is Some(_),
    will_retain: false,
    will_qos: @mqtt.QoS0,
    will_flag: false,
    clean_session: true,
  }
  let packet = @mqtt.Connect::{
    client_id: config.client_id,
    username: config.username,
    password: config.password,
    will_topic: None,
    will_message: None,
    keep_alive: config.keep_alive_seconds,
    flags,
  }
  let handshake = try {
    client.send(encoded(@mqtt.encode_connect(packet)))
    let frame = @async.with_timeout(
      config.timeout_ms,
      () => read_packet(tcp, config.max_packet_size),
      error=TimedOut,
    )
    match @mqtt.decode_packet(frame) {
      Ok(ConnackPacket(ack)) => {
        if ack.return_code != @mqtt.ConnectionAccepted {
          raise Rejected(ack.return_code)
        }
        if ack.session_present {
          raise Protocol("clean session CONNACK has session present set")
        }
      }
      Ok(_) => raise Protocol("expected CONNACK")
      Err(reason) => raise Protocol(reason)
    }
    Ok(())
  } catch {
    e => Err(e)
  }
  match handshake {
    Ok(_) => ()
    Err(e) => {
      client.finish(e)
      client.close()
      raise e
    }
  }
  client.reader_task = Some(
    group.spawn(no_wait=true, allow_failure=true) <| () => { client.reader() },
  )
  client.heartbeat_task = Some(
    group.spawn(no_wait=true, allow_failure=true) <| () => {
      client.heartbeat()
    },
  )
  client
}

///|
/// Scoped convenience API. Closes the connection on exit.
pub async fn[X] with_client(config : Config, f : async (Client) -> X) -> X {
  @async.with_task_group(group => {
    let client = Client::connect(config, group)
    let outcome = Ok(f(client)) catch { e => Err(e) }
    match outcome {
      Ok(value) => {
        client.disconnect()
        value
      }
      Err(e) => {
        client.close()
        raise e
      }
    }
  })
}

///|
/// Receive the next publication or raise the terminal connection error.
pub async fn Client::receive(self : Client) -> Message {
  self.messages.get()
}

///|
async fn Client::request(
  self : Client,
  id : Int32,
  bytes : Array[Byte],
) -> @mqtt.Packet {
  let queue = @async.Queue::Queue(kind=Blocking(1))
  self.pending[id] = queue
  defer self.pending.remove(id)
  self.send(bytes)
  let answer = @async.with_timeout_opt(self.config.timeout_ms, () => queue.get())
  match answer {
    Some(packet) => packet
    None => {
      self.finish(TimedOut)
      raise TimedOut
    }
  }
}

///|
/// Subscribe to one filter at QoS 0 or 1. Returns the granted QoS.
pub async fn Client::subscribe(
  self : Client,
  filter : String,
  qos : @mqtt.QoS,
) -> @mqtt.QoS {
  self.ensure_open()
  if qos == @mqtt.QoS2 {
    raise InvalidConfig("QoS 2 is unsupported")
  }
  let id = self.allocate_id()
  let bytes = encoded(
    @mqtt.encode_subscribe({
      message_id: id,
      topics: [{ topic: filter, qos, }],
    }),
  )
  match self.request(id, bytes) {
    SubackPacket(ack) if ack.return_codes.length() == 1 =>
      match ack.return_codes[0] {
        SuccessQoS0 => @mqtt.QoS0
        SuccessQoS1 => @mqtt.QoS1
        SuccessQoS2 => raise Protocol("server granted unsupported QoS 2")
        Failure => raise Protocol("subscription rejected")
      }
    _ => raise Protocol("expected matching SUBACK")
  }
}

///|
pub async fn Client::unsubscribe(self : Client, filter : String) -> Unit {
  self.ensure_open()
  let id = self.allocate_id()
  let bytes = encoded(
    @mqtt.encode_unsubscribe({ message_id: id, topics: [filter], }),
  )
  match self.request(id, bytes) {
    UnsubackPacket(_) => ()
    _ => raise Protocol("expected matching UNSUBACK")
  }
}

///|
/// QoS 1 completes on the matching PUBACK; QoS 0 completes after TCP write.
pub async fn Client::publish(
  self : Client,
  topic : String,
  payload : Bytes,
  qos~ : @mqtt.QoS = @mqtt.QoS0,
  retain~ : Bool = false,
) -> Unit {
  self.ensure_open()
  if qos == @mqtt.QoS2 {
    raise InvalidConfig("QoS 2 is unsupported")
  }
  let id = if qos == @mqtt.QoS1 { Some(self.allocate_id()) } else { None }
  let bytes = encoded(
    @mqtt.encode_publish({
      topic,
      payload,
      qos,
      message_id: id,
      retain,
      dup: false,
    }),
  )
  match id {
    Some(id) =>
      match self.request(id, bytes) {
        PubackPacket(_) => ()
        _ => raise Protocol("expected matching PUBACK")
      }
    None => self.send(bytes)
  }
}

///|
/// Send DISCONNECT and close the TCP connection. Idempotent after closure.
pub async fn Client::disconnect(self : Client) -> Unit {
  if !self.closed {
    let sent = try {
      self.send(encoded(@mqtt.encode_disconnect()))
      Ok(())
    } catch {
      e => Err(e)
    }
    self.finish(Closed)
    self.close()
    match sent {
      Ok(_) => ()
      Err(e) => raise e
    }
  }
}
