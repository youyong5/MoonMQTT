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
  mut inbox_capacity : Int32
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
    inbox_capacity: 256,
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
  InboxFull
} derive(Debug)

///|
pub extend MqttError with @debug.Debug::{to_repr}

///|
enum PendingKind {
  Puback
  Suback
  Unsuback
  Pubrec
  Pubcomp
}

///|
struct Pending {
  mut kind : PendingKind
  reply : @async.Queue[@mqtt.Packet]
}

///|
pub struct Client {
  config : Config
  tcp : @socket.Tcp
  write_lock : @async.Mutex
  messages : @async.Queue[Message]
  pending : Map[Int32, Pending]
  incoming_qos2 : Map[Int32, Bool]
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
    config.max_packet_size > 268435460 ||
    config.inbox_capacity < 1 {
    raise InvalidConfig(
      "invalid host, client ID, port, keep alive, timeout or queue/packet limit",
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
    for _, pending in self.pending {
      pending.reply.close(error=reason)
    }
    self.pending.clear()
    self.incoming_qos2.clear()
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
fn Client::reserve(self : Client, kind : PendingKind) -> Int32 raise {
  self.ensure_open()
  let id = self.allocate_id()
  self.pending[id] = { kind, reply: @async.Queue::Queue(kind=Blocking(1)), }
  id
}

///|
fn Client::resolve(self : Client, id : Int32, packet : @mqtt.Packet) -> Unit {
  match self.pending.get(id) {
    Some(pending) => {
      let valid = match (pending.kind, packet) {
        (Puback, PubackPacket(_))
        | (Suback, SubackPacket(_))
        | (Unsuback, UnsubackPacket(_))
        | (Pubrec, PubrecPacket(_))
        | (Pubcomp, PubcompPacket(_)) => true
        _ => false
      }
      if !valid {
        self.finish(Protocol("acknowledgement type does not match packet ID"))
      } else {
        let queued = pending.reply.try_put(packet) catch { _ => false }
        if !queued && !(pending.kind is Pubrec) {
          self.finish(
            Protocol("duplicate acknowledgement before request advanced"),
          )
        }
      }
    }
    None => self.finish(Protocol("acknowledgement for unknown packet ID"))
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
        let id = publish.message_id
        let duplicate_qos2 = if publish.qos == @mqtt.QoS2 {
          match id {
            Some(id) => self.incoming_qos2.get(id) is Some(false)
            None => {
              self.finish(Protocol("QoS 2 PUBLISH has no ID"))
              return
            }
          }
        } else {
          false
        }
        if !duplicate_qos2 {
          let accepted = self.messages.try_put({
            topic: publish.topic,
            payload: publish.payload,
            qos: publish.qos,
            retain: publish.retain,
            dup: publish.dup,
          }) catch {
            _ => false
          }
          if !accepted {
            self.finish(InboxFull)
            return
          }
        }
        try {
          match publish.qos {
            @mqtt.QoS0 => ()
            @mqtt.QoS1 =>
              match id {
                Some(id) => self.send(encoded(@mqtt.encode_puback(id)))
                None => raise Protocol("QoS 1 PUBLISH has no ID")
              }
            @mqtt.QoS2 =>
              match id {
                Some(id) => {
                  if !duplicate_qos2 {
                    self.incoming_qos2[id] = false
                  }
                  self.send(encoded(@mqtt.encode_pubrec(id)))
                }
                None => raise Protocol("QoS 2 PUBLISH has no ID")
              }
          }
        } catch {
          e => {
            self.finish(e)
            return
          }
        }
      }
      PubackPacket(id) => self.resolve(id.message_id, packet)
      PubrecPacket(id) =>
        match self.pending.get(id.message_id) {
          Some(pending) if pending.kind is Pubcomp =>
            // A repeated PUBREC asks for PUBREL again, never PUBLISH.
            self.send(encoded(@mqtt.encode_pubrel(id.message_id))) catch {
              e => {
                self.finish(e)
                return
              }
            }
          _ => self.resolve(id.message_id, packet)
        }
      PubcompPacket(id) => self.resolve(id.message_id, packet)
      PubrelPacket(id) =>
        match self.incoming_qos2.get(id.message_id) {
          Some(_) => {
            self.incoming_qos2[id.message_id] = true
            self.send(encoded(@mqtt.encode_pubcomp(id.message_id))) catch {
              e => {
                self.finish(e)
                return
              }
            }
          }
          None =>
            // MQTT 3.1.1 requires PUBCOMP for every PUBREL, including a
            // duplicate whose completed state is no longer available.
            self.send(encoded(@mqtt.encode_pubcomp(id.message_id))) catch {
              e => {
                self.finish(e)
                return
              }
            }
        }
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
    messages: @async.Queue::Queue(kind=Blocking(config.inbox_capacity)),
    pending: Map([]),
    incoming_qos2: Map([]),
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
  kind : PendingKind,
  make_bytes : (Int32) -> Result[Array[Byte], String],
) -> @mqtt.Packet {
  let id = self.reserve(kind)
  defer self.pending.remove(id)
  let mut completed = false
  defer (if !completed { self.finish(Closed) })
  let result : Result[@mqtt.Packet, Error] = try {
    let bytes = encoded(make_bytes(id))
    self.send(bytes)
    let pending = self.pending.get(id).unwrap()
    match
      @async.with_timeout_opt(self.config.timeout_ms, () => pending.reply.get()) {
      Some(packet) => Ok(packet)
      None => Err(TimedOut)
    }
  } catch {
    e => Err(e)
  }
  match result {
    Ok(packet) => {
      completed = true
      packet
    }
    Err(e) => {
      self.finish(e)
      raise e
    }
  }
}

///|
/// Subscribe to one filter at QoS 0, 1 or 2. Returns the granted QoS.
pub async fn Client::subscribe(
  self : Client,
  filter : String,
  qos : @mqtt.QoS,
) -> @mqtt.QoS {
  self.ensure_open()
  match
    self.request(Suback, id => {
      @mqtt.encode_subscribe({
        message_id: id,
        topics: [{ topic: filter, qos, }],
      })
    }) {
    SubackPacket(ack) if ack.return_codes.length() == 1 =>
      match ack.return_codes[0] {
        SuccessQoS0 => @mqtt.QoS0
        SuccessQoS1 => @mqtt.QoS1
        SuccessQoS2 => @mqtt.QoS2
        Failure => raise Protocol("subscription rejected")
      }
    _ => raise Protocol("expected matching SUBACK")
  }
}

///|
pub async fn Client::unsubscribe(self : Client, filter : String) -> Unit {
  self.ensure_open()
  match
    self.request(Unsuback, id => {
      @mqtt.encode_unsubscribe({ message_id: id, topics: [filter], })
    }) {
    UnsubackPacket(_) => ()
    _ => raise Protocol("expected matching UNSUBACK")
  }
}

///|
/// QoS 1 completes on PUBACK; QoS 2 completes on PUBCOMP.
pub async fn Client::publish(
  self : Client,
  topic : String,
  payload : Bytes,
  qos~ : @mqtt.QoS = @mqtt.QoS0,
  retain~ : Bool = false,
) -> Unit {
  self.ensure_open()
  if qos == @mqtt.QoS0 {
    self.send(
      encoded(
        @mqtt.encode_publish({
          topic,
          payload,
          qos,
          message_id: None,
          retain,
          dup: false,
        }),
      ),
    )
    return
  }
  let kind = if qos == @mqtt.QoS1 { Puback } else { Pubrec }
  let id = self.reserve(kind)
  defer self.pending.remove(id)
  let mut completed = false
  defer (if !completed { self.finish(Closed) })
  let result = try {
    let bytes = encoded(
      @mqtt.encode_publish({
        topic,
        payload,
        qos,
        message_id: Some(id),
        retain,
        dup: false,
      }),
    )
    self.send(bytes)
    let pending = self.pending.get(id).unwrap()
    let first = @async.with_timeout_opt(self.config.timeout_ms, () => {
      pending.reply.get()
    })
    match (qos, first) {
      (@mqtt.QoS1, Some(PubackPacket(_))) => ()
      (@mqtt.QoS2, Some(PubrecPacket(_))) => {
        pending.kind = Pubcomp
        self.send(encoded(@mqtt.encode_pubrel(id)))
        let final_ack = @async.with_timeout_opt(self.config.timeout_ms, () => {
          pending.reply.get()
        })
        match final_ack {
          Some(PubcompPacket(_)) => ()
          None => raise TimedOut
          _ => raise Protocol("expected PUBCOMP")
        }
      }
      (_, None) => raise TimedOut
      _ => raise Protocol("unexpected publish acknowledgement")
    }
    Ok(())
  } catch {
    e => Err(e)
  }
  match result {
    Ok(_) => completed = true
    Err(e) => {
      self.finish(e)
      raise e
    }
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
  } else {
    self.close()
  }
}
