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
  mut clean_session : Bool
  mut session_store : SessionStore?
  mut will : Will?
  mut tls : TlsConfig?
}

///|
/// TLS verifies the broker certificate and config.host by default.
/// A custom PEM CA replaces the system root set.
pub(all) struct TlsConfig {
  ca_file : String?
}

///|
pub fn TlsConfig::new(ca_file? : String) -> TlsConfig {
  { ca_file, }
}

///|
/// Last Will associated with each accepted MQTT network connection.
pub(all) struct Will {
  topic : String
  payload : Bytes
  qos : @mqtt.QoS
  retain : Bool
}

///|
pub fn Will::new(
  topic : String,
  payload : Bytes,
  qos~ : @mqtt.QoS = @mqtt.QoS0,
  retain~ : Bool = false,
) -> Will {
  { topic, payload, qos, retain, }
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
    clean_session: true,
    session_store: None,
    will: None,
    tls: None,
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
  Storage(String)
  CorruptSession(String)
  IncompatibleSessionVersion(Int32)
  SessionClientIdMismatch(String)
  RecoveryUncertain
  DisconnectUncertain
  TlsValidation(String)
} derive(Debug)

///|
pub extend MqttError with @debug.Debug::{to_repr}

///|
priv enum PendingKind {
  Puback
  Suback
  Unsuback
  Pubrec
  Pubcomp
}

///|
struct Pending {
  mut kind : PendingKind
  mut ack_queued : Bool
  reply : @async.Queue[@mqtt.Packet]
}

///|
priv enum OutStage {
  WaitPuback
  WaitPubrec
  WaitPubcomp
}

///|
priv struct Outgoing {
  topic : String
  payload : Bytes
  qos : @mqtt.QoS
  retain : Bool
  mut stage : OutStage
  mut pubrel_sent : Bool
}

///|
struct Session {
  client_id : String
  store : SessionStore?
  persist_lock : @async.Mutex
  mut next_id : Int32
  outgoing : Map[Int32, Outgoing]
  mut order : Array[Int32]
  incoming_qos2 : Map[Int32, Bool]
  subscriptions : Map[String, @mqtt.QoS]
  mut backlog : Array[Message]
  mut ever_connected : Bool
  mut connecting : Bool
  mut resubscribe_needed : Bool
  mut loaded_uncertain : Bool
  mut storage_failed : Bool
  mut active : Bool
}

///|
fn Session::new(client_id : String, store : SessionStore?) -> Session {
  {
    client_id,
    store,
    persist_lock: @async.Mutex::Mutex(),
    next_id: 1,
    outgoing: Map([]),
    order: [],
    incoming_qos2: Map([]),
    subscriptions: Map([]),
    backlog: [],
    ever_connected: false,
    connecting: false,
    resubscribe_needed: false,
    loaded_uncertain: false,
    storage_failed: false,
    active: false,
  }
}

///|
fn Session::forget_outgoing(self : Session, id : Int32) -> Unit {
  self.outgoing.remove(id)
  let remaining : Array[Int32] = []
  for old_id in self.order {
    if old_id != id {
      remaining.push(old_id)
    }
  }
  self.order = remaining
}

///|
pub struct Client {
  config : Config
  session : Session
  tcp : @socket.Tcp
  tls : @tls.Tls?
  write_lock : @async.Mutex
  receive_lock : @async.Mutex
  messages : @async.Queue[Message]
  pending : Map[Int32, Pending]
  ping_reply : @async.Queue[Unit]
  mut session_present : Bool
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
  if config.clean_session && config.session_store is Some(_) {
    raise InvalidConfig("session store requires clean_session=false")
  }
  if config.tls is Some({ ca_file: Some(""), }) {
    raise InvalidConfig("TLS CA file path is empty")
  }
  match config.will {
    Some(will) => {
      if will.topic == "" ||
        will.topic.contains("#") ||
        will.topic.contains("+") ||
        @mqtt.encode_string(will.topic) is Err(_) {
        raise InvalidConfig("will topic must be a valid MQTT topic name")
      }
      if will.payload.length() > 65535 {
        raise InvalidConfig("will payload exceeds 65535 bytes")
      }
    }
    None => ()
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
async fn Client::save_session(self : Client) -> Unit {
  errdefer self.finish(Closed)
  let saved : Result[Unit, Error] = try {
    self.session.persist()
    Ok(())
  } catch {
    e => Err(e)
  }
  match saved {
    Ok(_) => self.ensure_open()
    Err(e) => {
      self.finish(e)
      raise e
    }
  }
}

///|
fn Client::finish(self : Client, reason : Error) -> Unit {
  if !self.closed {
    self.closed = true
    self.last_error = Some(reason)
    self.session.active = false
    match self.reader_task {
      Some(task) => task.cancel()
      None => ()
    }
    match self.heartbeat_task {
      Some(task) => task.cancel()
      None => ()
    }
    if self.config.clean_session {
      self.session.outgoing.clear()
      self.session.order = []
      self.session.incoming_qos2.clear()
      self.session.subscriptions.clear()
      self.session.backlog = []
    } else if self.session.store is None {
      while true {
        let next = self.messages.try_get() catch { _ => None }
        match next {
          Some(message) => self.session.backlog.push(message)
          None => break
        }
      }
    }
    self.messages.close(error=reason, clear=true)
    self.ping_reply.close(error=reason, clear=true)
    for _, pending in self.pending {
      pending.reply.close(error=reason, clear=true)
    }
    self.pending.clear()
  }
}

///|
pub async fn Client::close(self : Client) -> Unit {
  self.close_with_tls_close(() => {
    match self.tls {
      Some(tls) => tls.close()
      None => ()
    }
    Ok(())
  })
}

///|
/// Kept separate so a failing TLS close can be injected in a transport test.
async fn Client::close_with_tls_close(
  self : Client,
  close_tls : () -> Result[Unit, Error],
) -> Unit {
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
      let tls_result = close_tls()
      self.tcp.close()
      match tls_result {
        Err(e) => raise e
        Ok(_) => ()
      }
    })
  }
}

///|
/// Read exactly one frame, including a variable length header and fragmented body.
async fn[R : @io.Reader] read_rest(
  reader : R,
  length : Int32,
  timeout_ms : Int32,
) -> Bytes {
  if timeout_ms == 0 {
    return reader.read_exactly(length)
  }
  @async.with_timeout(
    timeout_ms,
    () => reader.read_exactly(length),
    error=TimedOut,
  )
}

///|
async fn[R : @io.Reader] read_packet(
  reader : R,
  max_size : Int32,
  timeout_ms~ : Int32 = 0,
) -> Array[Byte] {
  let first = reader.read_exactly(1)
  let packet : Array[Byte] = [first[0]]
  let mut multiplier = 1
  let mut remaining = 0
  let mut finished = false
  for _ in 0..<4 {
    let byte = read_rest(reader, 1, timeout_ms)[0]
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
  for byte in read_rest(reader, remaining, timeout_ms) {
    packet.push(byte)
  }
  packet
}

///|
async fn Client::read_packet(
  self : Client,
  timeout_ms~ : Int32 = 0,
) -> Array[Byte] {
  match self.tls {
    Some(tls) => read_packet(tls, self.config.max_packet_size, timeout_ms~)
    None => read_packet(self.tcp, self.config.max_packet_size, timeout_ms~)
  }
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
        match self.tls {
          Some(tls) => tls.write(Bytes::from_array(bytes))
          None => self.tcp.write(Bytes::from_array(bytes))
        }
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
    let id = self.session.next_id
    self.session.next_id = if id == 65535 { 1 } else { id + 1 }
    if !self.pending.contains(id) && !self.session.outgoing.contains(id) {
      return id
    }
  }
  raise Protocol("all packet identifiers are in use")
}

///|
fn Client::reserve(self : Client, kind : PendingKind) -> Int32 raise {
  self.ensure_open()
  let id = self.allocate_id()
  self.pending[id] = {
    kind,
    ack_queued: false,
    reply: @async.Queue::Queue(kind=Blocking(1)),
  }
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
      } else if pending.kind is Pubrec && pending.ack_queued {
        // The first PUBREC may already be delivered to a waiting task even
        // though that task has not advanced its state yet.
        ()
      } else {
        pending.ack_queued = true
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
    let bytes = self.read_packet(timeout_ms=self.config.timeout_ms) catch {
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
            Some(id) => self.session.incoming_qos2.get(id) is Some(false)
            None => {
              self.finish(Protocol("QoS 2 PUBLISH has no ID"))
              return
            }
          }
        } else {
          false
        }
        if !duplicate_qos2 {
          let message = {
            topic: publish.topic,
            payload: publish.payload,
            qos: publish.qos,
            retain: publish.retain,
            dup: publish.dup,
          }
          let durable = self.session.store is Some(_)
          if durable {
            if self.session.backlog.length() >= self.config.inbox_capacity {
              self.finish(InboxFull)
              return
            }
            self.session.backlog.push(message)
            if publish.qos == @mqtt.QoS2 {
              self.session.incoming_qos2[id.unwrap()] = false
            }
            self.save_session() catch {
              _ => return
            }
          }
          let accepted = self.messages.try_put(message) catch { _ => false }
          if !accepted {
            self.finish(InboxFull)
            return
          }
          if !durable && publish.qos == @mqtt.QoS2 {
            self.session.incoming_qos2[id.unwrap()] = false
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
                Some(id) => self.send(encoded(@mqtt.encode_pubrec(id)))
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
      PubackPacket(id) =>
        match self.session.outgoing.get(id.message_id) {
          Some(flow) if flow.stage is WaitPuback => {
            self.session.forget_outgoing(id.message_id)
            self.save_session() catch {
              _ => return
            }
            if self.pending.contains(id.message_id) {
              self.resolve(id.message_id, packet)
            }
          }
          _ => self.finish(Protocol("unexpected PUBACK ID or stage"))
        }
      PubrecPacket(id) =>
        match self.session.outgoing.get(id.message_id) {
          Some(flow) if flow.stage is WaitPubrec => {
            flow.stage = WaitPubcomp
            self.save_session() catch {
              _ => return
            }
            if self.pending.contains(id.message_id) {
              self.resolve(id.message_id, packet)
            } else {
              flow.pubrel_sent = true
              self.send(encoded(@mqtt.encode_pubrel(id.message_id))) catch {
                e => {
                  self.finish(e)
                  return
                }
              }
            }
          }
          Some(flow) if flow.stage is WaitPubcomp =>
            if flow.pubrel_sent {
              // Once PUBREL was attempted, duplicate PUBREC repeats PUBREL.
              self.send(encoded(@mqtt.encode_pubrel(id.message_id))) catch {
                e => {
                  self.finish(e)
                  return
                }
              }
            }
          _ => self.finish(Protocol("unexpected PUBREC ID or stage"))
        }
      PubcompPacket(id) =>
        match self.session.outgoing.get(id.message_id) {
          Some(flow) if flow.stage is WaitPubcomp && flow.pubrel_sent => {
            self.session.forget_outgoing(id.message_id)
            self.save_session() catch {
              _ => return
            }
            if self.pending.contains(id.message_id) {
              self.resolve(id.message_id, packet)
            }
          }
          _ => self.finish(Protocol("unexpected PUBCOMP ID or stage"))
        }
      PubrelPacket(id) =>
        match self.session.incoming_qos2.get(id.message_id) {
          Some(_) => {
            self.session.incoming_qos2[id.message_id] = true
            self.save_session() catch {
              _ => return
            }
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

///|
/// Replay in original publication order. MQTT 3.1.1 retransmits PUBREL
/// without changing its reserved fixed-header flags.
async fn Client::replay_outgoing(self : Client) -> Unit {
  for id in self.session.order {
    match self.session.outgoing.get(id) {
      Some(flow) =>
        match flow.stage {
          WaitPuback | WaitPubrec =>
            self.send(
              encoded(
                @mqtt.encode_publish({
                  topic: flow.topic,
                  payload: flow.payload,
                  qos: flow.qos,
                  message_id: Some(id),
                  retain: flow.retain,
                  dup: true,
                }),
              ),
            )
          WaitPubcomp => {
            flow.pubrel_sent = true
            self.send(encoded(@mqtt.encode_pubrel(id)))
          }
        }
      None => ()
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
  connect_with_session(config, load_session(config), group)
}

///|
async fn load_session(config : Config) -> Session {
  match config.session_store {
    None => Session::new(config.client_id, None)
    Some(store) => {
      let loaded = (store.load_data)() catch {
        _ => raise Storage("session store read failed")
      }
      match loaded {
        None => Session::new(config.client_id, Some(store))
        Some(data) => decode_session(data, config.client_id, store)
      }
    }
  }
}

///|
/// Reconnect a persistent session using the same client ID and in-memory state.
pub async fn[X] Client::reconnect(
  self : Client,
  group : @async.TaskGroup[X],
) -> Client {
  if self.config.clean_session {
    raise InvalidConfig("reconnect requires clean_session=false")
  }
  self.close()
  if self.session.storage_failed {
    raise Storage(
      "storage failed; create a new Client from the durable snapshot",
    )
  }
  connect_with_session(self.config, self.session, group)
}

///|
/// Reports the Session Present bit received on this connection's CONNACK.
pub fn Client::session_present(self : Client) -> Bool {
  self.session_present
}

///|
async fn[X] connect_with_session(
  config : Config,
  session : Session,
  group : @async.TaskGroup[X],
  on_mqtt_connect~ : (() -> Unit)? = None,
) -> Client {
  validate(config)
  if session.client_id != config.client_id {
    raise InvalidConfig("persistent session client ID changed")
  }
  if session.active {
    raise InvalidConfig("session is already connected or reconnecting")
  }
  match config.tls {
    Some({ ca_file: Some(path), }) => {
      let _ = @fs.read_file(path) catch {
        _ => raise InvalidConfig("TLS CA file cannot be read")
      }
    }
    _ => ()
  }
  session.active = true
  errdefer {
    session.active = false
  }
  let tcp = @async.with_timeout(
    config.timeout_ms,
    () => @socket.Tcp::connect_to_host(config.host, port=config.port),
    error=TimedOut,
  )
  let mut tcp_owned_by_client = false
  errdefer (if !tcp_owned_by_client { tcp.close() })
  let tls = match config.tls {
    Some(options) => {
      let trust = match options.ca_file {
        Some(path) => @tls.CustomPemFile(path)
        None => @tls.SystemRoot
      }
      let secured = @async.with_timeout(
        config.timeout_ms,
        () => @tls.Tls::client(tcp, host=config.host, trust~),
        error=TimedOut,
      ) catch {
        @tls.TlsError(reason) => raise TlsValidation(reason)
        e => raise e
      }
      Some(secured)
    }
    None => None
  }
  let client = {
    config,
    session,
    tcp,
    tls,
    write_lock: @async.Mutex::Mutex(),
    receive_lock: @async.Mutex::Mutex(),
    messages: @async.Queue::Queue(kind=Blocking(config.inbox_capacity)),
    pending: Map([]),
    ping_reply: @async.Queue::Queue(kind=DiscardOldest(1)),
    session_present: false,
    closed: false,
    last_error: None,
    disposed: false,
    reader_task: None,
    heartbeat_task: None,
  }
  tcp_owned_by_client = true
  errdefer @async.protect_from_cancel(() => client.close())
  if !config.clean_session && session.store is Some(_) {
    session.connecting = true
    client.save_session()
  }
  let (will_retain, will_qos, will_topic, will_message) = match config.will {
    Some(will) => (will.retain, will.qos, Some(will.topic), Some(will.payload))
    None => (false, @mqtt.QoS0, None, None)
  }
  let flags = @mqtt.ConnectFlags::{
    username: config.username is Some(_),
    password: config.password is Some(_),
    will_retain,
    will_qos,
    will_flag: config.will is Some(_),
    clean_session: config.clean_session,
  }
  let packet = @mqtt.Connect::{
    client_id: config.client_id,
    username: config.username,
    password: config.password,
    will_topic,
    will_message,
    keep_alive: config.keep_alive_seconds,
    flags,
  }
  let handshake : Result[Bool, Error] = try {
    match on_mqtt_connect {
      Some(callback) => callback()
      None => ()
    }
    client.send(encoded(@mqtt.encode_connect(packet)))
    let frame = @async.with_timeout(
      config.timeout_ms,
      () => client.read_packet(),
      error=TimedOut,
    )
    match @mqtt.decode_packet(frame) {
      Ok(ConnackPacket(ack)) => {
        if ack.return_code != @mqtt.ConnectionAccepted {
          raise Rejected(ack.return_code)
        }
        if config.clean_session && ack.session_present {
          raise Protocol("clean session CONNACK has session present set")
        }
        if !config.clean_session &&
          !session.ever_connected &&
          ack.session_present {
          raise Protocol(
            "server session exists but local session state is unavailable",
          )
        }
        if !config.clean_session &&
          session.connecting &&
          ack.session_present &&
          session.store is Some(_) &&
          session.loaded_uncertain {
          raise RecoveryUncertain
        }
        Ok(ack.session_present)
      }
      Ok(_) => raise Protocol("expected CONNACK")
      Err(reason) => raise Protocol(reason)
    }
  } catch {
    e => Err(e)
  }
  match handshake {
    Ok(present) => {
      client.session_present = present
      if !config.clean_session {
        if present {
          if session.backlog.length() > config.inbox_capacity {
            client.finish(InboxFull)
            client.close()
            raise InboxFull
          }
          for message in session.backlog {
            let _ = client.messages.try_put(message) catch { _ => false }
          }
          if session.store is None {
            session.backlog = []
          }
        } else {
          session.outgoing.clear()
          session.order = []
          session.incoming_qos2.clear()
          session.backlog = []
          session.next_id = 1
          session.resubscribe_needed = !session.subscriptions.is_empty()
        }
      }
      session.ever_connected = true
      if !config.clean_session && session.store is Some(_) {
        session.connecting = false
        client.save_session()
        session.loaded_uncertain = false
      }
    }
    Err(e) => raise e
  }
  client.reader_task = Some(
    group.spawn(no_wait=true, allow_failure=true) <| () => { client.reader() },
  )
  client.heartbeat_task = Some(
    group.spawn(no_wait=true, allow_failure=true) <| () => {
      client.heartbeat()
    },
  )
  let recovered = try {
    if !config.clean_session {
      if client.session_present {
        client.replay_outgoing()
      }
      if !client.session_present || session.resubscribe_needed {
        let filters : Array[(String, @mqtt.QoS)] = []
        for filter, qos in session.subscriptions {
          filters.push((filter, qos))
        }
        for (filter, qos) in filters {
          let _ = client.subscribe(filter, qos)
        }
        if session.resubscribe_needed {
          session.resubscribe_needed = false
          client.save_session()
        }
      }
    }
    Ok(())
  } catch {
    e => Err(e)
  }
  match recovered {
    Ok(_) => ()
    Err(e) => raise e
  }
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
  self.receive_lock.acquire()
  defer self.receive_lock.release()
  let message = self.messages.get()
  if self.session.store is Some(_) {
    if self.session.backlog.is_empty() {
      self.finish(CorruptSession("message queue and snapshot diverged"))
      raise CorruptSession("message queue and snapshot diverged")
    }
    let _ = self.session.backlog.remove(0)
    self.save_session()
  }
  message
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
    self.save_session()
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
  let granted = match
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
  // Restore the requested maximum QoS, not a broker's previous grant.
  self.session.subscriptions[filter] = qos
  self.save_session()
  granted
}

///|
pub async fn Client::unsubscribe(self : Client, filter : String) -> Unit {
  self.ensure_open()
  match
    self.request(Unsuback, id => {
      @mqtt.encode_unsubscribe({ message_id: id, topics: [filter], })
    }) {
    UnsubackPacket(_) => {
      self.session.subscriptions.remove(filter)
      self.save_session()
    }
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
    self.session.outgoing[id] = {
      topic,
      payload,
      qos,
      retain,
      stage: if qos == @mqtt.QoS1 {
        WaitPuback
      } else {
        WaitPubrec
      },
      pubrel_sent: false,
    }
    self.session.order.push(id)
    self.save_session()
    self.send(bytes)
    let pending = self.pending.get(id).unwrap()
    let first = @async.with_timeout_opt(self.config.timeout_ms, () => {
      pending.reply.get()
    })
    self.ensure_open()
    match (qos, first) {
      (@mqtt.QoS1, Some(PubackPacket(_))) => ()
      (@mqtt.QoS2, Some(PubrecPacket(_))) => {
        pending.kind = Pubcomp
        let flow = self.session.outgoing.get(id).unwrap()
        flow.stage = WaitPubcomp
        flow.pubrel_sent = true
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
