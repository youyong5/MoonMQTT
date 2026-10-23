///|
/// An application supplied durable store for one stable MQTT client ID.
/// `load` returns None only when no snapshot has ever been written.
pub struct SessionStore {
  load_data : async () -> Bytes?
  save_data : async (Bytes) -> Unit
}

///|
pub fn SessionStore::new(
  load : async () -> Bytes?,
  save : async (Bytes) -> Unit,
) -> SessionStore {
  { load_data: load, save_data: save, }
}

///|
/// Native file store. The temporary file must be on the same filesystem.
pub fn SessionStore::file(path : String) -> SessionStore {
  SessionStore::new(
    () => {
      if !@fs.exists(path) {
        return None
      }
      Some(@fs.read_file(path).binary())
    },
    bytes => {
      let temporary = path + ".tmp"
      let file = @fs.open(
        temporary,
        mode=WriteOnly,
        create_mode=CreateOrTruncate,
      )
      let outcome = try {
        file.write(bytes)
        file.sync()
        Ok(())
      } catch {
        e => Err(e)
      }
      file.close()
      match outcome {
        Ok(_) => @fs.rename(temporary, path)
        Err(e) => raise e
      }
    },
  )
}

///|
fn put_u32(out : Array[Byte], value : Int32) -> Unit {
  out.push((value & 255).to_byte())
  out.push(((value >> 8) & 255).to_byte())
  out.push(((value >> 16) & 255).to_byte())
  out.push(((value >> 24) & 255).to_byte())
}

///|
fn put_bytes(out : Array[Byte], value : Bytes) -> Unit {
  put_u32(out, value.length())
  for byte in value {
    out.push(byte)
  }
}

///|
fn put_string(out : Array[Byte], value : String) -> Unit {
  put_bytes(out, @utf8.encode(value))
}

///|
fn put_bool(out : Array[Byte], value : Bool) -> Unit {
  out.push(if value { 1 } else { 0 })
}

///|
fn qos_number(qos : @mqtt.QoS) -> Int32 {
  match qos {
    @mqtt.QoS0 => 0
    @mqtt.QoS1 => 1
    @mqtt.QoS2 => 2
  }
}

///|
fn checksum(data : Bytes, end : Int32) -> Int32 {
  let mut a = 1
  let mut b = 0
  for i in 0..<end {
    a = (a + data[i].to_int()) % 65521
    b = (b + a) % 65521
  }
  (b << 16) | a
}

///|
fn encode_session(session : Session) -> Bytes raise MqttError {
  let out : Array[Byte] = [0x4d, 0x4d, 0x51, 0x53, 3]
  put_string(out, session.client_id)
  put_u32(out, session.next_id)
  put_bool(out, session.ever_connected)
  put_bool(out, session.connecting)
  put_bool(out, session.resubscribe_needed)
  put_u32(out, session.order.length())
  for id in session.order {
    let flow = match session.outgoing.get(id) {
      Some(value) => value
      None => raise CorruptSession("outgoing order has missing ID")
    }
    put_u32(out, id)
    put_string(out, flow.topic)
    put_bytes(out, flow.payload)
    out.push(qos_number(flow.qos).to_byte())
    put_bool(out, flow.retain)
    out.push(
      match flow.stage {
        WaitPuback => 1
        WaitPubrec => 2
        WaitPubcomp => 3
      },
    )
  }
  put_u32(out, session.incoming_qos2.length())
  for id, complete in session.incoming_qos2 {
    put_u32(out, id)
    put_bool(out, complete)
  }
  put_u32(out, session.subscriptions.length())
  for filter, qos in session.subscriptions {
    put_string(out, filter)
    out.push(qos_number(qos).to_byte())
  }
  put_u32(out, session.backlog.length())
  for message in session.backlog {
    put_string(out, message.topic)
    put_bytes(out, message.payload)
    out.push(qos_number(message.qos).to_byte())
    put_bool(out, message.retain)
    put_bool(out, message.dup)
  }
  if out.length() > 64 * 1024 * 1024 - 4 {
    raise Storage("session snapshot exceeds 64 MiB")
  }
  let body = Bytes::from_array(out)
  put_u32(out, checksum(body, body.length()))
  Bytes::from_array(out)
}

///|
priv struct SnapshotReader {
  data : Bytes
  mut pos : Int32
  end : Int32
}

///|
fn SnapshotReader::byte(self : SnapshotReader) -> Byte raise MqttError {
  if self.pos >= self.end {
    raise CorruptSession("truncated snapshot")
  }
  let value = self.data[self.pos]
  self.pos += 1
  value
}

///|
fn SnapshotReader::u32(self : SnapshotReader) -> Int32 raise MqttError {
  let a = self.byte().to_int()
  let b = self.byte().to_int()
  let c = self.byte().to_int()
  let d = self.byte().to_int()
  a | (b << 8) | (c << 16) | (d << 24)
}

///|
fn SnapshotReader::blob(self : SnapshotReader) -> Bytes raise MqttError {
  let length = self.u32()
  if length < 0 || length > self.end - self.pos {
    raise CorruptSession("invalid snapshot field length")
  }
  let bytes : Array[Byte] = []
  for _ in 0..<length {
    bytes.push(self.byte())
  }
  Bytes::from_array(bytes)
}

///|
fn SnapshotReader::string(self : SnapshotReader) -> String raise MqttError {
  @utf8.decode(self.blob()) catch {
    _ => raise CorruptSession("invalid UTF-8 in snapshot")
  }
}

///|
fn SnapshotReader::boolean(self : SnapshotReader) -> Bool raise MqttError {
  match self.byte().to_int() {
    0 => false
    1 => true
    _ => raise CorruptSession("invalid boolean")
  }
}

///|
fn SnapshotReader::qos(self : SnapshotReader) -> @mqtt.QoS raise MqttError {
  match self.byte().to_int() {
    0 => @mqtt.QoS0
    1 => @mqtt.QoS1
    2 => @mqtt.QoS2
    _ => raise CorruptSession("invalid QoS")
  }
}

///|
fn SnapshotReader::count(
  self : SnapshotReader,
  limit : Int32,
) -> Int32 raise MqttError {
  let n = self.u32()
  if n < 0 || n > limit {
    raise CorruptSession("invalid snapshot count")
  }
  n
}

///|
fn decode_session(
  data : Bytes,
  client_id : String,
  store : SessionStore,
) -> Session raise MqttError {
  if data.length() < 9 || data.length() > 64 * 1024 * 1024 {
    raise CorruptSession("invalid snapshot size")
  }
  if data[0] != 0x4d || data[1] != 0x4d || data[2] != 0x51 || data[3] != 0x53 {
    raise CorruptSession("invalid snapshot magic")
  }
  let version = data[4].to_int()
  if version != 3 {
    raise IncompatibleSessionVersion(version)
  }
  let end = data.length() - 4
  let reader = { data, pos: 5, end, }
  let stored_id = reader.string()
  if stored_id != client_id {
    raise SessionClientIdMismatch(stored_id)
  }
  let expected = checksum(data, end)
  let actual = data[end].to_int() |
    (data[end + 1].to_int() << 8) |
    (data[end + 2].to_int() << 16) |
    (data[end + 3].to_int() << 24)
  if expected != actual {
    raise CorruptSession("snapshot checksum mismatch")
  }
  let session = Session::new(client_id, Some(store))
  session.next_id = reader.u32()
  if session.next_id < 1 || session.next_id > 65535 {
    raise CorruptSession("invalid next packet ID")
  }
  session.ever_connected = reader.boolean()
  session.connecting = reader.boolean()
  session.resubscribe_needed = reader.boolean()
  session.loaded_uncertain = session.connecting
  let outgoing_count = reader.count(65535)
  for _ in 0..<outgoing_count {
    let id = reader.u32()
    if id < 1 || id > 65535 || session.outgoing.contains(id) {
      raise CorruptSession("invalid or repeated outgoing packet ID")
    }
    let topic = reader.string()
    let payload = reader.blob()
    let qos = reader.qos()
    let retain = reader.boolean()
    let stage = match reader.byte().to_int() {
      1 if qos == @mqtt.QoS1 => WaitPuback
      2 if qos == @mqtt.QoS2 => WaitPubrec
      3 if qos == @mqtt.QoS2 => WaitPubcomp
      _ => raise CorruptSession("invalid outgoing QoS stage")
    }
    if topic == "" {
      raise CorruptSession("empty outgoing topic")
    }
    session.outgoing[id] = {
      topic,
      payload,
      qos,
      retain,
      stage,
      pubrel_sent: false,
    }
    session.order.push(id)
  }
  let incoming_count = reader.count(65535)
  for _ in 0..<incoming_count {
    let id = reader.u32()
    if id < 1 || id > 65535 || session.incoming_qos2.contains(id) {
      raise CorruptSession("invalid or repeated incoming packet ID")
    }
    session.incoming_qos2[id] = reader.boolean()
  }
  let subscription_count = reader.count(65535)
  for _ in 0..<subscription_count {
    let filter = reader.string()
    if filter == "" || session.subscriptions.contains(filter) {
      raise CorruptSession("invalid or repeated subscription")
    }
    session.subscriptions[filter] = reader.qos()
  }
  let message_count = reader.count(65535)
  for _ in 0..<message_count {
    let topic = reader.string()
    let payload = reader.blob()
    let qos = reader.qos()
    let retain = reader.boolean()
    let dup = reader.boolean()
    if topic == "" {
      raise CorruptSession("empty queued topic")
    }
    session.backlog.push({ topic, payload, qos, retain, dup, })
  }
  if reader.pos != end {
    raise CorruptSession("trailing snapshot bytes")
  }
  session
}

///|
async fn Session::persist(self : Session) -> Unit {
  match self.store {
    None => ()
    Some(store) =>
      @async.protect_from_cancel(() => {
        self.persist_lock.acquire()
        defer self.persist_lock.release()
        if self.storage_failed {
          raise Storage("session store failed; reload the durable snapshot")
        }
        errdefer {
          self.storage_failed = true
        }
        let bytes = encode_session(self)
        (store.save_data)(bytes) catch {
          _ => raise Storage("session store write failed")
        }
      })
  }
}
