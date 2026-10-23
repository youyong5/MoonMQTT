///|
/// Delay doubles after each failed connection, up to max_delay_ms.
pub(all) struct ReconnectPolicy {
  initial_delay_ms : Int32
  max_delay_ms : Int32
}

///|
pub fn ReconnectPolicy::new(
  initial_delay_ms~ : Int32 = 100,
  max_delay_ms~ : Int32 = 30000,
) -> ReconnectPolicy {
  { initial_delay_ms, max_delay_ms, }
}

///|
/// Connected carries the current CONNACK Session Present bit.
/// Retrying carries the consecutive failure count and next delay in ms.
pub(all) enum AutoStatus {
  Connecting
  Connected(Bool)
  Retrying(Int32, Int32)
  Failed
  Stopped
} derive(Debug)

///|
pub extend AutoStatus with @debug.Debug::{to_repr}

///|
/// A stable handle whose underlying Client is replaced after network failure.
pub struct AutoClient {
  config : Config
  policy : ReconnectPolicy
  session : Session
  mut current : Client?
  mut status : AutoStatus
  mut last_error : Error?
  mut stopped : Bool
  mut will_connect_attempted : Bool
  mut earlier_will_uncertain : Bool
  mut task : @async.Task[Unit]?
  receive_lock : @async.Mutex
}

///|
fn retryable(error : Error) -> Bool {
  match error {
    InvalidConfig(_)
    | Codec(_)
    | Protocol(_)
    | Rejected(_)
    | InboxFull
    | Storage(_)
    | CorruptSession(_)
    | IncompatibleSessionVersion(_)
    | SessionClientIdMismatch(_)
    | DisconnectUncertain
    | TlsValidation(_)
    | RecoveryUncertain => false
    _ => true
  }
}

///|
fn ReconnectPolicy::validate(self : ReconnectPolicy) -> Unit raise MqttError {
  if self.initial_delay_ms < 1 || self.max_delay_ms < self.initial_delay_ms {
    raise InvalidConfig("invalid reconnect delay range")
  }
}

///|
fn ReconnectPolicy::next_delay(
  self : ReconnectPolicy,
  current : Int32,
) -> Int32 {
  if current > self.max_delay_ms / 2 {
    self.max_delay_ms
  } else {
    current * 2
  }
}

///|
/// Start a background supervisor. The task group must outlive the handle.
/// Invalid configuration and unreadable snapshots fail before a handle is returned.
pub async fn[X] AutoClient::start(
  config : Config,
  group : @async.TaskGroup[X],
  policy~ : ReconnectPolicy = ReconnectPolicy::new(),
) -> AutoClient {
  validate(config)
  policy.validate()
  let session = load_session(config)
  let self = {
    config,
    policy,
    session,
    current: None,
    status: Connecting,
    last_error: None,
    stopped: false,
    will_connect_attempted: false,
    earlier_will_uncertain: false,
    task: None,
    receive_lock: @async.Mutex::Mutex(),
  }
  self.task = Some(
    group.spawn(no_wait=true, allow_failure=true) <| () => {
      let result : Result[Unit?, Error] = Ok(
        @async.handle_cancellation(() => self.run(group)),
      ) catch {
        e => Err(e)
      }
      match result {
        Ok(Some(_)) => ()
        Ok(None) => {
          self.stopped = true
          self.status = Stopped
          match self.current {
            Some(client) => @async.protect_from_cancel(() => client.close())
            None => ()
          }
          self.current = None
        }
        Err(e) => {
          self.last_error = Some(e)
          self.status = Failed
          match self.current {
            Some(client) => client.close() catch { _ => () }
            None => ()
          }
          self.current = None
        }
      }
    },
  )
  self
}

///|
async fn[X] AutoClient::run(
  self : AutoClient,
  group : @async.TaskGroup[X],
) -> Unit {
  let mut failures = 0
  let mut delay = self.policy.initial_delay_ms
  while !self.stopped {
    let connected : Result[Client, Error] = Ok(
      connect_with_session(
        self.config,
        self.session,
        group,
        on_mqtt_connect=Some(() => self.will_connect_attempted = true),
      ),
    ) catch {
      e => Err(e)
    }
    let reason = match connected {
      Ok(client) => {
        self.current = Some(client)
        self.last_error = None
        self.status = Connected(client.session_present())
        failures = 0
        delay = self.policy.initial_delay_ms
        while !self.stopped && !client.closed {
          @async.sleep(10)
        }
        if self.stopped {
          return
        }
        if self.will_connect_attempted && self.config.will is Some(_) {
          self.earlier_will_uncertain = true
        }
        self.will_connect_attempted = false
        let error = match client.last_error {
          Some(e) => e
          None => Closed
        }
        client.close()
        self.current = None
        error
      }
      Err(e) => {
        if self.will_connect_attempted && self.config.will is Some(_) {
          self.earlier_will_uncertain = true
        }
        self.will_connect_attempted = false
        e
      }
    }
    if self.stopped {
      return
    }
    self.last_error = Some(reason)
    if self.session.storage_failed || !retryable(reason) {
      self.status = Failed
      return
    }
    if failures < 2147483647 {
      failures += 1
    }
    self.status = Retrying(failures, delay)
    @async.sleep(delay)
    delay = self.policy.next_delay(delay)
    if !self.stopped {
      self.status = Connecting
    }
  }
}

///|
pub fn AutoClient::status(self : AutoClient) -> AutoStatus {
  self.status
}

///|
/// The latest connection or terminal error, if any.
pub fn AutoClient::last_error(self : AutoClient) -> Error? {
  self.last_error
}

///|
async fn AutoClient::connected_client(self : AutoClient) -> Client {
  while true {
    match self.status {
      Stopped => raise Closed
      Failed =>
        match self.last_error {
          Some(e) => raise e
          None => raise Closed
        }
      Connected(_) =>
        match self.current {
          Some(client) if !client.closed => return client
          _ => ()
        }
      _ => ()
    }
    @async.sleep(10)
  }
  raise Closed
}

///|
/// Wait for a live connection or raise its permanent failure.
pub async fn AutoClient::wait_connected(self : AutoClient) -> Unit {
  let _ = self.connected_client()
}

///|
/// A receive wait spans reconnections. A message already returned to the caller
/// retains the normal receive() delivery boundary.
pub async fn AutoClient::receive(self : AutoClient) -> Message {
  self.receive_lock.acquire()
  defer self.receive_lock.release()
  while true {
    let client = self.connected_client()
    let received : Result[Message, Error] = Ok(client.receive()) catch {
      e => Err(e)
    }
    match received {
      Ok(message) => return message
      Err(e) => if self.stopped || !client.closed || !retryable(e) { raise e }
    }
  }
  raise Closed
}

///|
/// Executes once on the current connection. An interrupted call reports an
/// error; the session state machine may still finish its QoS handshake later.
pub async fn AutoClient::publish(
  self : AutoClient,
  topic : String,
  payload : Bytes,
  qos~ : @mqtt.QoS = @mqtt.QoS0,
  retain~ : Bool = false,
) -> Unit {
  let client = self.connected_client()
  client.publish(topic, payload, qos~, retain~)
}

///|
/// Executes once on the current connection.
pub async fn AutoClient::subscribe(
  self : AutoClient,
  filter : String,
  qos : @mqtt.QoS,
) -> @mqtt.QoS {
  let client = self.connected_client()
  client.subscribe(filter, qos)
}

///|
/// Executes once on the current connection.
pub async fn AutoClient::unsubscribe(
  self : AutoClient,
  filter : String,
) -> Unit {
  let client = self.connected_client()
  client.unsubscribe(filter)
}

///|
/// Stop retries, send DISCONNECT if the current connection is live, then close.
/// DisconnectUncertain means cleanup succeeded but the broker may publish a Will.
pub async fn AutoClient::stop(self : AutoClient) -> Unit {
  self.stop_with_disconnect(client => client.disconnect())
}

///|
async fn AutoClient::stop_with_disconnect(
  self : AutoClient,
  send_disconnect : async (Client) -> Unit,
) -> Unit {
  if !self.stopped {
    self.stopped = true
    self.status = Stopped
    @async.protect_from_cancel(() => {
      let mut uncertain = self.earlier_will_uncertain
      let mut stop_error : Error? = None
      match self.current {
        Some(client) => {
          if client.closed {
            if self.config.will is Some(_) {
              uncertain = true
            }
          } else {
            let sent : Result[Unit?, Error] = Ok(
              @async.with_timeout_opt(self.config.timeout_ms, () => {
                send_disconnect(client)
              }),
            ) catch {
              e => Err(e)
            }
            match sent {
              Err(e) =>
                if self.config.will is Some(_) {
                  uncertain = true
                } else {
                  stop_error = Some(e)
                }
              Ok(None) =>
                if self.config.will is Some(_) {
                  uncertain = true
                } else {
                  stop_error = Some(TimedOut)
                }
              Ok(Some(_)) => {
                // MQTT 3.1.1 has no DISCONNECT acknowledgement. A local
                // write cannot prove the broker discarded this Will.
                if self.config.will is Some(_) {
                  uncertain = true
                }
                self.will_connect_attempted = false
              }
            }
          }
          client.close() catch {
            e =>
              if self.config.will is Some(_) {
                uncertain = true
              } else if stop_error is None {
                stop_error = Some(e)
              }
          }
        }
        None => ()
      }
      match self.task {
        Some(task) => {
          task.cancel()
          task.wait() catch {
            _ => ()
          }
        }
        None => ()
      }
      if self.will_connect_attempted && self.config.will is Some(_) {
        uncertain = true
      }
      self.current = None
      if uncertain {
        raise DisconnectUncertain
      }
      match stop_error {
        Some(e) => raise e
        None => ()
      }
    })
  }
}
