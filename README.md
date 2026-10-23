# MoonMQTT

`youyong5/moonmqtt` 是供其他 MoonBit 程序调用的 MQTT 3.1.1 客户端库。当前使用 `moonbitlang/async` 的原生 TCP 与 TLS，使用 `zbhzs1/moonbit-mqtt` 编解码 MQTT 报文。目标平台为 MoonBit **native**；明文 TCP 与 MQTT over TLS 均可配置。

## 能力矩阵

| 能力 | 当前状态 |
| --- | --- |
| CONNECT/CONNACK、拒绝原因、用户名/密码、clean session | 已支持；`clean_session` 默认 `true`，可设为 `false`；公开 `session_present()` |
| SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK | 已支持；每次一个过滤器，订阅可请求 QoS 0/1/2 |
| QoS 0 双向发布 | 已支持；发送在 TCP 写入完成后返回 |
| QoS 1 双向发布 | 已支持；发送等待匹配的 PUBACK；接收在入队后发 PUBACK |
| QoS 2 双向发布 | 已支持四步握手；发送等待 PUBCOMP；接收采用规范的 Method B，持久存储可保留去重状态 |
| 心跳、正常断开 | PINGREQ/PINGRESP、DISCONNECT 已支持 |
| 流式读取 | 支持 TCP 分片与合包、最大报文限制 |
| 待处理请求与队列 | 报文 ID 预留、响应类型/ID 检查、超时和取消清理；接收队列有容量上限 |
| 同进程持久会话与显式重连 | `clean_session=false` 时保留 QoS 1/2 在途状态及入站 QoS 2 去重；按原 ID 重发未确认的 PUBLISH 或 PUBREL |
| 可选自动重连 | `AutoClient` 稳定句柄；连接中、已连接、重试中、永久失败和停止状态；临时故障使用有上限指数退避，支持停止与取消 |
| native 进程重启恢复 | 可选 `SessionStore`；保存出站 QoS 1/2 阶段及原 ID、入站 QoS 2 状态和未消费消息；支持文件存储或应用自定义存储 |
| MQTT 3.1.1 遗嘱 | 可配置主题、二进制负载、QoS 0/1/2 和 retain；每代自动重连的 CONNECT 都重新携带遗嘱 |
| native MQTT over TLS | 可选；默认验证系统信任链与连接主机名，并以该主机名设置 SNI；可指定 PEM CA；握手受连接超时限制 |
| WebSocket、MQTT 5 | 未实现 |

接收队列默认容纳 256 条消息，可在连接前设置 `config.inbox_capacity`。队列满时客户端报 `InboxFull` 并终止连接；QoS 1/2 消息只有成功入队后才确认。应用须持续调用 `receive()`。`clean_session=false` 时，同一进程内可使用旧 `Client` 调用 `reconnect()`；进程重启后则以同一 Client ID 和快照路径调用 `Client::connect()`。收到 `Session Present=1` 后按协议恢复。若 broker 回 `Session Present=0`，本地旧在途与去重状态会清除，并自动重新订阅已确认的过滤器。若无本地快照却收到 `Session Present=1`，连接会报协议错误。

持久化是**可选**的；未配置 `session_store` 时仍仅支持同进程恢复。旧 `publish` 在断线或取消时仍会报错，即使后续连接自动完成协议重发；调用方不能将该错误直接判作 broker 未收到消息，也没有跨连接的逐条完成回调。待确认的订阅/取消订阅不会自动重试，调用方应自行重试。超时会终止当前连接，旧 TCP 连接的响应不会进入新连接的待处理表。

关闭或异常断线后，clean session 清理本地会话；无存储的持久会话将未消费的接收队列收拢到内存会话；配置 `SessionStore` 的持久会话保留与最近一次成功快照一致的内存状态。后一种情况下，**旧 `Client`** 可直接调用 `reconnect(group)` 恢复同进程会话，包括原 ID、QoS 2 阶段、入站去重及订阅。若发生存储失败，旧会话会被标记为不可直接复用，须先修复存储并用 `Client::connect()` 从最后完整快照重建。

使用持久存储时，库会先保存未消费消息和入站 QoS 2 去重记录，再发送 PUBREC；先保存 QoS 2 的 PUBREL 阶段，再发送 PUBREL。`receive()` 会先从快照删除消息，再返回给调用方。因此**返回后应用尚未完成处理就崩溃时，库不能保证应用层恰好一次**；即使崩溃发生在保存删除记录与函数返回之间，应用也可能拿不到消息。需要应用处理确认或事务一致性时，须在应用层另行设计。QoS 1 允许重投递。

## 最小示例

库在[本仓库的 `examples/basic`](https://github.com/youyong5/MoonMQTT/tree/main/examples/basic) 中有可运行示例。其他 MoonBit 项目先添加依赖：

```powershell
moon add youyong5/moonmqtt@0.1.0
moon add moonbitlang/async@0.22.5
moon add zbhzs1/moonbit-mqtt@0.1.0
```

然后在 `moon.pkg` 导入本库、`moonbitlang/async` 和 `zbhzs1/moonbit-mqtt`：

```moonbit
import {
  "youyong5/moonmqtt" @client,
  "moonbitlang/async" @async,
  "zbhzs1/moonbit-mqtt" @packet,
}
```

```moonbit
async fn main {
  let config = @client.Config::new("127.0.0.1", "my-client", port=1883)
  config.inbox_capacity = 128
  @client.with_client(config, client => {
    let _ = client.subscribe("demo/#", @packet.QoS2)
    client.publish("demo/hello", b"hello", qos=@packet.QoS2)
    let message = @async.with_timeout(5000, () => client.receive())
    println(message.topic)
    client.unsubscribe("demo/#")
  })
}
```

`Config::new` 默认端口 1883、Keep Alive 30 秒、请求超时 5 秒、最大报文 1 MiB。连接前也可设置 `username`、`password`、`keep_alive_seconds`、`timeout_ms`、`max_packet_size`。调用 `Client::connect` 或 `AutoClient::start` 后不要修改配置。

配置遗嘱时，负载按字节发送，可以包含零字节；主题须是有效的 MQTT 主题名（不能为空或包含 `+`、`#`），负载最多 65535 字节。非法配置在连接前抛 `InvalidConfig`：

```moonbit
let config = @client.Config::new("127.0.0.1", "stable-client-id")
config.will = Some(@client.Will::new(
  "devices/alpha/status",
  Bytes::from_array([0x00, 0xff, 0x41]),
  qos=@packet.QoS2,
  retain=true,
))
```

遗嘱关联每一条被 broker 接受的网络连接。正常 `disconnect()` 与连接仍可用时的 `AutoClient::stop()` 会发送 DISCONNECT；broker 收到后不应发布该连接的遗嘱。`Client::close()` 直接关闭连接；网络故障、进程强制终止及自动重连前的断线都可能触发此前连接的遗嘱。重连只给新连接重新登记遗嘱，不能撤销旧连接的发布。MQTT 3.1.1 没有 DISCONNECT 响应，因此**配置了遗嘱的 `AutoClient::stop()` 即使成功写入 DISCONNECT，也在清理后抛 `DisconnectUncertain`**：库无法证明 broker 已收到它并撤销遗嘱。发送失败、在已发送 CONNECT 但未完成握手期间停止，或此前连接已异常断开时也抛该错误。TLS 握手尚未完成且 MQTT CONNECT 未发送时，没有遗嘱登记。

TLS 保留原有明文默认配置；在连接前设置 `config.tls` 才启用 TLS。默认使用系统可信根，验证证书链和 `config.host`，并用 `config.host` 设置 SNI。私有 CA 使用 PEM 文件，替换系统可信根集合；证书或主机名验证失败报 `TlsValidation`，不会自动无限重试。CA 文件不可读报 `InvalidConfig`。请用证书中的 DNS 名称作为 `host`；端口可设为 broker 的 TLS listener 端口：

```moonbit
let config = @client.Config::new("broker.example.com", "stable-client-id", port=8883)
config.tls = Some(@client.TlsConfig::new()) // 系统可信根
// 私有 CA：config.tls = Some(@client.TlsConfig::new(ca_file="./ca.pem"))
```

TLS 握手在 MQTT CONNECT 之前完成，受 `timeout_ms` 限制；失败时关闭 TCP/TLS 资源。临时 TCP 故障（包括握手时断线）和已建立连接后的网络断线仍按自动重连策略恢复。当前没有客户端证书认证配置。

`with_client` 创建任务组并在正常返回时发送 DISCONNECT；回调抛错时关闭 TCP。手动使用 `Client::connect(config, group)` 时，任务组须覆盖所有客户端操作，并最终调用异步 `disconnect()` 或 `close()`。`receive()` 一直等待下一条消息或终止错误，可由调用方用 `@async.with_timeout` 限时。

需要续接时，在**同一个任务组和进程**中设置 `config.clean_session = false`，并保存旧 `Client` 对象：

```moonbit
let config = @client.Config::new("127.0.0.1", "stable-client-id")
config.clean_session = false
@async.with_task_group(group => {
  let first = @client.Client::connect(config, group)
  let _ = first.subscribe("demo/#", @packet.QoS1)
  // 断线后，或主动调用 first.close()/first.disconnect() 后：
  let second = first.reconnect(group)
  if second.session_present() {
    println("resumed broker session")
  }
  second.disconnect()
})
```

完整的同进程离线收消息示例见[仓库中的 `examples/session_resume`](https://github.com/youyong5/MoonMQTT/blob/main/examples/session_resume/main.mbt)。`with_client` 适合单次连接；需要显式续接时请手动管理任务组和每一代 `Client`。

自动模式使用稳定的 `AutoClient` 句柄；原有 `Client::connect`、`reconnect(group)` 和手动管理方式仍可使用：

```moonbit
let config = @client.Config::new("127.0.0.1", "stable-auto-id")
config.clean_session = false
@async.with_task_group(group => {
  let client = @client.AutoClient::start(
    config, group,
    policy=@client.ReconnectPolicy::new(initial_delay_ms=100, max_delay_ms=5000),
  )
  client.wait_connected()
  let _ = client.subscribe("demo/#", @packet.QoS1)
  let message = client.receive() // 可跨临时断线继续等待
  println(message.topic)
  client.stop()
})
```

`status()` 返回 `Connecting`、`Connected(session_present)`、`Retrying(连续失败次数, 下次延迟毫秒)`、`Failed` 或 `Stopped`；`last_error()` 可查看最近的连接或终止错误。首次配置或快照读取错误由 `start()` 直接抛出。`wait_connected()` 及自动句柄上的发布、订阅、取消订阅和接收在连接中等待；永久失败时抛出原因，停止后抛 `Closed`。等待可由调用方取消。`stop()` 会停止退避，当前连接可用时先发送 DISCONNECT，再关闭连接并等待后台任务；带遗嘱时因协议没有收件确认，清理后报告 `DisconnectUncertain`，调用方不能据此判断 broker 实际是否发布遗嘱。任务组取消会直接关闭连接，不保证抑制遗嘱。

配置遗嘱时，调用方应捕获停止后的不确定结果；本地资源此时已经清理：

```moonbit
try {
  client.stop()
} catch {
  @client.DisconnectUncertain => println("broker 可能已发布遗嘱")
}
```

未配置遗嘱且连接已关闭时，`stop()` 正常返回；若 DISCONNECT 发送失败，则清理后返回原始错误，不报 `DisconnectUncertain`。

`AutoClient::publish()`、`subscribe()`、`unsubscribe()` 每次只在选中的连接上调用一次。**调用中途断线时，原调用返回错误，不会作为新请求自动调用第二次**；持久会话仅按 MQTT 协议恢复已登记的 QoS 1/2 在途阶段。调用方不能依据该错误判断 broker 是否收到消息，也不应盲目重发。`receive()` 可跨临时断线继续等待，但已返回消息仍受前述应用层交付边界限制。`clean_session=false` 才能保留在途和订阅状态；若 broker 回 `Session Present=0`，仍按现有规则清理旧在途并重建已确认订阅。未确认的订阅操作不会自动重新发起。

退避从 `initial_delay_ms` 开始，每次失败翻倍，最高 `max_delay_ms`；成功连接后重置。网络断开、连接失败和心跳超时会重试；配置、编解码、认证拒绝、协议、`RecoveryUncertain`、存储和 `InboxFull` 错误进入 `Failed`，不会无限重试。完整示例见[仓库中的 `examples/auto_resume`](https://github.com/youyong5/MoonMQTT/blob/main/examples/auto_resume/main.mbt)。

跨进程恢复使用同一 Client ID、同一快照路径，并在每个进程中重新创建配置：

```moonbit
let config = @client.Config::new("127.0.0.1", "stable-client-id")
config.clean_session = false
config.session_store = Some(@client.SessionStore::file("./mqtt-session.bin"))
@async.with_task_group(group => {
  let client = @client.Client::connect(config, group)
  println("broker session present: \{client.session_present()}")
  // subscribe / publish / receive ...
  client.disconnect()
})
```

完整的分进程示例见[仓库中的 `examples/process_resume`](https://github.com/youyong5/MoonMQTT/blob/main/examples/process_resume/main.mbt)，运行 `moon run --target native examples/process_resume seed .\mqtt-session.bin`，在另一进程以 `resume` 模式使用同一路径。一个快照文件只允许一个活动客户端进程使用。文件快照格式版本为 3，限 64 MiB；写入临时文件、同步后替换旧文件。`SessionStore::new(load, save)` 可接入应用存储；`load` 仅在没有快照时返回 `None`，`save` 必须完整、原子且持久地替换旧快照。文件实现针对**进程退出/崩溃**恢复；未承诺断电时目录项也已落盘。

损坏快照、未知版本、Client ID 不一致分别报 `CorruptSession`、`IncompatibleSessionVersion`、`SessionClientIdMismatch`，不会当成新会话。`Storage` 表示读写失败；一次写失败后旧 `Client` 不能直接 `reconnect()`，应先排除故障，再用 `Client::connect()` 从持久快照重建。连接尝试中若进程崩溃，或 broker 会话重置的本地记录未能保存，快照会保留不确定标记；下次 `Session Present=1` 报 `RecoveryUncertain`，`Session Present=0` 可安全清理旧状态。若必须放弃不确定旧会话，先以同一 ID 建立 clean session 连接清除 broker 状态，再删除快照并建立新持久会话；此操作会丢弃旧会话消息。

broker 回 `Session Present=0` 后，已确认的订阅会自动重建。若重建途中进程退出，快照会保留待重订阅标记；下次即使 broker 回 1，也会继续重订阅。重复 SUBSCRIBE 可能使 retained 消息再次到达。

公开错误 `MqttError` 另包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`、`InboxFull`、`DisconnectUncertain` 和 `TlsValidation`。底层 socket 错误及取消也可能传播。`publish` 的 QoS 1/2 成功只表示协议确认完成，不表示订阅方已处理消息。

## 运行和测试

```powershell
cd D:\Projects\MoonMQTT
moon info
moon fmt
moon check --target native
moon build --target native
moon test --target native
moon run examples/basic --target native
```

端到端测试需要 Mosquitto broker、`mosquitto_pub`、`mosquitto_sub` 和 OpenSSL（脚本默认使用 Git for Windows 自带的 `openssl.exe`）。总脚本自行启动和关闭本地 broker，先运行明文 QoS、会话续接、自动重连与遗嘱回归，再用临时测试 CA 启动 TLS listener，验证默认/指定 CA、错误主机名、双向 QoS 0/1/2、跨进程持久会话与自动重连：

```powershell
powershell -ExecutionPolicy Bypass -File tests\interop.ps1
# 若 Mosquitto 不在默认安装目录：
powershell -ExecutionPolicy Bypass -File tests\interop.ps1 -MosquittoDirectory 'D:\path\to\mosquitto\bin'
# 单跑 TLS：powershell -ExecutionPolicy Bypass -File tests\tls_interop.ps1 -MosquittoDirectory 'D:\path\to\mosquitto\bin'
```

本地模拟 broker 的测试还覆盖拒绝连接、意外断开、错误报文、分片/合包、并发 ID 与乱序确认、取消、超时、队列溢出、QoS 2 连续重复 PUBREC、旧 `Client` 在主动关闭和异常断线后的同进程恢复、关闭与异步保存交错、跨进程 QoS 1/2 重发、损坏快照、存储故障前不发送确认、持久会话两种 `Session Present` 结果，以及自动模式的退避上限、停止/取消、QoS 1/2 在途恢复、订阅重建和永久错误。遗嘱测试检查非法配置、CONNECT 字段、每代重连携带相同遗嘱、DISCONNECT 发送失败，以及握手和退避交错时的停止结果。TLS 回归覆盖 ClientHello SNI、握手超时、握手中停止和不可读 CA。设计依据和依赖取舍见 [docs/design.md](docs/design.md)。
