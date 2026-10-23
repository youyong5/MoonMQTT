# MoonMQTT

`youyong5/moonmqtt` 是供其他 MoonBit 程序调用的 MQTT 3.1.1 客户端库。当前使用 `moonbitlang/async` 的原生 TCP socket，使用 `zbhzs1/moonbit-mqtt` 编解码 MQTT 报文。目标平台为 MoonBit **native**，传输为明文 TCP。尚未发布到 Mooncakes。

## 能力矩阵

| 能力 | 第三阶段状态 |
| --- | --- |
| CONNECT/CONNACK、拒绝原因、用户名/密码、clean session | 已支持；`clean_session` 默认 `true`，可设为 `false`；公开 `session_present()` |
| SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK | 已支持；每次一个过滤器，订阅可请求 QoS 0/1/2 |
| QoS 0 双向发布 | 已支持；发送在 TCP 写入完成后返回 |
| QoS 1 双向发布 | 已支持；发送等待匹配的 PUBACK；接收在入队后发 PUBACK |
| QoS 2 双向发布 | 已支持四步握手；发送等待 PUBCOMP；接收采用规范的 Method B，同进程续接保留去重状态 |
| 心跳、正常断开 | PINGREQ/PINGRESP、DISCONNECT 已支持 |
| 流式读取 | 支持 TCP 分片与合包、最大报文限制 |
| 待处理请求与队列 | 报文 ID 预留、响应类型/ID 检查、超时和取消清理；接收队列有容量上限 |
| 同进程持久会话与显式重连 | `clean_session=false` 时保留 QoS 1/2 在途状态及入站 QoS 2 去重；按原 ID 重发未确认的 PUBLISH 或 PUBREL；不自动重连 |
| 进程退出后的会话恢复 | 未实现；没有磁盘状态保存 |
| 遗嘱消息、TLS、WebSocket、MQTT 5 | 未实现 |

接收队列默认容纳 256 条消息，可在连接前设置 `config.inbox_capacity`。队列满时客户端报 `InboxFull` 并终止连接；QoS 1/2 消息只有成功入队后才确认。应用须持续调用 `receive()`。`clean_session=false` 时，同一进程内使用**同一个旧 `Client` 调用 `reconnect()`**，会保留在途消息、原报文 ID、入站 QoS 2 去重状态和未消费队列；收到 `Session Present=1` 后按协议恢复。若 broker 回 `Session Present=0`，本地旧在途与去重状态会清除，并自动重新订阅已确认的过滤器。若首次连接就收到 `Session Present=1` 而本进程没有本地状态，连接会报协议错误，避免误称完整恢复。

没有磁盘持久化：**进程退出后无法恢复客户端的在途 QoS 1/2 状态或入站 QoS 2 去重状态**。旧 `publish` 在断线或取消时仍会报错，即使后续重连自动完成协议重发；调用方不能将该错误直接判作 broker 未收到消息，也没有跨连接的逐条完成回调。待确认的订阅/取消订阅不会自动重试，调用方应自行重试。超时会终止当前连接，旧 TCP 连接的响应不会进入新连接的待处理表。

## 最小示例

库在本仓库内可直接运行 `examples/basic`。其他 MoonBit 项目在本模块发布 Mooncakes 版本后可添加依赖；目前可通过 Git 源码集成。`moon.pkg` 需要导入本库、`moonbitlang/async` 和 `zbhzs1/moonbit-mqtt`：

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

`Config::new` 默认端口 1883、Keep Alive 30 秒、请求超时 5 秒、最大报文 1 MiB。连接前也可设置 `username`、`password`、`keep_alive_seconds`、`timeout_ms`、`max_packet_size`。连接后不要修改配置。

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

完整的同进程离线收消息示例见 [`examples/session_resume`](examples/session_resume/main.mbt)。`with_client` 适合单次连接；需要显式续接时请手动管理任务组和每一代 `Client`。

公开错误 `MqttError` 包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut` 和 `InboxFull`。底层 socket 错误及取消也可能传播。`publish` 的 QoS 1/2 成功只表示协议确认完成，不表示订阅方已处理消息。

## 运行和测试

```powershell
cd D:\Projects\MoonMQTT
moon check --target native
moon build --target native
moon test --target native
moon run examples/basic --target native
```

端到端测试需要 Mosquitto broker 及 `mosquitto_pub`、`mosquitto_sub`。脚本自行启动和关闭本地 broker，验证 MoonBit 与 CLI 双向 QoS 0/1/2，以及同进程持久订阅在离线期间接收 QoS 1/2：

```powershell
powershell -ExecutionPolicy Bypass -File tests\interop.ps1
# 若 Mosquitto 不在默认安装目录：
powershell -ExecutionPolicy Bypass -File tests\interop.ps1 -MosquittoDirectory 'D:\path\to\mosquitto\bin'
```

本地模拟 broker 的测试还覆盖拒绝连接、意外断开、错误报文、分片/合包、并发 ID 与乱序确认、取消、超时、队列溢出、QoS 2 连续重复 PUBREC，以及持久会话各确认阶段断线、`Session Present` 两种结果和订阅重建。设计依据和依赖取舍见 [docs/design.md](docs/design.md)。
