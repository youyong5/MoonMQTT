# MoonMQTT

`youyong5/moonmqtt` 是供其他 MoonBit 程序调用的 MQTT 3.1.1 客户端库。当前使用 `moonbitlang/async` 的原生 TCP socket，使用 `zbhzs1/moonbit-mqtt` 编解码 MQTT 报文。目标平台为 MoonBit **native**，传输为明文 TCP。尚未发布到 Mooncakes。

## 能力矩阵

| 能力 | 第二阶段状态 |
| --- | --- |
| CONNECT/CONNACK、拒绝原因、用户名/密码、clean session | 已支持；固定 `clean_session=true` |
| SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK | 已支持；每次一个过滤器，订阅可请求 QoS 0/1/2 |
| QoS 0 双向发布 | 已支持；发送在 TCP 写入完成后返回 |
| QoS 1 双向发布 | 已支持；发送等待匹配的 PUBACK；接收在入队后发 PUBACK |
| QoS 2 双向发布 | 已支持当前连接内四步握手；发送等待 PUBCOMP；接收采用规范的 Method B，按报文 ID 去重，重复 PUBREL 再次确认 |
| 心跳、正常断开 | PINGREQ/PINGRESP、DISCONNECT 已支持 |
| 流式读取 | 支持 TCP 分片与合包、最大报文限制 |
| 待处理请求与队列 | 报文 ID 预留、响应类型/ID 检查、超时和取消清理；接收队列有容量上限 |
| 持久会话、断线重发、自动重连 | 未实现 |
| 遗嘱消息、TLS、WebSocket、MQTT 5 | 未实现 |

接收队列默认容纳 256 条消息，可在连接前设置 `config.inbox_capacity`。队列满时客户端报 `InboxFull` 并终止连接；QoS 1/2 消息只有成功入队后才确认。应用须持续调用 `receive()`。QoS 2 去重状态只保留在当前连接；断线后既不保存入站状态，也不重发出站 PUBLISH/PUBREL，因此**不承诺跨连接的 exactly once 或 QoS 1 至少一次交付**。QoS 1/2 调用在发送中、等待确认时断线或被取消，结果可能已到达 broker，调用方应自行处理这种不确定性。当前超时会终止连接，避免旧确认与同连接后续复用的 ID 混淆。旧 TCP 连接的响应无法进入新 `Client` 的待处理表。

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

公开错误 `MqttError` 包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut` 和 `InboxFull`。底层 socket 错误及取消也可能传播。`publish` 的 QoS 1/2 成功只表示协议确认完成，不表示订阅方已处理消息。

## 运行和测试

```powershell
cd D:\Projects\MoonMQTT
moon check --target native
moon build --target native
moon test --target native
moon run examples/basic --target native
```

端到端测试需要 Mosquitto broker 及 `mosquitto_pub`、`mosquitto_sub`。脚本自行启动和关闭本地 broker，验证 MoonBit 与 CLI 双向 QoS 0/1/2：

```powershell
powershell -ExecutionPolicy Bypass -File tests\interop.ps1
# 若 Mosquitto 不在默认安装目录：
powershell -ExecutionPolicy Bypass -File tests\interop.ps1 -MosquittoDirectory 'D:\path\to\mosquitto\bin'
```

本地模拟 broker 的测试还覆盖拒绝连接、意外断开、错误报文、分片/合包、并发 ID 与乱序确认、取消、超时、队列溢出及 QoS 2 重复报文。设计依据和依赖取舍见 [docs/design.md](docs/design.md)。
