# MoonMQTT 设计记录（第二阶段）

## 依据与依赖

- [OASIS MQTT 3.1.1 标准](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) §4.3.2、§4.3.3、§4.4：QoS 1 等待 PUBACK；QoS 2 发送方在 PUBREC 后发送 PUBREL，PUBCOMP 后才能复用 ID；接收方对 PUBREL 回 PUBCOMP，对 PUBREL 前重复 PUBLISH 回 PUBREC 且不能再次交付。§4.4 要求 `CleanSession=0` 重连时重发未确认的 PUBLISH 或 PUBREL；本库固定 clean session，不实现该跨连接恢复。
- [`moonbitlang/async` 0.22.5](https://mooncakes.io/docs/moonbitlang/async)：使用 `Tcp::connect_to_host`、`read_exactly`、`write`、`close`，一个连接只有一个读任务。TLS 包具有 `Tls::client`，但本阶段仍只使用明文 TCP。
- [`zbhzs1/moonbit-mqtt` 0.1.0](https://mooncakes.io/docs/zbhzs1/moonbit-mqtt)：已公开 QoS 2 的 `encode_pubrec/pubrel/pubcomp`、解码和 `Packet` 变体，足以继续复用为编解码依赖。本库保留其 QoS 类型作为公开 API 的参数/字段类型，不复制报文编码。其 `packet_total_length` 不能区分不完整缓冲与非法报文，因此本库独立按固定头 Remaining Length 读取完整帧，再交给 codec 解码。

现有 codec 仅处理报文，不提供网络连接、异步任务、请求匹配、消息队列、超时、QoS 握手状态、会话或重连。MoonMQTT 实现这些客户端行为。

## 结构和公开 API

| 路径 | 用途 |
| --- | --- |
| `moonmqtt.mbt` | 配置、客户端 API、TCP 帧读取、请求表、QoS 1/2 握手、心跳、关闭 |
| `moonmqtt_wbtest.mbt` | 第一阶段及回归模拟 broker 测试 |
| `qos2_wbtest.mbt` | 并发、取消、队列与 QoS 2 状态测试 |
| `examples/basic` | 最小库调用 |
| `examples/interop_pub`、`examples/interop_sub` | Mosquitto CLI 双向互通端点 |
| `tests/interop.ps1` | 自动启动 broker，执行 QoS 0/1/2 互通 |

`Config::new(host, client_id, port?)` 后可设置用户名、密码、Keep Alive、请求超时、报文上限和接收队列容量。`Client::connect(config, group)` 完成 CONNACK 后生成读任务与心跳任务。`with_client(config, callback)` 自动管理任务组并调用正常断开/异常关闭。`subscribe(filter, qos)` 返回授予的 QoS；`unsubscribe(filter)` 等待 UNSUBACK；`publish(topic, payload, qos?, retain?)` 对 QoS 0 等待 TCP 写入、QoS 1 等待 PUBACK、QoS 2 等待 PUBCOMP；`receive()` 获取下一条消息。`disconnect()` 发送 DISCONNECT 并关闭，`close()` 不发送 DISCONNECT，二者均可在终止后清理资源。

`MqttError` 为 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`、`InboxFull`。底层 socket 错误和取消可能直接向调用方传播。连接关闭时消息队列、心跳队列与所有待处理请求一起关闭，等待者被唤醒；调用方应在任务组退出前调用 `close()` 或 `disconnect()` 以等待后台任务并关闭 socket。

## 请求与状态

客户端在同步的 `reserve()` 中选取 1–65535 范围内未使用的 ID，并立即放入待处理表。并发 `publish`、`subscribe`、`unsubscribe` 因此不能拿到相同 ID。每条待处理请求记录期望确认类型；未知 ID、类型不符或错误顺序是协议错误，终止当前连接。成功确认后释放 ID。超时、任务取消、写失败或异常断开会终止连接并清理整张表，防止旧确认命中同连接后复用的 ID。每个 `Client` 独占其 TCP、读任务和待处理表，新连接不会读取旧 socket。

发送 QoS 2 状态：`PUBLISH → AwaitPUBREC → PUBREL → AwaitPUBCOMP → done`。收到重复 PUBREC 时只再发 PUBREL，绝不在 PUBREL 后重发 PUBLISH；仅 PUBCOMP 使 ID 可用。当前连接内没有定时重发；等确认超时则关闭连接。按 §4.4，持久会话重连才要求按原 ID 重发未确认的 PUBLISH/PUBREL；持久会话尚未实现，故无跨连接恢复。

接收 QoS 2 使用标准图 4.3 的 Method B：首次 PUBLISH 成功入队并记录 ID 后发 PUBREC；到 PUBREL 前重复 PUBLISH 仅重发 PUBREC；PUBREL 后标记完成并发 PUBCOMP；重复 PUBREL 仍发 PUBCOMP。后续相同 ID 的 PUBLISH 被视作新消息，包括 DUP 位为 1 的情况。完成 ID 的标记最多覆盖 65535 个编号，随连接终止清除。对未知 ID 的 PUBREL 仍返回 PUBCOMP，遵循标准对 PUBREL 的无条件确认要求。QoS 1 每个已成功入队的 PUBLISH 都单独 PUBACK；标准允许相同 ID 在 PUBACK 后代表新消息。

接收队列使用可配置的有界 `Blocking(capacity)`，读任务以 `try_put` 非阻塞入队。如果队列满，发出 `InboxFull` 并终止连接，且不确认当前 QoS 1/2 消息，避免读任务卡住后无法处理其他确认报文。应用若已收到 QoS 1/2 的成功确认但尚未消费入队消息，随后发生断线，则消息仍可能丢失：队列和去重表不持久化。QoS 0 则没有协议确认。

TCP 是字节流；`read_packet` 读取 1 字节固定头、1–4 字节 Remaining Length、再读取精确正文长度，限制最大报文。它不会将一次 `read` 视作一包。`write_lock` 串行化完整报文写入；心跳与业务请求共用该锁。

## 尚未实现

下一阶段按此顺序：1. 持久会话与本地状态存储，恢复 QoS 1/2 的 PUBLISH/PUBREL；2. 自动重连与退避，并验证旧连接隔离和恢复顺序；3. 遗嘱消息及其配置和测试；4. TLS 传输、证书验证与真实 broker 互通。此后再考虑 WebSocket、MQTT 5 和更细的流控策略。
