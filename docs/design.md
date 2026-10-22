# MoonMQTT 第一阶段设计记录

## 依据与选型

- [OASIS MQTT 3.1.1 标准](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)：TCP 是有序字节流，报文由固定头的 Remaining Length 定界；QoS 1 发布在对应 PUBACK 到达后完成；收到 QoS 1 PUBLISH 要发送 PUBACK。
- [`moonbitlang/async` TCP API](https://mooncakes.io/docs/moonbitlang/async/socket)：`Tcp::connect_to_host`、`read_exactly`、`write`、`close`；一个连接只允许一个读取任务。项目固定 `0.22.5`，使用 native 目标。
- [`moonbitlang/async` TLS API](https://mooncakes.io/docs/moonbitlang/async/tls)：`Tls::client` 可封装 TCP 且实现 Reader/Writer，适合后续传输抽象；本阶段只接受明文 TCP。
- [`zbhzs1/moonbit-mqtt` 0.1.0](https://mooncakes.io/docs/zbhzs1/moonbit-mqtt)：公开 `Connect`、`Publish`、`Subscribe`、`Packet` 等类型及各报文编码/解码函数，足以作为本阶段的编解码依赖。

现有包是 packet codec，没有连接、异步任务、确认匹配、保活、接收队列、会话或重传。其 `packet_total_length` 对尚未收齐的缓冲区和非法报文都返回 `Err(String)`，不适合作为流式读取器的唯一判据。本库自行读取固定头和变长 Remaining Length，再按确切长度读取正文，并交给该包校验和解码。没有复制其报文编解码实现。

## 结构

| 路径 | 用途 |
| --- | --- |
| `moonmqtt.mbt` | 对外 API、TCP 流读取、请求匹配、心跳、生命周期 |
| `moonmqtt_wbtest.mbt` | 本地假 broker 的字节流与异常路径测试 |
| `examples/basic` | 以外部项目方式调用库的最小示例 |
| `examples/interop_pub`、`examples/interop_sub` | 与 Mosquitto CLI 互通的两端 |
| `tests/interop.ps1` | 本地 Mosquitto 互通脚本 |

## 公开 API 和生命周期

`Config::new(host, client_id, port?)` 给出配置默认值，调用方可在连接前设置 username、password、keep_alive_seconds、timeout_ms 和 max_packet_size。连接只采用 clean session。`Client::connect(config, group)` 完成 CONNECT/CONNACK 后，在调用方的 `TaskGroup` 中启动一个读取任务和一个心跳任务。所有客户端操作均须在该组结束之前完成。

首选 `with_client(config, callback)` 管理任务组：回调正常结束时发送 DISCONNECT，异常结束时关闭连接。直接调用 `connect` 时，调用方须调用异步 `disconnect()` 或 `close()`。`close()` 先取消并等待后台任务，再关闭 TCP；这样可避免 Windows 上关闭仍有挂起读取的 socket 时卡住任务组。

`subscribe(filter, qos)` 返回授予的 QoS，`unsubscribe(filter)` 等待 UNSUBACK，`publish(topic, payload, qos?, retain?)` 的 QoS 1 版本等待匹配的 PUBACK，`receive()` 从接收队列取下一条消息。内部 packet ID 在 1…65535 间循环分配，跳过待处理 ID；请求匹配按 ID，收到错误类型的响应会报协议错误。超时会终止连接，避免迟到的确认被错误地匹配到以后复用的 ID。关闭会唤醒所有等待确认和接收的任务。

错误包含 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`；底层 async TCP 错误及调用方取消也可能直接传播。`receive()` 没有隐式超时，可由调用方使用 `@async.with_timeout` 限定。

## 范围与后续

本阶段支持 QoS 0/1 双向传输、订阅与取消、周期 PINGREQ/PINGRESP、正常断开。保留位随发布传递，收到的重复标志传递给应用；QoS 1 不做持久化和断线重发，因此不承诺跨连接的至少一次交付。接收队列当前无容量上限，应用需持续消费。尚无 QoS 2、持久会话/恢复、自动重连、遗嘱消息、TLS、WebSocket、MQTT 5。
