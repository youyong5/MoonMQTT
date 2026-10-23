# MoonMQTT 设计记录（第三阶段）

## 协议依据与依赖

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) §3.1.2.4、§3.2.2.2：`CleanSession=0` 时会话以稳定 Client ID 标识；CONNACK `Session Present` 表明 broker 是否保留旧会话。客户端会话状态包括未完全确认的出站 QoS 1/2 和未完全确认的入站 QoS 2；broker 会话另包含订阅。`Session Present=0` 代表新会话。
- 同一规范 §4.3.3、§4.4、§4.6：出站 QoS 2 收到 PUBREC 后只能重发 PUBREL；重连且 `CleanSession=0` 时按原 ID 重发未确认的 PUBLISH 或 PUBREL；重发 PUBLISH 保持原始顺序并设置 DUP。PUBREL 固定头保留位仍为 `0x62`。
- `moonbitlang/async` 0.22.5 提供 TCP、任务组、队列、互斥锁和超时；`zbhzs1/moonbit-mqtt` 0.1.0 提供 MQTT 报文编解码。codec 的 `packet_total_length` 不能区分不完整缓冲与非法报文，因此本库先读取固定头和 Remaining Length 指定的完整帧，再交给 codec。TLS 包存在，但尚未接入本库。

## 代码结构和公开 API

| 路径 | 用途 |
| --- | --- |
| `moonmqtt.mbt` | 客户端 API、TCP 帧读取、连接任务、请求/会话状态、QoS 1/2 恢复 |
| `moonmqtt_wbtest.mbt` | 初始连接及流式读写回归 |
| `qos2_wbtest.mbt` | QoS 2 双向握手、重复 PUBREC、取消、错误报文 |
| `session_wbtest.mbt` | 会话重连、各阶段断线、订阅恢复、在途 ID 与顺序 |
| `examples/basic`、`examples/session_resume` | 最小单连接使用、同进程续接示例 |
| `examples/interop_pub`、`examples/interop_sub`、`tests/interop.ps1` | Mosquitto CLI 双向互通和真实会话续接 |

`Config::new(host, client_id, port?)` 后可配置 `clean_session`（默认 `true`）、用户名/密码、Keep Alive、超时、报文上限和接收队列容量。`Client::connect(config, group)` 建立 TCP 并完成 CONNECT/CONNACK。`Client::session_present()` 返回本次 CONNACK 的状态；持久会话由旧 `Client::reconnect(group)` 显式创建下一代连接，仍使用原客户端 ID 和同一内存会话。不能对 clean session 调用 `reconnect`，也不能让同一会话同时连接两条 TCP。

`subscribe(filter, qos)` 返回授予的 QoS，`unsubscribe(filter)` 等待 UNSUBACK；`publish(topic, payload, qos?, retain?)` 对 QoS 0 等待 TCP 写入、QoS 1 等待 PUBACK、QoS 2 等待 PUBCOMP；`receive()` 取下一条消息。`disconnect()` 发送 DISCONNECT 并关 TCP，`close()` 直接关 TCP。二者均等待后台读/心跳任务。`with_client` 管理单次连接的任务组；显式重连应自行管理任务组及每一代 `Client`。错误包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`、`InboxFull`，底层 socket 错误及取消可能直接传播。

## 同进程会话状态

每个初始 `Client` 拥有一份 `Session`；重连的新 `Client` 复用它。会话记录稳定 Client ID、下一个报文 ID、出站在途消息及原始发送顺序、入站 QoS 2 去重状态、已确认的订阅，以及旧连接尚未被 `receive()` 消费的消息。每代 `Client` 独占 TCP、读/心跳任务、响应队列和临时请求表。旧连接关闭时清空临时表并唤醒等待者；持久会话的出站和入站状态留在 `Session` 中。旧 socket 的迟到响应无法进入新连接的临时表。

新报文 ID 在同步 `reserve()` 中立即进入临时表，分配时同时跳过会话里未确认的出站 ID。收到 PUBACK 或 PUBCOMP 才从会话删除对应消息并释放 ID。超时、取消、写失败、异常断开使当前连接终止；持久会话保留已登记的 QoS 1/2 在途消息，clean session 则清除。旧 `publish` 调用仍报错；随后恢复的确认不会回填到原调用，目前没有逐条跨连接完成通知。已确认的订阅记在会话中；尚未得到 SUBACK/UNSUBACK 的操作结果不确定，调用方应重试。

若重连 CONNACK 的 `Session Present=1`，本库在返回新客户端前按原顺序重发出站在途报文：等待 PUBACK/PUBREC 的消息发送原 ID 且 `DUP=1` 的 PUBLISH；已经收到 PUBREC 的消息仅重发原 ID 的 PUBREL。之后 broker 的确认会清理该在途状态。连续重复 PUBREC 在首次 PUBREL 写出前合并；PUBREL 已尝试发送后收到重复 PUBREC，则再次发送 PUBREL。测试同步注入两个 PUBREC，确保发布任务处理首个确认前不会将第二个留在确认队列中。

若 `Session Present=0`，broker 已无旧会话，本库清除旧出站在途、入站去重和未消费消息，并对已确认的订阅自动重发 SUBSCRIBE。旧在途发布是否曾被 broker 或下游处理无法再判定。首次创建本地持久会话时若 broker 却返回 `Session Present=1`，本库拒绝连接：进程内缺少必要的客户端 QoS 状态，不能安全宣称恢复。若需要丢弃 broker 旧会话，应用应使用 clean session 建立新会话。

入站 QoS 2 使用标准图 4.3 的 Method B：首次 PUBLISH 成功入队并记 ID 后回 PUBREC；PUBREL 前同 ID 的 PUBLISH 只回 PUBREC，不重复交付；PUBREL 后回 PUBCOMP，重复 PUBREL 仍回 PUBCOMP。完成后同 ID 的新 PUBLISH 即使 `DUP=1` 也视为新消息。持久会话断线时保留去重状态及未消费队列，`Session Present=0` 时清除。对未知 ID 的 PUBREL 仍回复 PUBCOMP。QoS 1 已成功入队后回 PUBACK；同 ID 后续 PUBLISH 视为新消息。

接收队列是可配置的有界队列，读任务用 `try_put` 非阻塞入队。队列满时发出 `InboxFull` 并终止当前连接，不确认当前 QoS 1/2 报文，避免读任务卡住而无法处理其他确认。TCP 是字节流；`read_packet` 按 1 字节固定头、1–4 字节 Remaining Length 和精确正文长度读取，支持分片与合包。`write_lock` 串行化报文写入。

## 范围边界

本阶段仅支持**同进程跨 TCP 连接恢复**。没有磁盘状态保存；进程退出后，出站在途与入站 QoS 2 去重状态消失。没有自动重连、跨连接逐条完成回调、遗嘱消息、TLS、WebSocket 或 MQTT 5。后续顺序：先实现磁盘状态保存及崩溃恢复，再实现自动重连与退避，之后支持遗嘱消息，最后接入 TLS 与证书验证。
