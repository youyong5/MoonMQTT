# MoonMQTT 设计记录（第四阶段）

## 协议依据与依赖

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) §3.1.2.4、§3.2.2.2：`CleanSession=0` 时会话以稳定 Client ID 标识；CONNACK `Session Present` 表明 broker 是否保留旧会话。客户端会话状态包括未完全确认的出站 QoS 1/2 和未完全确认的入站 QoS 2；broker 会话另包含订阅。`Session Present=0` 代表新会话。
- 同一规范 §4.3.3、§4.4、§4.6：出站 QoS 2 收到 PUBREC 后只能重发 PUBREL；重连且 `CleanSession=0` 时按原 ID 重发未确认的 PUBLISH 或 PUBREL；重发 PUBLISH 保持原始顺序并设置 DUP。PUBREL 固定头保留位仍为 `0x62`。
- `moonbitlang/async` 0.22.5 提供 TCP、任务组、队列、互斥锁、超时及 native 文件操作；`zbhzs1/moonbit-mqtt` 0.1.0 提供 MQTT 报文编解码。codec 的 `packet_total_length` 不能区分不完整缓冲与非法报文，因此本库先读取固定头和 Remaining Length 指定的完整帧，再交给 codec。TLS 包存在，但尚未接入本库。

## 代码结构和公开 API

| 路径 | 用途 |
| --- | --- |
| `moonmqtt.mbt` | 客户端 API、TCP 帧读取、连接任务、请求/会话状态、QoS 1/2 恢复 |
| `session_store.mbt` | 版本化会话快照、校验与验证、可替换存储接口和 native 文件存储 |
| `moonmqtt_wbtest.mbt` | 初始连接及流式读写回归 |
| `qos2_wbtest.mbt` | QoS 2 双向握手、重复 PUBREC、取消、错误报文 |
| `session_wbtest.mbt` | 会话重连、各阶段断线、订阅恢复、在途 ID 与顺序 |
| `persistence_wbtest.mbt` | 文件快照、损坏检测、进程级重建和存储故障断点 |
| `examples/basic`、`examples/session_resume` | 最小单连接使用、同进程续接示例 |
| `examples/process_resume` | 独立 native 进程共享快照路径的恢复示例 |
| `examples/interop_pub`、`examples/interop_sub`、`tests/interop.ps1` | Mosquitto CLI 双向互通和真实会话续接 |

`Config::new(host, client_id, port?)` 后可配置 `clean_session`（默认 `true`）、用户名/密码、Keep Alive、超时、报文上限和接收队列容量。`clean_session=false` 时可设置 `config.session_store = Some(SessionStore::file(path))`，也可用 `SessionStore::new(load, save)` 接入应用存储。`Client::connect(config, group)` 先读取并校验快照，再建立 TCP 和完成 CONNECT/CONNACK；无快照时才创建新本地会话。`Client::session_present()` 返回本次 CONNACK 状态。旧 `Client::reconnect(group)` 沿用同进程内存会话；进程重启则以相同 Client ID 和存储重新调用 `Client::connect`。不能对 clean session 配置存储或调用 `reconnect`，同一会话也不能同时连接两条 TCP。

`subscribe(filter, qos)` 返回授予的 QoS，`unsubscribe(filter)` 等待 UNSUBACK；`publish(topic, payload, qos?, retain?)` 对 QoS 0 等待 TCP 写入、QoS 1 等待 PUBACK、QoS 2 等待 PUBCOMP；`receive()` 取下一条消息。`disconnect()` 发送 DISCONNECT 并关 TCP，`close()` 直接关 TCP。二者均等待后台读/心跳任务。`with_client` 管理单次连接的任务组；显式重连应自行管理任务组及每一代 `Client`。公开存储错误为 `Storage`、`CorruptSession`、`IncompatibleSessionVersion`、`SessionClientIdMismatch`、`RecoveryUncertain`；其他连接错误仍包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`、`InboxFull`。底层 socket 错误及取消可能直接传播。

## 同进程会话状态

每个初始 `Client` 拥有一份 `Session`；同进程重连复用它，进程重启则从存储重建。会话记录稳定 Client ID、下一个报文 ID、出站在途消息及原始发送顺序、入站 QoS 2 去重状态、已确认的订阅，以及尚未被 `receive()` 消费的消息。每代 `Client` 独占 TCP、读/心跳任务、响应队列和临时请求表。旧连接关闭时清空临时表并唤醒等待者；旧 socket 的迟到响应无法进入新连接的临时表。

新报文 ID 在同步 `reserve()` 中立即进入临时表，分配时同时跳过会话里未确认的出站 ID。存储模式下，SUBSCRIBE/UNSUBSCRIBE 的下一个 ID 在发送前保存；QoS 1/2 发送记录及其 ID 也在发送前保存。收到 PUBACK 或 PUBCOMP 并保存删除记录后才允许后续连接复用 ID。超时、取消、写失败、异常断开使当前连接终止；持久会话保留已登记的 QoS 1/2 在途消息，clean session 则清除。旧 `publish` 调用仍报错；随后恢复的确认不会回填到原调用，目前没有逐条跨连接完成通知。已确认的订阅记在会话中；尚未得到 SUBACK/UNSUBACK 的操作结果不确定，调用方应重试。

若重连 CONNACK 的 `Session Present=1`，本库在返回新客户端前按原顺序重发出站在途报文：等待 PUBACK/PUBREC 的消息发送原 ID 且 `DUP=1` 的 PUBLISH；已经收到 PUBREC 的消息仅重发原 ID 的 PUBREL。之后 broker 的确认会清理该在途状态。连续重复 PUBREC 在首次 PUBREL 写出前合并；PUBREL 已尝试发送后收到重复 PUBREC，则再次发送 PUBREL。测试同步注入两个 PUBREC，确保发布任务处理首个确认前不会将第二个留在确认队列中。

若 `Session Present=0`，broker 已无旧会话，本库清除旧出站在途、入站去重和未消费消息，先保存清理后的快照，再对已确认的订阅自动重发 SUBSCRIBE。旧在途发布是否曾被 broker 或下游处理无法再判定。首次创建本地持久会话时若 broker 却返回 `Session Present=1`，本库拒绝连接：缺少必要的客户端 QoS 状态，不能安全宣称恢复。若需要丢弃 broker 旧会话，应用应使用 clean session 建立新会话。

SP0 清理时还持久记录“待重订阅”；只有所有已确认过滤器重新收到 SUBACK 并保存后才清除标记。若进程在重订阅中退出，下一次即使 broker 回 SP1 也会再次发送这些 SUBSCRIBE。重复订阅可能触发 broker 再发 retained 消息，应用应按其业务语义处理。

入站 QoS 2 使用标准图 4.3 的 Method B：首次 PUBLISH 成功入队并记 ID 后回 PUBREC；PUBREL 前同 ID 的 PUBLISH 只回 PUBREC，不重复交付；PUBREL 后回 PUBCOMP，重复 PUBREL 仍回 PUBCOMP。完成后同 ID 的新 PUBLISH 即使 `DUP=1` 也视为新消息。持久会话断线时保留去重状态及未消费队列，`Session Present=0` 时清除。对未知 ID 的 PUBREL 仍回复 PUBCOMP。QoS 1 已成功入队后回 PUBACK；同 ID 后续 PUBLISH 视为新消息。

接收队列是可配置的有界队列，读任务用 `try_put` 非阻塞入队。存储模式下，未消费消息先写入会话快照，随后才进入内存队列；快照队列与内存队列受同一容量限制。队列满时发出 `InboxFull` 并终止当前连接，不确认当前 QoS 1/2 报文，避免读任务卡住而无法处理其他确认。TCP 是字节流；`read_packet` 按 1 字节固定头、1–4 字节 Remaining Length 和精确正文长度读取，支持分片与合包。`write_lock` 串行化报文写入。

## native 会话存储与崩溃窗口

快照格式以 `MMQS` 魔数、版本 3、长度限定字段和 Adler-32 校验尾组成，最大 64 MiB。解码检查 Client ID、报文 ID 范围及唯一性、QoS 与出站阶段组合、订阅和消息字段、尾部多余数据。损坏、版本不兼容和 Client ID 不匹配分别抛出明确错误，绝不默默新建会话。`SessionStore::new(load, save)` 供应用接入其他持久后端，回调须保证完整原子替换与持久写入；`SessionStore::file(path)` 在同目录写临时文件、同步文件，再原子替换主文件。临时文件在进程崩溃后可留存，读取时只认主文件。一个快照路径只允许一个活动进程；当前没有跨进程文件锁，也不承诺断电后的目录项持久性。

| 窗口 | 持久化顺序与恢复结果 |
| --- | --- |
| 出站 QoS 1/2 首次 PUBLISH | 先保存消息、原 ID 和等待阶段，再写 TCP；保存失败不发送，保存后崩溃则以原 ID、`DUP=1` 重发 |
| 出站 QoS 2 PUBREC → PUBREL | 收到 PUBREC 后先保存 `WaitPubcomp`，再通知发布任务或写 PUBREL；已落盘的 PUBREL 阶段不会退回 PUBLISH，断线只重发 PUBREL。若崩溃发生在读到 PUBREC 与保存阶段之间，磁盘仍是 `WaitPubrec`，恢复时重发 PUBLISH，由 broker 再回 PUBREC |
| 出站 PUBACK/PUBCOMP | 先删除在途记录并保存，再唤醒等待者；保存失败即关闭连接且同进程会话不可继续，重启后仍保留旧 ID。QoS 1 在该故障窗口可能重复投递，符合其至少一次语义 |
| 入站 QoS 2 PUBLISH → PUBREC | 先原子保存未消费消息与去重 ID，再发送 PUBREC；保存失败不确认。崩溃发生在保存与 PUBREC 之间时，broker 重发 PUBLISH，只重复 PUBREC，不重复入队 |
| 入站 PUBREL → PUBCOMP | 先保存完成标记，再发送 PUBCOMP；保存失败不确认，broker 重发 PUBREL 后仍幂等回复 PUBCOMP |
| `receive()` | 从内存队列取出后，先从持久队列删除并保存，再向调用方返回；保存失败使连接终止。保存删除成功后即使调用方随后崩溃，也不会再次交付该队列条目 |
| CONNECT/CONNACK | 发送 CONNECT 前先保存“连接尝试中”标记；收到 CONNACK 并完成 `Session Present` 处理后才清除。若重启加载到该标记且 broker 回 1，报 `RecoveryUncertain`，以免把先前 `Session Present=0` 后未落盘的旧状态当作可恢复；回 0 时可清理旧状态并恢复订阅 |

保存失败会将当前内存会话标记为不可继续；应用须修复存储后重新 `Client::connect()` 读取最后一个完整快照。进程崩溃发生在接收 PUBREC 与阶段落盘之间，以及连接尝试标记未清除时，恢复可能需要 broker 再次参与协议；对不确定的 `Session Present=1` 采取拒绝而不是猜测。`receive()` 的删除与应用自身副作用之间没有共同事务，故库只保证协议层的 QoS 2 去重，不保证应用处理的绝对恰好一次；调用方如需更强保证，应将消息处理结果和应用确认放入同一事务设计。未完成的 SUBSCRIBE/UNSUBSCRIBE 在崩溃前后仍可能有不确定结果。

## 范围边界

本阶段支持 native 平台的**同进程续接和可选的跨进程快照恢复**。没有自动重连、跨连接逐条完成回调、应用消息处理事务、遗嘱消息、TLS、WebSocket 或 MQTT 5。后续可依次加入自动重连与退避、遗嘱消息、TLS 与证书验证。
