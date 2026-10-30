# MoonMQTT 设计记录（native TLS 阶段）

## 协议依据与依赖

- [OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) §3.1.2.4、§3.2.2.2：`CleanSession=0` 时会话以稳定 Client ID 标识；CONNACK `Session Present` 表明 broker 是否保留旧会话。客户端会话状态包括未完全确认的出站 QoS 1/2 和未完全确认的入站 QoS 2；broker 会话另包含订阅。`Session Present=0` 代表新会话。
- 同一规范 §4.3.3、§4.4、§4.6：出站 QoS 2 收到 PUBREC 后只能重发 PUBREL；重连且 `CleanSession=0` 时按原 ID 重发未确认的 PUBLISH 或 PUBREL；重发 PUBLISH 保持原始顺序并设置 DUP。PUBREL 固定头保留位仍为 `0x62`。
- 同一规范 §3.1.2.5、§3.1.2.8、§3.14：CONNECT 可携带 Will Topic、二进制 Will Message、Will QoS 和 Will Retain；broker 收到 DISCONNECT 时不发布该网络连接的遗嘱，未收到时关闭网络连接会触发遗嘱。每次重连都是新网络连接，须再次携带遗嘱配置。
- `moonbitlang/async` 0.22.5 提供 TCP、TLS、任务组、队列、互斥锁、超时及 native 文件操作；`zbhzs1/moonbit-mqtt` 0.1.0 提供 MQTT 报文编解码。codec 的 `packet_total_length` 不能区分不完整缓冲与非法报文，因此本库先读取固定头和 Remaining Length 指定的完整帧，再交给 codec。TLS 使用 async 的 `Tls::client`：`host` 同时用于主机名验证和 SNI；`SystemRoot` 为默认信任集合，`CustomPemFile` 为应用指定的独立 PEM CA 集合。

## 代码结构和公开 API

| 路径 | 用途 |
| --- | --- |
| `moonmqtt.mbt` | 客户端 API、TCP 帧读取、连接任务、请求/会话状态、QoS 1/2 恢复 |
| `auto_client.mbt` | 稳定自动句柄、状态、可取消指数退避与错误分类 |
| `session_store.mbt` | 版本化会话快照、校验与验证、可替换存储接口和 native 文件存储 |
| `moonmqtt_wbtest.mbt` | 初始连接及流式读写回归 |
| `qos2_wbtest.mbt` | QoS 2 双向握手、重复 PUBREC、取消、错误报文 |
| `session_wbtest.mbt` | 会话重连、各阶段断线、订阅恢复、在途 ID 与顺序 |
| `persistence_wbtest.mbt` | 文件快照、损坏检测、进程级重建和存储故障断点 |
| `auto_wbtest.mbt` | 模拟 broker 自动恢复、退避、停止/取消及永久错误 |
| `will_wbtest.mbt` | 遗嘱配置、CONNECT 字段、每代重连与停止发送失败的模拟 broker 测试 |
| `tls_wbtest.mbt` | TLS ClientHello SNI、握手超时、握手中停止、握手时 TCP 断线重试与 CA 配置错误回归 |
| `examples/basic`、`examples/session_resume` | 最小单连接使用、同进程续接示例 |
| `examples/process_resume` | 独立 native 进程共享快照路径的恢复示例 |
| `examples/auto_resume` | 稳定自动句柄及 broker 重启后的订阅恢复示例 |
| `examples/will_probe`、`tests/will_interop.ps1` | Mosquitto 遗嘱主题、QoS、retain、异常断线和自动重连互通 |
| `examples/tls_probe`、`tests/tls_interop.ps1` | Mosquitto TLS listener、证书拒绝、双向 QoS、持久会话与自动重连互通 |
| `examples/auth_probe`、`tests/auth_interop.ps1` | Mosquitto 禁止匿名的独立 listener；公开用户名/密码配置、QoS 1 收发和两种明确的认证拒绝 |
| `examples/interop_pub`、`examples/interop_sub`、`tests/interop.ps1` | Mosquitto CLI 双向互通和真实会话续接 |

`Config::new(host, client_id, port?)` 后可配置 `clean_session`（默认 `true`）、用户名/密码、Keep Alive、超时、报文上限和接收队列容量。`clean_session=false` 时可设置 `config.session_store = Some(SessionStore::file(path))`，也可用 `SessionStore::new(load, save)` 接入应用存储。`Client::connect(config, group)` 先读取并校验快照，再建立 TCP 和完成 CONNECT/CONNACK；无快照时才创建新本地会话。`Client::session_present()` 返回本次 CONNACK 状态。旧 `Client::reconnect(group)` 沿用同进程内存会话；进程重启则以相同 Client ID 和存储重新调用 `Client::connect`。不能对 clean session 配置存储或调用 `reconnect`，同一会话也不能同时连接两条 TCP。

认证互通通过公开的 `Config.username` 和二进制 `Config.password` 设置 CONNECT 字段，在随机生成的 Mosquitto password file 上验证 QoS 1 回环。错误密码和不带凭据的连接只接受 `Rejected(NotAuthorized)` 或 `Rejected(BadUserNameOrPassword)` 作为预期结果；超时、TCP 失败或其他异常均令测试失败。password file、凭据和该 listener 的日志位于系统临时目录，不属于发布归档或 CI 诊断 artifact。

`config.will = Some(Will::new(topic, payload, qos?, retain?))` 在每次 CONNECT 构造时取值。主题不能为空、不能有主题过滤通配符，且须通过 MQTT UTF-8 字符串编码；负载是任意字节，受 CONNECT 中两字节长度字段限制，最多 65535 字节。配置在连接前验证，非法值抛 `InvalidConfig`。遗嘱不属于客户端持久会话快照；重新建立 TCP 时按当前固定配置再次发送，broker 对每一代连接分别管理其遗嘱。

`config.tls = Some(TlsConfig::new(ca_file?))` 选择 native TLS；未配置时沿用明文 TCP。指定 CA 时，库在 TCP 连接前检查文件可读，不可读报 `InvalidConfig`。TCP 连接后以 `config.host` 调用 `Tls::client`，使用系统根或指定 PEM CA 完成链与主机名验证，并发送 SNI；握手受 `timeout_ms` 限制，只有成功后才构造和发送 MQTT CONNECT。TLS 验证/握手错误映射为不可重试的 `TlsValidation`；TCP 连接失败、超时及已建立连接后的临时断线沿用现有重试策略。无关闭跳过验证的公开开关；当前不提供客户端证书认证。

`Client` 持有底层 TCP 和可选 TLS 会话。帧读取通过 `@io.Reader` 泛型共用分片读取逻辑，发送按传输类型写入；清理时先关闭 TLS 对象再关闭 TCP。连接、握手或 MQTT CONNACK 任一阶段失败都会释放已取得的资源。手动重连和 `AutoClient` 每代连接均复用同一 TLS 建立路径，持久会话快照不保存 TLS 会话。

`subscribe(filter, qos)` 返回授予的 QoS，`unsubscribe(filter)` 等待 UNSUBACK；`publish(topic, payload, qos?, retain?)` 对 QoS 0 等待传输层写入、QoS 1 等待 PUBACK、QoS 2 等待 PUBCOMP；`receive()` 取下一条消息。`disconnect()` 发送 DISCONNECT 并关连接，`close()` 直接关连接。二者均等待后台读/心跳任务。`with_client` 管理单次连接的任务组；显式重连应自行管理任务组及每一代 `Client`。公开存储错误为 `Storage`、`CorruptSession`、`IncompatibleSessionVersion`、`SessionClientIdMismatch`、`RecoveryUncertain`；其他连接错误仍包括 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed`、`TimedOut`、`InboxFull`、`DisconnectUncertain`、`TlsValidation`。底层 socket 错误及取消可能直接传播。

`AutoClient::start(config, group, policy?)` 返回独立于 TCP 世代的句柄；`status()` 提供 `Connecting`、`Connected(Session Present)`、`Retrying(连续失败次数, 下次延迟毫秒)`、`Failed`、`Stopped`，`last_error()` 提供最近原因。`wait_connected()`、`publish()`、`subscribe()`、`unsubscribe()`、`receive()` 和 `stop()` 都从稳定句柄调用。启动前验证配置并加载快照，因而非法配置或快照读取失败由 `start` 直接返回错误。自动句柄只在当前连接上执行每次应用层 publish/subscribe/unsubscribe；调用中途断线向调用方报错，不透明重调。稳定 `receive()` 在可重试断线后等待下一连接；已返回消息仍遵循原有持久消费边界。

后台 supervisor 持有同一 `Session`，每代连接复用 `connect_with_session`，故 SP1 的原 ID/PUBREL 重发、SP0 清理与重订阅、存储故障封锁均沿用手动模式。正常连接后重置失败次数；网络/握手中断、TCP 连接失败和心跳超时按 `initial_delay_ms`、两倍增长、`max_delay_ms` 上限重试。配置、编解码、认证拒绝、协议、`RecoveryUncertain`、存储、`InboxFull`、`TlsValidation` 为永久错误。`stop()` 用互斥锁串行化并发调用。首个调用标记停止；若当前连接可用，在请求超时内发送 DISCONNECT，然后关闭连接、取消 supervisor 并等待后台任务。后续调用等待这次清理完成，并复用其结果，不能仅因已标记 `Stopped` 就提前返回。即使写入成功，MQTT 3.1.1 也没有 DISCONNECT 响应；带遗嘱时清理后抛 `DisconnectUncertain`，表明无法确认 broker 是否已撤销遗嘱。任务组取消直接关闭连接；连接及退避等待均可取消。其任务组必须覆盖句柄及所有并发操作；同一 Client ID 和快照路径不能另启第二个活动句柄。

自动重连无法阻止此前连接的遗嘱：前代 TCP 因网络故障关闭时，broker 可能已经发布前代遗嘱；后代 CONNECT 只为新连接登记同一配置。正常停止若 DISCONNECT 只写入本地传输层、尚未被 broker 收到便断线，也存在遗嘱被发布的可能；即使本地写入成功，`stop()` 对带遗嘱连接也报告不确定。`Client::close()` 继续作为直接关闭连接的操作；强制终止进程与任务组取消同样不发送 DISCONNECT。

停止竞态使用 MQTT CONNECT 的发送边界记录：supervisor 在发送 CONNECT 前标记本代可能已登记遗嘱；如果连接在 CONNACK 前关闭，或进入退避，且配置了 Will，就保留此前代的不确定性。`stop()` 停止新尝试、在当前连接可用时发 DISCONNECT、取消并等待 supervisor 后，再检查握手标记与此前代记录。即使新连接已正常 DISCONNECT，此前代遗嘱仍可能已发布，故报告 `DisconnectUncertain`。在 TCP/TLS 握手阶段尚未发送 MQTT CONNECT 时停止则无遗嘱风险。模拟 broker 测试覆盖 CONNACK 等待、退避、前代异常断开后本代正常停止、TLS 握手中停止。

## 同进程会话状态

每个初始 `Client` 拥有一份 `Session`；同进程重连复用它，进程重启则从存储重建。会话记录稳定 Client ID、下一个报文 ID、出站在途消息及原始发送顺序、入站 QoS 2 去重状态、已确认的订阅，以及尚未被 `receive()` 消费的消息。每代 `Client` 独占 TCP、读/心跳任务、响应队列和临时请求表。旧连接关闭时清空临时表并唤醒等待者；旧 socket 的迟到响应无法进入新连接的临时表。

`finish()` 按会话类型收尾：clean session 清空出站、入站去重、订阅和未消费消息；无存储的持久会话把旧连接接收队列的未消费消息收拢到 `Session.backlog`，保留其余会话状态；有存储的持久会话不改动已落盘的会话字段，因为 `backlog` 本身就是快照的未消费消息表。异步保存由 `persist_lock` 串行化且受取消保护；若关闭与保存交错，关闭不会把清空的状态写回快照，保存完成后该操作也不能继续向已关闭的 TCP 发送。写失败标记 `storage_failed`，旧 `Client::reconnect()` 拒绝复用；应用须从最后完整快照重建。回归测试分别覆盖主动关闭、异常断线、保存中关闭及三类会话的收尾行为。

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

本阶段支持 native 平台的**手动或可选自动同进程续接、可选的跨进程快照恢复、遗嘱配置，以及经验证的 MQTT over TLS**。没有跨连接逐条完成回调、应用消息处理事务、TLS 客户端证书认证、WebSocket 或 MQTT 5。
