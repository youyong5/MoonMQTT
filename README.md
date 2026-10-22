# MoonMQTT

`youyong5/moonmqtt` 是供其他 MoonBit 程序使用的 MQTT 3.1.1 客户端库。第一阶段通过真实 TCP socket 连接 broker，并使用 [`zbhzs1/moonbit-mqtt` 0.1.0](https://mooncakes.io/docs/zbhzs1/moonbit-mqtt) 编解码报文，使用 [`moonbitlang/async` 0.22.5](https://mooncakes.io/docs/moonbitlang/async) 管理异步 I/O。当前只支持 MoonBit **native** 目标与明文 TCP。

## 已实现

- CONNECT/CONNACK，clean session，client ID，可选用户名和密码；拒绝连接会返回具体 CONNACK 原因。
- SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK；单次调用操作一个 topic filter，订阅返回 broker 授予的 QoS。
- QoS 0 和 QoS 1 PUBLISH 双向收发。QoS 1 发送在匹配的 PUBACK 到达后才完成；收到 QoS 1 消息会回复 PUBACK。支持发布 retain 位，收到的消息保留 retain 和 dup 位。
- 周期 PINGREQ/PINGRESP、DISCONNECT；Keep Alive 可设为 0 关闭心跳。
- TCP 分片与连续多个报文的流式读取、报文大小限制、packet ID 分配与响应匹配、连接/写入/确认超时。超时或异常关闭时清理待处理请求并唤醒接收者。

当前**不声称完整符合 MQTT 3.1.1**：没有 QoS 2、持久会话与重发、自动重连、遗嘱消息、TLS/WebSocket。QoS 1 不跨连接保存消息，因此断线后不保证重新投递。接收队列无容量上限，应用应持续消费。

## 最小用法

模块尚未发布到 Mooncakes；仓库内的 `examples` 包已按外部使用方方式导入它。发布后，其他项目可先运行 `moon add youyong5/moonmqtt`，再在 `moon.pkg` 中导入本库、`moonbitlang/async` 和 QoS 类型所在的编解码包：

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
  config.keep_alive_seconds = 30
  config.timeout_ms = 5000
  @client.with_client(config, client => {
    let granted = client.subscribe("demo/#", @packet.QoS1)
    client.publish("demo/hello", b"hello", qos=granted)
    let message = @async.with_timeout(5000, () => client.receive())
    println(message.topic)
    client.unsubscribe("demo/#")
  })
}
```

`Config::new` 默认端口 1883、Keep Alive 30 秒、超时 5 秒、最大报文 1 MiB。连接前还可设置 `username`、`password`（`Bytes?`）和 `max_packet_size`。不要在连接后修改传入的配置。

`with_client` 在回调正常完成时发送 DISCONNECT，异常时关闭连接。若需手动管理生命周期，可在 `@async.with_task_group` 内调用 `Client::connect(config, group)`，结束前调用异步 `client.disconnect()` 或 `client.close()`；任务组必须覆盖全部客户端操作。`close()` 可以重复调用。`receive()` 本身持续等待，调用者可以用 `@async.with_timeout` 限定等待时间。

`MqttError` 包含 `InvalidConfig`、`Codec`、`Protocol`、`Rejected`、`Closed` 和 `TimedOut`；底层 async socket 错误和取消也可能直接传播。一次请求超时后连接会关闭，避免迟到确认匹配到以后复用的 packet ID。

## 本地运行和测试

```powershell
cd D:\Projects\MoonMQTT
moon check --target native
moon test --target native
moon build --target native
```

安装 Mosquitto（例如 `winget install -e --id EclipseFoundation.Mosquitto --source winget`）后，启动本地 broker（2.x/2.1.x，默认本机监听 1883），再运行库使用示例：

```powershell
& 'C:\Program Files\mosquitto\mosquitto.exe' -p 1883 -v
# 另一个 PowerShell 窗口：
moon run examples/basic --target native
```

双向 CLI 互通脚本在安装 Mosquitto 后运行：

```powershell
powershell -ExecutionPolicy Bypass -File tests\interop.ps1
```

脚本验证最小示例、MoonBit QoS 0/1 发布 → `mosquitto_sub` 收取，以及 `mosquitto_pub` QoS 0/1 发布 → MoonBit 订阅者收取。可用 `-MosquittoDirectory` 指定其他安装目录。脚本会自行启动并关闭 broker，运行它时无需先执行上面的启动命令。测试日志保存在 `.tools/interop`。手动运行两端的程序位于 `examples/interop_pub` 和 `examples/interop_sub`。

仓库中的 10 个 native 测试使用本地 TCP 假 broker，覆盖拒绝连接、异常断开、错误报文、分片、合包、QoS 0/1、心跳、确认匹配和超时。**本机另用 MSYS2 打包的 Mosquitto 2.0.22 broker 与命令行客户端运行 `tests/interop.ps1`，最小示例以及 QoS 0/1 双向互通均通过。**

## 设计与后续

[设计记录](docs/design.md)记录了标准依据、依赖选择、与现有编解码包的差异和异步生命周期。下一阶段按以下顺序扩展：QoS 2 完整双向状态机；持久会话/恢复与在途消息重发；自动重连与退避；遗嘱消息；TLS 传输和证书验证。
