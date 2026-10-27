// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "youyong5/moonmqtt"

version = "0.1.1"

readme = "README.md"

repository = "https://github.com/youyong5/MoonMQTT"

license = "Apache-2.0"

keywords = [ ]

preferred_target = "native"

description = "MQTT 3.1.1 client library for MoonBit with TCP and TLS transports"

import {
  "moonbitlang/async@0.22.5",
  "zbhzs1/moonbit-mqtt@0.1.0",
}
