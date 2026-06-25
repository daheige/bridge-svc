# bridge
使用示例
```go
cfg, err := bridge.LoadConfig("./app.yaml")
if err != nil {
    log.Fatal(err)
}

client, err := bridge.NewClient(cfg) // 地址优先从 cfg.Bridge.Address 读取
if err != nil {
    log.Fatal(err)
}
defer client.Close()

req := &pb.HelloReq{Name: "daheige"}
resp, err := client.Call(ctx, "greeter-svc", "SayHello", req,
    bridge.WithMetadata(map[string]string{"x-request-id": "req-456"}),
    bridge.WithTimeout(3*time.Second),
)
if err != nil {
    log.Fatal(err)
}

var reply pb.HelloReply
if err := bridge.Into(resp, &reply); err != nil {
    log.Fatal(err)
}
```

# 配置文件兼容
支持你的 services 格式，并额外提供 bridge.address 指定 bridge-svc 地址，以及可选的 service 字段声明 gRPC 完整服务名：
```yaml
bridge:
address: "localhost:50052"
timeout: 5s

services:
    - name: uc-svc
      target: uc.cluster.local:8080
      service: "Uc.UserService"
    - name: greeter-svc
      target: greeter.cluster.local:8080
      service: "Hello.Greeter"  # 为空时默认使用 name 构造 target
```

如果 service 为空，client.Call("greeter-svc", "SayHello", ...) 会构造 target 为 greeter-svc/SayHello；配置了 service 则使用 Hello.Greeter/SayHello。
