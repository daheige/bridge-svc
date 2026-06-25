// Package bridge 提供基于 services 配置的下游 gRPC 服务客户端 SDK。
//
// 它读取 app.yaml 中的 services 列表，为每个服务创建 gRPC 连接，
// 并允许按服务名直接调用下游方法，无需业务方手动管理连接。
//
// 配置文件示例：
//
//	services:
//	  - name: uc-svc
//	    target: uc.cluster.local:8080
//	  - name: rbac-svc
//	    target: rbac.cluster.local:8080
//	    service: "rbac.RBAC"
//	  - name: greeter-svc
//	    target: greeter.cluster.local:8080
//	    service: "Hello.Greeter"
//	    timeout: 3s
//	    metadata:
//	      x-service: "greeter"
//
// 基本用法：
//
//	cfg, err := bridge.LoadConfig("./app.yaml")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	client, err := bridge.NewClient(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	req := &pb.HelloReq{Name: "daheige"}
//	var resp pb.HelloReply
//	if err := client.Invoke(ctx, "greeter-svc", "SayHello", req, &resp); err != nil {
//	    log.Fatal(err)
//	}
package bridge
