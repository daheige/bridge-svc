package bridge

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/daheige/hello-pb/pb"
)

func ExampleClient() {
	cfg, err := LoadConfig("./app.example.yaml")
	if err != nil {
		log.Fatal(err)
	}

	client, err := NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.HelloReq{Name: "daheige"}
	var resp pb.HelloReply
	if err := client.Invoke(ctx, "greeter-svc", "SayHello", req, &resp); err != nil {
		log.Fatal(err)
	}

	fmt.Println(resp.Message)
}
