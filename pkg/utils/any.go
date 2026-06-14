package utils

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// MarshalToAny 将 protobuf message 打包为 Any
// 业务方使用此函数将业务请求打包为 Any 传入 Bridge
func MarshalToAny(msg proto.Message) (*anypb.Any, error) {
	a, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal to any: %w", err)
	}
	return a, nil
}

// UnmarshalFromAny 从 Any 解包为指定类型
// 业务方使用此函数从 Bridge 响应中解包业务响应
func UnmarshalFromAny(any *anypb.Any, dst proto.Message) error {
	if err := anypb.UnmarshalTo(any, dst, proto.UnmarshalOptions{}); err != nil {
		return fmt.Errorf("unmarshal from any: %w", err)
	}
	return nil
}
