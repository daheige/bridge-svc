package router

import (
	"sync/atomic"
)

// LoadBalancer 负载均衡接口
type LoadBalancer interface {
	Select(endpoints []*Endpoint, ctx RouteContext) *Endpoint
}

// WeightedRoundRobin 加权轮询负载均衡器
type WeightedRoundRobin struct {
	counter uint64
}

// NewWeightedRoundRobin 创建加权轮询器
func NewWeightedRoundRobin() *WeightedRoundRobin {
	return &WeightedRoundRobin{}
}

// Select 按权重选择下游节点
func (w *WeightedRoundRobin) Select(endpoints []*Endpoint, ctx RouteContext) *Endpoint {
	totalWeight := uint32(0)
	for _, ep := range endpoints {
		totalWeight += ep.Weight
	}

	if totalWeight == 0 {
		return endpoints[0]
	}

	count := atomic.AddUint64(&w.counter, 1)
	pos := uint32((count - 1) % uint64(totalWeight))
	var current uint32
	for _, ep := range endpoints {
		current += ep.Weight
		if pos < current {
			return ep
		}
	}

	return endpoints[0]
}
