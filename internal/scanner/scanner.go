// Package scanner 探测炉台的 OPC 端口是否能连通，用于筛选 IP 范围中实际存在的炉台。
package scanner

import (
	"context"
	"net"
	"strconv"
	"sync"
	"time"

	"kx-opc-client/internal/model"
)

const (
	probeTimeout = time.Second
	concurrency  = 256
)

// ProbeFunc 探测 addr（host:port）能否连通。
type ProbeFunc func(ctx context.Context, addr string) bool

// TCPProbe 用 TCP 连接探测端口，连上即断开，不登录 OPC。
func TCPProbe(ctx context.Context, addr string) bool {
	conn, err := (&net.Dialer{Timeout: probeTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Reachable 并发探测，返回端口能连通的炉台，顺序与输入一致。
func Reachable(ctx context.Context, furnaces []model.Furnace, probe ProbeFunc) []model.Furnace {
	alive := make([]bool, len(furnaces))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for i, f := range furnaces {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			alive[i] = probe(ctx, net.JoinHostPort(f.IP, strconv.Itoa(f.Port)))
		}()
	}
	wg.Wait()

	var found []model.Furnace
	for i, ok := range alive {
		if ok {
			found = append(found, furnaces[i])
		}
	}
	return found
}
