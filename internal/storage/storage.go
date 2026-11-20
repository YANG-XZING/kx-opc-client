// Package storage 定义存储端接口，并把采集快照分发给各存储端。
//
// 每个存储端有独立的队列和写入协程：某个存储端变慢或故障，只会丢它自己的数据，不影响采集和其他存储端。
// 新增存储端（如 MySQL）只需实现 Writer，并在启动时传给 Start。
package storage

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"kx-opc-client/internal/model"
)

const (
	queueSize    = 10_000
	dropLogEvery = 1000
)

// Writer 是一个存储端。
type Writer interface {
	// Name 用于日志
	Name() string
	// Accept 返回是否需要这台炉台的数据，例如 MySQL 只要指定的炉台
	Accept(f model.Furnace) bool
	// Run 是写入循环：从 in 读取快照并写入，in 关闭后写完剩余数据再返回
	Run(in <-chan model.Snapshot)
}

// Dispatcher 把快照分发给各存储端。
type Dispatcher struct {
	routes []*route
	log    *slog.Logger
	wg     sync.WaitGroup
}

type route struct {
	writer  Writer
	queue   chan model.Snapshot
	dropped atomic.Uint64
}

// Start 为每个存储端创建队列并启动写入协程。
func Start(log *slog.Logger, writers ...Writer) *Dispatcher {
	d := &Dispatcher{log: log}
	for _, w := range writers {
		r := &route{writer: w, queue: make(chan model.Snapshot, queueSize)}
		d.routes = append(d.routes, r)
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			w.Run(r.queue)
		}()
	}
	return d
}

// Publish 把快照投递给需要它的存储端，不阻塞：队列满时丢弃并告警。
func (d *Dispatcher) Publish(s model.Snapshot) {
	for _, r := range d.routes {
		if !r.writer.Accept(s.Furnace) {
			continue
		}
		select {
		case r.queue <- s:
		default:
			if n := r.dropped.Add(1); n == 1 || n%dropLogEvery == 0 {
				d.log.Warn("写库跟不上，队列已满，丢弃快照", "存储端", r.writer.Name(), "炉台", s.Furnace.Name(), "累计丢弃", n)
			}
		}
	}
}

// Close 关闭所有队列，等待各存储端写完剩余数据。调用后不能再 Publish。
func (d *Dispatcher) Close() {
	for _, r := range d.routes {
		close(r.queue)
	}
	d.wg.Wait()
}
