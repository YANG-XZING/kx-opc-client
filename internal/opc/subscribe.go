package opc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"kx-opc-client/internal/model"
)

// 为点位创建监控项，服务端在值变化时推送（新版服务端赋值后调用了 ClearChangeMasks），
// 推送通过 Changes() 交给采集器。刚创建监控项时服务端会先推送一次全部点位的当前值。
//
// 服务端热更新 HMI_Value.XML 时会删除节点、或删除后用同一个 NodeId 重建节点（类型、文件夹、来源变了时），
// 原来的监控项不会收到任何通知，只是从此不再推送。所以采集器发现点位被删除或订阅失效时，
// 用 Unwatch 删掉对应的监控项，再 Watch 重新创建。

const (
	// publishInterval 服务端推送的最短间隔。远小于服务端约 1 秒的采集周期，保证每轮的变化单独推送、不被合并
	publishInterval    = 100 * time.Millisecond
	maxItemsPerRequest = 1000
	changeQueueSize    = 256
)

type subscription struct {
	sub     *opcua.Subscription
	notify  chan *opcua.PublishNotificationData
	changes chan model.Change
	done    chan struct{}

	mu         sync.Mutex
	lastHandle uint32                 // 已分配的最大句柄：只增不减，删掉的句柄不再复用，迟到的推送不会对错点位
	handles    map[uint32]string      // 监控项句柄 -> 点名
	items      map[string]watchedItem // 已成功创建监控项的点
}

type watchedItem struct {
	handle uint32 // 客户端句柄，推送里用它对应点名
	id     uint32 // 服务端分配的监控项 ID，删除监控项时用
}

// Watch 订阅点位：首次调用时创建订阅，之后只为还没有监控项的点位添加监控项。返回是否有点位已不存在。
func (c *Client) Watch(ctx context.Context, points []model.Point) (bool, error) {
	if c.sub == nil {
		s := &subscription{
			notify:  make(chan *opcua.PublishNotificationData, changeQueueSize),
			changes: make(chan model.Change, changeQueueSize),
			done:    make(chan struct{}),
			handles: map[uint32]string{},
			items:   map[string]watchedItem{},
		}
		sub, err := c.client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: publishInterval}, s.notify)
		if err != nil {
			return false, fmt.Errorf("创建订阅: %w", err)
		}
		s.sub = sub
		c.sub = s
		go s.forward()
	}
	s := c.sub

	var (
		requests []*ua.MonitoredItemCreateRequest
		names    []string
		handles  []uint32
	)
	s.mu.Lock()
	for _, p := range points {
		if _, ok := s.items[p.Name]; ok {
			continue
		}
		nodeID, err := ua.ParseNodeID(p.NodeID)
		if err != nil {
			s.mu.Unlock()
			return false, fmt.Errorf("非法 NodeId %q: %w", p.NodeID, err)
		}
		s.lastHandle++
		s.handles[s.lastHandle] = p.Name
		requests = append(requests, opcua.NewMonitoredItemCreateRequestWithDefaults(nodeID, ua.AttributeIDValue, s.lastHandle))
		names = append(names, p.Name)
		handles = append(handles, s.lastHandle)
	}
	s.mu.Unlock()

	unknown := false
	for start := 0; start < len(requests); start += maxItemsPerRequest {
		end := min(start+maxItemsPerRequest, len(requests))
		resp, err := s.sub.Monitor(ctx, ua.TimestampsToReturnBoth, requests[start:end]...)
		if err != nil {
			return unknown, fmt.Errorf("订阅 %d 创建监控项: %w", s.sub.SubscriptionID, err)
		}
		s.mu.Lock()
		for i := start; i < end; i++ {
			if i-start < len(resp.Results) && resp.Results[i-start].StatusCode == ua.StatusOK {
				s.items[names[i]] = watchedItem{handle: handles[i], id: resp.Results[i-start].MonitoredItemID}
				continue
			}
			delete(s.handles, handles[i]) // 没建成的下次 Watch 再试
			if i-start < len(resp.Results) && resp.Results[i-start].StatusCode == ua.StatusBadNodeIDUnknown {
				unknown = true
			}
		}
		s.mu.Unlock()
		for i, result := range resp.Results[:min(len(resp.Results), end-start)] {
			name := names[start+i]
			if c.log != nil && (result.StatusCode != ua.StatusOK || name == model.HeartbeatPoint || name == "HEART_UTC_TIME") {
				c.log.Debug("OPC 监控项创建结果", "订阅ID", s.sub.SubscriptionID, "点位", name,
					"监控项ID", result.MonitoredItemID, "状态", result.StatusCode.Error())
			}
			if name == model.HeartbeatPoint && result.StatusCode != ua.StatusOK && result.StatusCode != ua.StatusBadNodeIDUnknown {
				return unknown, fmt.Errorf("订阅 %d 创建 %s 监控项: %w", s.sub.SubscriptionID, name, result.StatusCode)
			}
		}
	}
	return unknown, nil
}

// Unwatch 删除这些点位的监控项（服务端删除或重建了节点时），之后再 Watch 会为仍在的点位重新创建。
// 服务端上节点已不存在时删除也会成功返回（结果里是错误码），不影响其他点位。
func (c *Client) Unwatch(ctx context.Context, names []string) error {
	s := c.sub
	if s == nil {
		return nil
	}
	var ids []uint32
	s.mu.Lock()
	for _, n := range names {
		if it, ok := s.items[n]; ok {
			ids = append(ids, it.id)
			delete(s.items, n)
			delete(s.handles, it.handle)
		}
	}
	s.mu.Unlock()
	for start := 0; start < len(ids); start += maxItemsPerRequest {
		if _, err := s.sub.Unmonitor(ctx, ids[start:min(start+maxItemsPerRequest, len(ids))]...); err != nil {
			return fmt.Errorf("删除监控项: %w", err)
		}
	}
	return nil
}

// Changes 返回服务端推送的变化，未订阅时返回 nil（读取会一直阻塞）。
func (c *Client) Changes() <-chan model.Change {
	if c.sub == nil {
		return nil
	}
	return c.sub.changes
}

// forward 把服务端推送转换为 model.Change，直到订阅关闭。
func (s *subscription) forward() {
	for {
		var n *opcua.PublishNotificationData
		select {
		case <-s.done:
			return
		case n = <-s.notify:
		}

		change := model.Change{Time: time.Now()}
		switch value := n.Value.(type) {
		case nil:
			change.Err = n.Error
		case *ua.StatusChangeNotification:
			change.Err = fmt.Errorf("订阅状态变化: %v", value.Status)
		case *ua.DataChangeNotification:
			change.Values = make(map[string]any, len(value.MonitoredItems))
			s.mu.Lock()
			for _, item := range value.MonitoredItems {
				name, ok := s.handles[item.ClientHandle]
				if !ok || item.Value == nil {
					continue
				}
				switch item.Value.Status {
				case ua.StatusOK:
					change.Values[name] = nativeValue(item.Value.Value)
				case ua.StatusBadNodeIDUnknown:
					change.Unknown = append(change.Unknown, name)
				}
			}
			s.mu.Unlock()
		default:
			continue
		}
		if n.Error != nil && change.Err == nil {
			change.Err = n.Error
		}
		if change.Err == nil && len(change.Values) == 0 && len(change.Unknown) == 0 {
			continue
		}

		select {
		case s.changes <- change:
		case <-s.done:
			return
		}
	}
}

// close 停止转发并取消服务端订阅。
func (s *subscription) close(ctx context.Context) {
	close(s.done)
	_ = s.sub.Cancel(ctx)
}
