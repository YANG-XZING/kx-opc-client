// Package opc 封装与一台炉台 OPC UA 服务端（KXOpcServices）的会话：匿名连接、读取点位列表、
// 订阅点位并接收服务端推送的变化（见 subscribe.go）。
package opc

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"kx-opc-client/internal/model"
)

const (
	// namespaceURI 是服务端自定义命名空间（见 KX.OPCServer/CustomNodeManagerAbs.cs）
	namespaceURI = "http://your-namespace-url"
	// serverNamePrefix 是服务端 OPC 应用名的前缀，应用名 = 前缀 + 上位机设备名（见 OpcSvrApplication.cs）
	serverNamePrefix = "LintonKXOpcServer_"

	dialTimeout    = 5 * time.Second
	requestTimeout = 10 * time.Second
	sessionTimeout = time.Minute
)

// Client 是与一台炉台的 OPC UA 会话，不要求并发安全（每台炉台一个采集协程）。
type Client struct {
	client     *opcua.Client
	namespace  uint16
	deviceName string
	log        *slog.Logger
	sub        *subscription // 首次 Watch 时创建
}

// Dial 连接服务端并创建会话。服务端只开放 None 安全策略 + 匿名登录。
func Dial(ctx context.Context, endpoint string, log *slog.Logger) (*Client, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout+requestTimeout)
	defer cancel()

	endpoints, err := opcua.GetEndpoints(ctx, endpoint, opcua.DialTimeout(dialTimeout))
	if err != nil {
		return nil, fmt.Errorf("获取端点: %w", err)
	}
	ep, err := opcua.SelectEndpoint(endpoints, ua.SecurityPolicyURINone, ua.MessageSecurityModeNone)
	if err != nil {
		return nil, fmt.Errorf("服务端没有 None 安全策略的端点: %w", err)
	}
	// 服务端可能对外公布 127.0.0.1 等地址，始终使用配置里的地址连接
	ep.EndpointURL = endpoint

	client, err := opcua.NewClient(endpoint,
		opcua.ApplicationName("kx-opc-client"),
		opcua.SecurityFromEndpoint(ep, ua.UserTokenTypeAnonymous),
		opcua.AuthAnonymous(),
		opcua.AutoReconnect(false),
		opcua.DialTimeout(dialTimeout),
		opcua.RequestTimeout(requestTimeout),
		opcua.SessionTimeout(sessionTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("创建客户端: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("建立会话: %w", err)
	}
	namespace, err := client.FindNamespace(ctx, namespaceURI)
	if err != nil {
		_ = client.Close(ctx)
		return nil, fmt.Errorf("命名空间 %s 不存在，不是 KXOpcServices 服务端: %w", namespaceURI, err)
	}

	c := &Client{client: client, namespace: namespace, log: log}
	if ep.Server != nil && ep.Server.ApplicationName != nil {
		c.deviceName = strings.TrimPrefix(ep.Server.ApplicationName.Text, serverNamePrefix)
	}
	if c.log != nil {
		c.log = c.log.With("endpoint", endpoint, "炉台", c.deviceName)
	}
	return c, nil
}

// DeviceName 返回服务端上报的设备名（上位机 LocalDeviceConfig.dat 中的 DeviceName）。
func (c *Client) DeviceName() string {
	return c.deviceName
}

// Connected 返回会话是否仍然可用。
func (c *Client) Connected() bool {
	return c.client.State() == opcua.Connected
}

// Close 取消订阅并关闭会话。
func (c *Client) Close(ctx context.Context) {
	if c.sub != nil {
		c.sub.close(ctx)
	}
	_ = c.client.Close(ctx)
}

// Points 读取服务端的变量索引节点 VarNodeIds，返回当前全部点位。
func (c *Client) Points(ctx context.Context) ([]model.Point, error) {
	resp, err := c.client.Read(ctx, &ua.ReadRequest{
		NodesToRead:        []*ua.ReadValueID{{NodeID: ua.NewStringNodeID(c.namespace, varNodeIDs), AttributeID: ua.AttributeIDValue}},
		TimestampsToReturn: ua.TimestampsToReturnNeither,
	})
	if err != nil {
		return nil, fmt.Errorf("读取 %s: %w", varNodeIDs, err)
	}
	if len(resp.Results) != 1 || resp.Results[0].Status != ua.StatusOK || resp.Results[0].Value == nil {
		return nil, nil
	}
	raw, _ := resp.Results[0].Value.Value().(string)
	return ParseIndex(raw), nil
}

// nativeValue 取出 Variant 中的值，保留基础类型；GUID、LocalizedText、数组等其他类型转为字符串。
func nativeValue(v *ua.Variant) any {
	if v == nil {
		return nil
	}
	switch value := v.Value().(type) {
	case nil, bool, int8, uint8, int16, uint16, int32, uint32, int64, uint64, float32, float64, string, time.Time:
		return value
	default:
		return fmt.Sprint(value)
	}
}

// ReadValues 一次性读取这些点位的当前值（排查用，不建订阅）；读不到的点位值为 nil。
func (c *Client) ReadValues(ctx context.Context, points []model.Point) (map[string]any, error) {
	out := make(map[string]any, len(points))
	for start := 0; start < len(points); start += 200 { // 分批，避免一次请求太大
		batch := points[start:min(start+200, len(points))]
		nodes := make([]*ua.ReadValueID, len(batch))
		for i, p := range batch {
			id, err := ua.ParseNodeID(p.NodeID)
			if err != nil {
				return nil, fmt.Errorf("点位 %s 的 NodeId %q 不合法: %w", p.Name, p.NodeID, err)
			}
			nodes[i] = &ua.ReadValueID{NodeID: id, AttributeID: ua.AttributeIDValue}
		}
		rctx, cancel := context.WithTimeout(ctx, requestTimeout)
		resp, err := c.client.Read(rctx, &ua.ReadRequest{NodesToRead: nodes, TimestampsToReturn: ua.TimestampsToReturnNeither})
		cancel()
		if err != nil {
			return nil, err
		}
		for i, r := range resp.Results {
			if i < len(batch) && r.Status == ua.StatusOK {
				out[batch[i].Name] = nativeValue(r.Value)
			} else if i < len(batch) {
				out[batch[i].Name] = nil
			}
		}
	}
	return out, nil
}
