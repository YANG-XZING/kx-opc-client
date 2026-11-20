// Package model 定义各模块共用的数据结构。
package model

import (
	"fmt"
	"time"
)

// 服务端固定点位（不依赖 HMI_Value.XML，服务启动即创建）
const (
	// HeartbeatPoint 心跳：服务端每成功采集一轮（约 1 秒）写入当前时间，不再变化说明上位机数据中断
	HeartbeatPoint = "HEART_DATETIME"
	// HeartbeatInitial 心跳的初始值：服务端刚启动、还没完成过一次采集，此时各点位都是初始值（0 / false / "0"）
	HeartbeatInitial = "0"
	// CommStatusPoint 服务端与上位机的通讯状态码：0 正常，10 通讯失败，20 解析错误
	CommStatusPoint = "HMIDataErrorID"
)

// DefaultDeviceName 是服务端 App.config 中 DeviceName 的默认值：上位机缺少 LocalDeviceConfig.dat 时服务端上报它，
// 说明上位机没有配置设备名，不能作为炉台号。
const DefaultDeviceName = "3G14"

// Furnace 是一台炉台（一个 OPC UA 服务端）。
type Furnace struct {
	IP   string // 写入 TDengine 标签 device_ip
	Port int    // OPC UA 端口
	// AreaDeviceID 按 furnaces.json 网段前缀规则得到的炉台号（区号 + IP 最后一段，如 A12），没有匹配的网段时为空
	AreaDeviceID string
	// DeviceID 炉台号，写入 TDengine 标签 device_id；连上服务端后确定（规则见 collector.resolveDeviceID）
	DeviceID string
	Table    string // TDengine 子表名，如 opc_10_65_108_12
}

// Name 返回用于日志的炉台标识：炉台号确定前用 IP。
func (f Furnace) Name() string {
	if f.DeviceID != "" {
		return f.DeviceID
	}
	return f.IP
}

// Endpoint 返回 OPC UA 端点地址。
func (f Furnace) Endpoint() string {
	return fmt.Sprintf("opc.tcp://%s:%d", f.IP, f.Port)
}

// Point 是服务端上的一个变量点。
type Point struct {
	Name   string // 点名，即服务端字符串 NodeId 的标识（上位机英文名）
	NodeID string // 完整 NodeId，如 ns=2;s=Temp
}

// Change 是服务端推送的一批变化。
type Change struct {
	Time time.Time // 收到推送的时间（客户端时间）
	// Values 变化了的点：点名 -> 新值。值保留服务端节点的类型（由 HMI_Value.XML 的 type 决定）：
	// bool、int8~int64、uint8~uint64、float32、float64、string、time.Time，其他类型转为 string
	Values  map[string]any
	Unknown []string // 服务端报告节点已不存在的点（被删除了），需要立即重新读取点位列表
	Err     error    // 订阅出错（如订阅失效），需要重连
}

// Snapshot 是一台炉台一轮采集的全部点位值（本轮未变化的点位为上次的值）。
type Snapshot struct {
	Furnace              Furnace
	Time                 time.Time      // 心跳变化到达的时刻（客户端时间），写入 TDengine 的 ts 列
	Values               map[string]any // 点名 -> 值（类型同 Change.Values）
	HeartbeatJumpSeconds int64          // 带符号的跳变秒数，0 表示没有跳变
}

// State 是炉台采集状态。
type State int

const (
	StateDisconnected State = iota
	StateConnecting
	StateConnected // 已连接且心跳正常
	StateStale     // 已连接但心跳不再变化：OPC 服务端在线，上位机数据中断
)
