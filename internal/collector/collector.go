// Package collector 实现一台炉台的采集循环：
// 连接 → 读取点位列表（定期刷新）→ 订阅全部点位 → 服务端推送变化时更新各点位最新值
// → 心跳变化时写一行全量（未变化的点位沿用上次的值）；任何错误都断开并按指数退避重连。
//
// 日志都带 furnace=炉台号：首次连上记一条（含 IP、炉台号来源、点位数），断线和重连成功各记一条，
// 心跳停滞/恢复、服务端通讯状态码变化各记一条；重连失败的重试只在 debug 级别输出。
//
// 服务端每轮（约 1 秒）先写各点位、最后刷新心跳 HEART_DATETIME（实测心跳之后至多 1ms 内还有个别点位），
// 所以“心跳变化”就代表服务端完成了一轮采集，收到即写一行；上位机断开时心跳不再变化，也就不再写入。
//
// 服务端热更新 HMI_Value.XML 时会增删点位、或删除后重建节点，而服务端不会为此推送任何通知，所以：
//   - 每 10 秒读一次点位目录 VarNodeIds，新增的点位建监控项，删除的点位删掉监控项、之后的行不再包含；
//   - 每 30 秒直接读一遍全部点位的当前值，发现订阅失效（节点被重建，旧监控项不再推送）的点位就重新订阅，
//     否则这个点位会一直沿用旧值写进库。心跳直读仍在更新、重建监控项后推送仍停滞时，重建这台炉的会话。
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"kx-opc-client/internal/config"
	"kx-opc-client/internal/model"
)

const (
	pointsRefreshInterval = 10 * time.Second // 读点位目录的周期：只读一个字符串，开销很小
	verifyInterval        = 30 * time.Second // 检查订阅是否失效的周期：直接读一遍全部点位
	reconnectMinDelay     = 5 * time.Second
	reconnectMaxDelay     = time.Minute
	closeTimeout          = 3 * time.Second
	checkInterval         = time.Second // 检查连接和心跳状态的周期（值不变时服务端不推送）
	heartbeatLayout       = "2006-01-02 15:04:05"
	heartbeatJumpLimit    = 10 * time.Minute
	nameListLimit         = 20 // 日志里最多列出的点名个数
)

var errSessionLost = errors.New("OPC 会话已断开")

// Source 是一台炉台的 OPC 会话（*opc.Client）。
type Source interface {
	DeviceName() string
	Connected() bool
	Points(ctx context.Context) ([]model.Point, error)
	Watch(ctx context.Context, points []model.Point) (bool, error)
	Unwatch(ctx context.Context, names []string) error
	ReadValues(ctx context.Context, points []model.Point) (map[string]any, error)
	Changes() <-chan model.Change
	Close(ctx context.Context)
}

// DialFunc 建立与炉台的会话。
type DialFunc func(ctx context.Context, furnace model.Furnace) (Source, error)

// PublishFunc 投递快照给存储端，不能阻塞。
type PublishFunc func(model.Snapshot)

// Collector 是一台炉台的采集器。
type Collector struct {
	furnace  model.Furnace
	cfg      config.Collector
	dial     DialFunc
	publish  PublishFunc
	registry *Registry
	baseLog  *slog.Logger
	log      *slog.Logger // 带 furnace=炉台号（确定前为 IP）

	// 单次会话内的状态，每次连上时重置
	points             []model.Point
	pointsRefreshedAt  time.Time
	refreshPoints      bool
	latest             map[string]any       // 各点位的最新值
	baseline           bool                 // 等待本次会话的第一个心跳（只作为基准，不写入）
	notifiedAt         map[string]time.Time // 各点位最近一次收到推送的时间
	suspect            map[string]time.Time // 上一次检查时值和直接读到的不一样的点位 -> 那次检查的时间
	verifiedAt         time.Time            // 上一次检查订阅是否失效的时间
	resubscribed       map[string]bool      // 本次会话已重新订阅过的点位（再次失效时只记 debug 日志，避免刷屏）
	verifiedHeartbeat  time.Time            // 上一次直接读到的有效心跳
	heartbeatRewatchAt time.Time            // 重建心跳监控项的时间，收到连续心跳推送后清零

	// 跨会话保留的状态
	connected          bool      // 是否连上过
	outageSince        time.Time // 本次断线的开始时间，零值表示没有断线
	lastHeartbeat      any
	heartbeatSample    time.Time // 本次会话上一次有效的上位机心跳时间
	heartbeatReceived  time.Time // 该心跳在采集端的接收时间
	heartbeatChangedAt time.Time
	lastRowTime        time.Time // 上一行的 ts，保证 ts 严格递增
	clockBehind        bool      // 系统时钟落后于上一行的 ts（被往回调过），ts 正在顺延
	staleSince         time.Time // 判定心跳停滞的时间，零值表示正常
	commStatus         int       // 服务端通讯状态码（HMIDataErrorID），-1 表示还没收到
	starting           bool      // 服务端启动中：状态码为初始值 10 且还没完成过一次采集
}

// New 创建采集器。
func New(furnace model.Furnace, cfg config.Collector, dial DialFunc, publish PublishFunc, registry *Registry, log *slog.Logger) *Collector {
	return &Collector{
		furnace:    furnace,
		cfg:        cfg,
		dial:       dial,
		publish:    publish,
		registry:   registry,
		baseLog:    log,
		log:        log.With("炉台", furnace.Name()),
		commStatus: -1,
	}
}

// Run 运行采集循环，直到 ctx 结束。断线只在开始时警告一次，重连成功时记一条恢复日志。
func (c *Collector) Run(ctx context.Context) {
	delay := reconnectMinDelay
	for ctx.Err() == nil {
		c.registry.Set(c.furnace.Table, model.StateConnecting)
		subscribed, err := c.session(ctx)
		if ctx.Err() != nil {
			break
		}
		c.registry.Set(c.furnace.Table, model.StateDisconnected)
		if subscribed {
			delay = reconnectMinDelay
		}
		switch {
		case !c.outageSince.IsZero():
			c.log.Debug("重连失败", "error", err, "下次重试", delay)
		case c.connected:
			c.log.Warn("连接断开，自动重连", "error", err)
		default:
			c.log.Warn("无法连接，持续重试", "error", err)
		}
		if c.outageSince.IsZero() {
			c.outageSince = time.Now()
		}

		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		delay = min(delay*2, reconnectMaxDelay)
	}
	c.registry.Set(c.furnace.Table, model.StateDisconnected)
}

// session 建立一次会话：连接 → 确定炉台号 → 订阅全部点位 → 处理推送，直到出错。返回是否订阅成功。
func (c *Collector) session(ctx context.Context) (bool, error) {
	src, err := c.dial(ctx, c.furnace)
	if err != nil {
		return false, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		src.Close(closeCtx)
		cancel()
	}()

	c.points, c.refreshPoints, c.latest, c.baseline = nil, true, map[string]any{}, true
	c.heartbeatSample, c.heartbeatReceived = time.Time{}, time.Time{}
	c.verifiedHeartbeat, c.heartbeatRewatchAt = time.Time{}, time.Time{}
	c.notifiedAt, c.suspect, c.resubscribed, c.verifiedAt = map[string]time.Time{}, nil, map[string]bool{}, time.Now()
	if c.staleSince.IsZero() {
		c.heartbeatChangedAt = time.Now() // 给新会话一个完整的心跳超时
	}
	source := c.resolveDeviceID(src.DeviceName())
	if err := c.updatePoints(ctx, src); err != nil {
		return false, err
	}

	switch {
	case !c.connected:
		c.log.Info("已连接", "ip", c.furnace.IP, "炉台号来源", source, "点位数", len(c.points))
	case !c.outageSince.IsZero():
		c.log.Info("重连成功", "断开了", time.Since(c.outageSince).Round(time.Second))
	}
	c.connected, c.outageSince = true, time.Time{}
	return true, c.collect(ctx, src)
}

// resolveDeviceID 确定炉台号，返回来源：优先用上位机配置的设备名（服务端上报）；上位机为默认值 3G14（没有配置）时，
// 有 furnaces.json 网段前缀的用前缀规则，没有的就用 3G14（子表和 device_ip 按 IP 区分，炉台号重复不影响）。
func (c *Collector) resolveDeviceID(deviceName string) string {
	id, source := deviceName, "上位机"
	if (deviceName == "" || deviceName == model.DefaultDeviceName) && c.furnace.AreaDeviceID != "" {
		id, source = c.furnace.AreaDeviceID, "网段前缀"
	}
	if id == "" { // 服务端没有上报应用名（极少见）
		id, source = c.furnace.IP, "IP"
	}
	if id == c.furnace.DeviceID {
		return source
	}

	previous := c.furnace.DeviceID
	c.furnace.DeviceID = id
	c.registry.SetDeviceID(c.furnace.Table, id)
	c.log = c.baseLog.With("炉台", id)
	if previous != "" {
		c.log.Info("炉台号变更", "原来", previous, "来源", source)
	}
	return source
}

// collect 处理服务端推送，并每秒检查会话和心跳。刚订阅时服务端会先推送全部点位的当前值。
func (c *Collector) collect(ctx context.Context, src Source) error {
	check := time.NewTicker(checkInterval)
	defer check.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case change := <-src.Changes():
			if change.Err != nil {
				return change.Err
			}
			c.handle(change)

		case <-check.C:
			if !src.Connected() {
				return errSessionLost
			}
			c.updateState()
		}

		if err := c.updatePoints(ctx, src); err != nil {
			return err
		}
		if err := c.verifySubscription(ctx, src); err != nil {
			return err
		}
	}
}

// handle 处理一次推送：合并进各点位最新值；心跳变化（且服务端已完成过采集）时写一行全量。
func (c *Collector) handle(change model.Change) {
	if len(change.Unknown) > 0 {
		// 节点已被服务端删除：不再沿用旧值，并马上重读点位目录
		for _, name := range change.Unknown {
			delete(c.latest, name)
		}
		c.refreshPoints = true
	}
	maps.Copy(c.latest, change.Values)
	for name := range change.Values {
		c.notifiedAt[name] = change.Time
	}
	if status, ok := change.Values[model.CommStatusPoint]; ok {
		c.onCommStatus(status)
	}
	heartbeat, ok := change.Values[model.HeartbeatPoint]
	if !ok {
		return
	}
	baseline := c.baseline
	c.baseline = false
	var jump int64
	if baseline || heartbeat != c.lastHeartbeat {
		jump = c.checkHeartbeatJump(heartbeat, change.Time)
	}
	if heartbeat == c.lastHeartbeat {
		return
	}
	if !c.heartbeatRewatchAt.IsZero() && c.heartbeatChangedAt.After(c.heartbeatRewatchAt) {
		previous, _ := c.lastHeartbeat.(string)
		current, _ := heartbeat.(string)
		if previous != "" && previous != model.HeartbeatInitial && current != "" && current != model.HeartbeatInitial {
			c.log.Info("重建监控项后心跳推送已恢复")
			c.heartbeatRewatchAt = time.Time{}
		}
	}
	c.lastHeartbeat = heartbeat
	c.heartbeatChangedAt = change.Time
	// 会话的第一个心跳是订阅时推送的当前值，不代表新一轮采集（点位多时也可能还没推全），只作为基准；
	// 心跳为初始值说明服务端还没完成过一次采集，各点位都是初始值。这两种都不写入
	if baseline || heartbeat == model.HeartbeatInitial {
		return
	}
	// 写一行全量：各点位的最新值；复制一份交给存储端，避免后续合并时改到已投递的快照
	c.publish(model.Snapshot{Furnace: c.furnace, Time: c.rowTime(change.Time), Values: maps.Clone(c.latest), HeartbeatJumpSeconds: jump})
}

// checkHeartbeatJump 只比较在线期间的相邻心跳，通讯恢复后的首个心跳作为新基准。
func (c *Collector) checkHeartbeatJump(heartbeat any, received time.Time) int64 {
	if c.commStatus != -1 && c.commStatus != 0 {
		return 0
	}
	value, _ := heartbeat.(string)
	sample, err := time.Parse(heartbeatLayout, value)
	previous, previousReceived := c.heartbeatSample, c.heartbeatReceived
	c.heartbeatSample, c.heartbeatReceived = sample, received
	if err != nil || previous.IsZero() {
		return 0
	}

	elapsed := received.Sub(previousReceived)
	timeout := c.cfg.HeartbeatTimeout
	if timeout == 0 {
		timeout = config.DefaultHeartbeatTimeout
	}
	if elapsed < 0 || elapsed > timeout {
		return 0
	}
	jump := sample.Sub(previous) - elapsed.Round(time.Second)
	if jump.Abs() < heartbeatJumpLimit {
		return 0
	}
	c.log.Warn("心跳时间跳变", "ip", c.furnace.IP, "跳变开始", received,
		"上次心跳接收时间", previousReceived, "跳变前心跳", previous.Format(heartbeatLayout), "跳变后心跳", value,
		"跳变秒数", jump.Seconds())
	return int64(jump / time.Second)
}

// rowTime 返回本行的 ts：比上一行至少晚 1 毫秒。系统时钟被往回调时，TDengine 里相同 ts 的行会被覆盖、
// 重叠的 ts 也会打乱顺序，所以顺延到上一行之后，等系统时钟追上来再恢复用收到推送的时间。
func (c *Collector) rowTime(received time.Time) time.Time {
	// 去掉单调时钟读数：否则 Sub 按单调时钟计算，看不出系统时钟被往回调
	received = received.Round(0)
	ts := received
	behind := ts.Sub(c.lastRowTime) < time.Millisecond
	if behind {
		ts = c.lastRowTime.Add(time.Millisecond)
	}
	switch {
	case behind && !c.clockBehind && c.lastRowTime.Sub(received) >= time.Second:
		c.clockBehind = true
		c.log.Warn("系统时钟被往回调，ts 顺延到上一行之后", "回调了", c.lastRowTime.Sub(received).Round(time.Millisecond))
	case !behind && c.clockBehind:
		c.clockBehind = false
		c.log.Info("系统时钟已追上，ts 恢复正常")
	}
	c.lastRowTime = ts
	return ts
}

// updatePoints 到期或需要时重新读取点位目录：删除的点位删掉监控项、之后的行不再包含它们，新增的点位创建监控项。
// 删掉监控项后，同名点位以后再加回来时能重新订阅上。
func (c *Collector) updatePoints(ctx context.Context, src Source) error {
	now := time.Now()
	if !c.refreshPoints && now.Sub(c.pointsRefreshedAt) < pointsRefreshInterval {
		return nil
	}
	points, err := src.Points(ctx)
	if err != nil {
		return err
	}
	c.pointsRefreshedAt = now
	if len(points) == 0 && len(c.points) > 0 {
		// 目录读成空多半是读取异常，不当成全部点位被删除，下次再读
		c.log.Debug("点位目录读到空列表，保持原点位")
		return nil
	}

	added, removed := diffPoints(c.points, points)
	if c.points != nil && (len(added) > 0 || len(removed) > 0) {
		args := []any{"点位数", len(points)}
		if len(added) > 0 {
			args = append(args, "新增", listNames(added))
		}
		if len(removed) > 0 {
			args = append(args, "删除", listNames(removed))
		}
		c.log.Info("点位变化", args...)
	}
	if len(removed) > 0 {
		if err := src.Unwatch(ctx, removed); err != nil {
			return err
		}
	}
	c.points = points
	if c.refreshPoints, err = src.Watch(ctx, points); err != nil {
		return err
	}

	// 去掉已删除点位的最新值，之后写入的行不再包含它们
	current := make(map[string]bool, len(points))
	for _, p := range points {
		current[p.Name] = true
	}
	gone := func(name string, _ any) bool { return !current[name] }
	maps.DeleteFunc(c.latest, gone)
	maps.DeleteFunc(c.notifiedAt, func(name string, _ time.Time) bool { return !current[name] })
	return nil
}

// verifySubscription 检查订阅是否失效。服务端热更新时删除后重建的节点（类型、文件夹或来源变了），
// 原来的监控项从此不再推送、也不报错，点位的值会冻在旧值上。这里定期直接读一遍全部点位的当前值：
// 和最新值不一样、并且到下一次检查时仍没收到这个点位推送的，判定订阅失效，删掉它的监控项重新创建，
// 最新值先换成直接读到的值。心跳监控项已经重建但推送仍停滞，且直读确认心跳递增时，由采集循环重连。
func (c *Collector) verifySubscription(ctx context.Context, src Source) error {
	now := time.Now()
	if now.Sub(c.verifiedAt) < verifyInterval || len(c.points) == 0 {
		return nil
	}
	c.verifiedAt = now
	values, err := src.ReadValues(ctx, c.points)
	if err != nil {
		return err
	}

	heartbeat, _ := values[model.HeartbeatPoint].(string)
	previousHeartbeat := c.verifiedHeartbeat
	c.verifiedHeartbeat, _ = time.Parse(heartbeatLayout, heartbeat)

	var stale []string
	suspect := map[string]time.Time{}
	for _, p := range c.points {
		v := values[p.Name]
		if v == nil || sameValue(v, c.latest[p.Name]) {
			continue
		}
		lastUpdate := c.notifiedAt[p.Name]
		if p.Name == model.HeartbeatPoint {
			lastUpdate = c.heartbeatChangedAt
		}
		if since, ok := c.suspect[p.Name]; ok && lastUpdate.Before(since) {
			stale = append(stale, p.Name)
			continue
		}
		suspect[p.Name] = now
	}
	c.suspect = suspect
	if len(stale) == 0 {
		return nil
	}

	heartbeatStale := slices.Contains(stale, model.HeartbeatPoint)
	if heartbeatStale && !c.heartbeatRewatchAt.IsZero() &&
		!previousHeartbeat.IsZero() && c.verifiedHeartbeat.After(previousHeartbeat) {
		return fmt.Errorf("心跳直读从 %s 更新到 %s，重建监控项后仍未收到连续推送",
			previousHeartbeat.Format(heartbeatLayout), heartbeat)
	}

	if err := src.Unwatch(ctx, stale); err != nil {
		return err
	}
	for _, name := range stale {
		c.latest[name] = values[name]
	}
	if c.refreshPoints, err = src.Watch(ctx, c.points); err != nil {
		return err
	}
	if heartbeatStale {
		c.heartbeatRewatchAt = time.Now()
	}

	var first []string // 本次会话第一次失效的点位记 WARN，反复失效的只记 debug
	for _, name := range stale {
		if !c.resubscribed[name] {
			first = append(first, name)
			c.resubscribed[name] = true
		}
	}
	if len(first) > 0 {
		c.log.Warn("点位推送停滞，已请求重建监控项", "点位", listNames(first))
	} else {
		c.log.Debug("点位推送再次停滞，已请求重建监控项", "点位", listNames(stale))
	}
	return nil
}

// diffPoints 按点名比较两份点位列表，返回新增和删除的点名。
func diffPoints(old, cur []model.Point) (added, removed []string) {
	before := make(map[string]bool, len(old))
	for _, p := range old {
		before[p.Name] = true
	}
	now := make(map[string]bool, len(cur))
	for _, p := range cur {
		now[p.Name] = true
		if !before[p.Name] {
			added = append(added, p.Name)
		}
	}
	for _, p := range old {
		if !now[p.Name] {
			removed = append(removed, p.Name)
		}
	}
	return added, removed
}

// listNames 点名太多时只列前 nameListLimit 个，如 "A,B,… 共35个"。
func listNames(names []string) string {
	if len(names) <= nameListLimit {
		return strings.Join(names, ",")
	}
	return fmt.Sprintf("%s,… 共%d个", strings.Join(names[:nameListLimit], ","), len(names))
}

// sameValue 比较两个点位值（类型不同算不同；两个 NaN 算相同，否则 NaN 永远不等于自己）。
func sameValue(a, b any) bool {
	switch x := a.(type) {
	case float32:
		if y, ok := b.(float32); ok && x != x && y != y {
			return true
		}
	case float64:
		if y, ok := b.(float64); ok && x != x && y != y {
			return true
		}
	}
	return a == b
}

// updateState 更新炉台状态：心跳超过 heartbeat_timeout 不变化判定为停滞（上位机断开）。
// 心跳不变时本来就不会写入，这里只用于状态汇总和日志。
func (c *Collector) updateState() {
	stale := c.cfg.HeartbeatTimeout > 0 && time.Since(c.heartbeatChangedAt) > c.cfg.HeartbeatTimeout
	state := model.StateConnected
	if stale {
		state = model.StateStale
	}
	c.registry.Set(c.furnace.Table, state)

	switch {
	case stale && c.staleSince.IsZero():
		c.staleSince = time.Now()
		c.log.Warn("心跳停止变化，暂停写入", "通讯状态", commStatusText(c.commStatus))
	case !stale && !c.staleSince.IsZero():
		c.log.Info("心跳恢复，继续写入", "停了", time.Since(c.staleSince).Round(time.Second))
		c.staleSince = time.Time{}
	}
}

// onCommStatus 服务端通讯状态码变化时记一条日志：刚连上时为 0 不记；
// 服务端启动阶段（心跳仍为初始值）的初始状态码 10 只记一条 INFO，首次采集完成后也不记“恢复”。
// 状态码不论服务端发的是什么数值类型（Float、Int16、数字字符串等）都能识别；不是数值时忽略。
func (c *Collector) onCommStatus(value any) {
	f, ok := number(value)
	if !ok {
		return
	}
	status := int(f)
	if status == c.commStatus {
		return
	}
	previous := c.commStatus
	c.commStatus = status
	if status != 0 {
		c.heartbeatSample, c.heartbeatReceived = time.Time{}, time.Time{}
	}
	switch {
	case status == 10 && c.latest[model.HeartbeatPoint] == model.HeartbeatInitial:
		c.starting = true
		c.log.Info("服务端启动中，等待首次采集")
	case status != 0:
		c.starting = false
		c.log.Warn("服务端与上位机通讯异常", "通讯状态", commStatusText(status))
	case c.starting:
		c.starting = false
	case previous != -1:
		c.log.Info("服务端与上位机通讯恢复")
	}
}

// commStatusText 状态码含义（见服务端 README：HMIDataErrorID）。
func commStatusText(status int) string {
	switch status {
	case 0:
		return "0 正常"
	case 10:
		return "10 通讯失败"
	case 20:
		return "20 解析错误"
	case -1:
		return "未知"
	default:
		return fmt.Sprintf("%d 未定义", status)
	}
}

// number 把任意数值类型（或数字字符串）转为有限的 float64。
func number(v any) (float64, bool) {
	var f float64
	switch x := v.(type) {
	case int8:
		f = float64(x)
	case uint8:
		f = float64(x)
	case int16:
		f = float64(x)
	case uint16:
		f = float64(x)
	case int32:
		f = float64(x)
	case uint32:
		f = float64(x)
	case int64:
		f = float64(x)
	case uint64:
		f = float64(x)
	case float32:
		f = float64(x)
	case float64:
		f = x
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		f = parsed
	default:
		return 0, false
	}
	return f, !math.IsNaN(f) && !math.IsInf(f, 0)
}
