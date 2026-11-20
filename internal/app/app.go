// Package app 组装各模块并管理生命周期：
// 读取炉台清单（IP 范围先探测端口）→ 每台炉台启动一个采集协程 → 定期重读清单（只新增不减少）
// → 收到停止信号后先停采集、再写完剩余数据。
package app

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"kx-opc-client/internal/collector"
	"kx-opc-client/internal/config"
	"kx-opc-client/internal/model"
	"kx-opc-client/internal/opc"
	"kx-opc-client/internal/scanner"
	"kx-opc-client/internal/storage"
	"kx-opc-client/internal/storage/tdengine"
)

const (
	statusLogInterval = time.Minute
	statusListLimit   = 20 // 状态汇总中最多列出的炉台号个数
)

// Run 运行采集客户端，ctx 结束后优雅停机并返回。
func Run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	list, err := config.LoadFurnaces(cfg.Collector.FurnacesFile, cfg.TDengine.TablePrefix)
	if err != nil {
		return err
	}
	tdWriter, err := tdengine.NewWriter(cfg.TDengine, log)
	if err != nil {
		return err
	}
	dispatcher := storage.Start(log, tdWriter)
	registry := collector.NewRegistry()

	dial := func(ctx context.Context, f model.Furnace) (collector.Source, error) {
		client, err := opc.Dial(ctx, f.Endpoint(), log)
		if err != nil {
			return nil, err
		}
		return client, nil
	}

	var collectors sync.WaitGroup
	// start 为尚未采集的炉台启动采集协程：单独写出的 IP 全部启动，IP 范围中只启动端口能连通的。返回新增炉台的 IP
	start := func(list config.Furnaces) []string {
		var candidates []model.Furnace
		for _, f := range list.Ranged {
			if !registry.Has(f.Table) {
				candidates = append(candidates, f)
			}
		}
		reachable := scanner.Reachable(ctx, candidates, scanner.TCPProbe)

		var added []string
		for _, f := range slices.Concat(list.Fixed, reachable) {
			if !registry.Add(f) {
				continue
			}
			c := collector.New(f, cfg.Collector, dial, dispatcher.Publish, registry, log)
			collectors.Add(1)
			go func() {
				defer collectors.Done()
				c.Run(ctx)
			}()
			added = append(added, f.IP)
		}
		return added
	}

	log.Info("开始采集", "furnaces", len(start(list)), "ip_range_size", len(list.Ranged),
		"tdengine", cfg.TDengine.Address()+"/"+cfg.TDengine.Database()+"."+cfg.TDengine.SuperTable)

	reload := time.NewTicker(cfg.Collector.FurnacesReloadInterval)
	defer reload.Stop()
	status := time.NewTicker(statusLogInterval)
	defer status.Stop()
	var last statusSnapshot

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-reload.C:
			// 只新增不减少：清单里删掉的炉台继续采集，重启服务后才停止
			list, err := config.LoadFurnaces(cfg.Collector.FurnacesFile, cfg.TDengine.TablePrefix)
			if err != nil {
				log.Error("重读炉台清单失败，继续使用原清单", "error", err)
				continue
			}
			if added := start(list); len(added) > 0 {
				log.Info("新增炉台", "ips", abbreviate(added))
			}
		case <-status.C:
			last = logStatus(registry, tdWriter, last, log)
		}
	}

	log.Info("正在停止")
	collectors.Wait()
	dispatcher.Close()
	st := tdWriter.Status()
	args := []any{"累计写入", st.Written}
	if st.Dropped > 0 {
		args = append(args, "累计丢弃", st.Dropped)
	}
	if st.SpoolBytes > 0 {
		args = append(args, "暂存待补写", mb(st.SpoolBytes))
	}
	log.Info("已停止", args...)
	return nil
}

// statusSnapshot 是上一次状态日志时的计数，用来算“最近一分钟”的增量。
type statusSnapshot struct {
	writer    tdengine.Status
	connected map[string]bool // 上一次在线的炉台
}

// logStatus 每分钟一条状态汇总。一切正常时是一行 INFO：
//
//	运行状态 在线=8/8 写入_1m=472 数据库=正常
//
// 有任何异常时升为 WARN，只把异常的项加在后面，看这一行就知道问题出在哪一层：
//
//	断开 / 连接中=炉台号  → 采集端到炉台的网络，或炉台上的 OPC 服务
//	心跳停=炉台号        → 连着，但上位机没在采集（心跳不变）
//	连着没入库=炉台号    → 采集到了，这台的数据却一行都没写进库（多半是数据有问题，看“点位值有问题”那条日志）
//	数据库=不可用(时长)  → TDengine；期间数据落盘暂存（暂存_1m、暂存积压），恢复后补写（补写_1m）
//	去掉坏点位_1m / 跳过坏行_1m / 丢弃_1m → 有数据没完整入库
//	类型不符写空_1m    → 服务端改了点位类型，值转换不了列类型而写成 NULL（看“点位类型和列不一致”那条日志）
func logStatus(registry *collector.Registry, tdWriter *tdengine.Writer, last statusSnapshot, log *slog.Logger) statusSnapshot {
	args, abnormal, now := statusLine(registry.ByState(), tdWriter.Status(), last, time.Now())
	if abnormal {
		log.Warn("运行状态", args...)
	} else {
		log.Info("运行状态", args...)
	}
	return now
}

// statusLine 生成状态日志的字段，返回是否有异常和这一次的快照。
func statusLine(groups map[model.State][]string, st tdengine.Status, last statusSnapshot, now time.Time) ([]any, bool, statusSnapshot) {
	total := 0
	for _, ids := range groups {
		total += len(ids)
	}
	connected := groups[model.StateConnected]
	abnormal := false
	add := func(args []any, key string, value any) []any {
		abnormal = true
		return append(args, key, value)
	}

	args := []any{"在线", fmt.Sprintf("%d/%d", len(connected), total), "写入_1m", st.Written - last.writer.Written}
	if st.DownSince.IsZero() {
		args = append(args, "数据库", "正常")
	} else {
		args = add(args, "数据库", "不可用("+now.Sub(st.DownSince).Round(time.Second).String()+")")
	}
	for _, g := range []struct {
		key   string
		state model.State
	}{{"断开", model.StateDisconnected}, {"连接中", model.StateConnecting}, {"心跳停", model.StateStale}} {
		if ids := groups[g.state]; len(ids) > 0 {
			args = add(args, g.key, abbreviate(ids))
		}
	}
	// 上一分钟和现在都在线、数据库也正常，这一分钟却一行都没写进库的炉台
	if st.DownSince.IsZero() {
		var silent []string
		for _, id := range connected {
			if last.connected[id] && st.ByFurnace[id] == last.writer.ByFurnace[id] {
				silent = append(silent, id)
			}
		}
		if len(silent) > 0 {
			args = add(args, "连着没入库", abbreviate(silent))
		}
	}
	for _, c := range []struct {
		key        string
		now, prev  uint64
		isAbnormal bool
	}{
		{"去掉坏点位_1m", st.Fixed, last.writer.Fixed, true},
		{"跳过坏行_1m", st.Rejected, last.writer.Rejected, true},
		{"丢弃_1m", st.Dropped, last.writer.Dropped, true},
		{"类型不符写空_1m", st.Nulled, last.writer.Nulled, true},
		{"暂存_1m", st.Spooled, last.writer.Spooled, true},
		{"补写_1m", st.Replayed, last.writer.Replayed, false},
	} {
		if d := c.now - c.prev; d > 0 {
			if c.isAbnormal {
				args = add(args, c.key, d)
			} else {
				args = append(args, c.key, d)
			}
		}
	}
	if st.SpoolBytes > 0 {
		args = append(args, "暂存积压", mb(st.SpoolBytes))
	}

	snap := statusSnapshot{writer: st, connected: map[string]bool{}}
	for _, id := range connected {
		snap.connected[id] = true
	}
	return args, abnormal, snap
}

// mb 把字节数写成便于阅读的大小，如 3.2MB。
func mb(b int64) string {
	if b < 1<<20 {
		return fmt.Sprintf("%dKB", max(b>>10, 1))
	}
	return fmt.Sprintf("%.1fMB", float64(b)/(1<<20))
}

// abbreviate 太多时只列前 statusListLimit 个，如 "A1,A2,… 共35台"。
func abbreviate(ids []string) string {
	if len(ids) <= statusListLimit {
		return strings.Join(ids, ",")
	}
	return fmt.Sprintf("%s,… 共%d台", strings.Join(ids[:statusListLimit], ","), len(ids))
}
