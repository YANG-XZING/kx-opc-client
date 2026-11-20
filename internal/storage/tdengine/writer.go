// Package tdengine 把采集快照写入 TDengine 宽表：
//
//	<库>.opc_data（超级表：ts + 每个点位一列，列类型按 OPC 节点类型，标签 device_id、device_ip）
//	└── opc_10_65_108_12 …（每台炉台一张子表）
//
// 超级表、子表、新点位的列都会自动创建，不需要提前建表。
//
// 写入尽量不丢数据：
//   - 某一行数据有问题（TDengine 报语法 / 数据错误）时，先按炉台、再按行拆开重写，只跳过真正写不进去的那一行，
//     且只去掉有问题的那个点位，同一行的其他点位照常写入；不会因为一台炉台的一个坏值丢掉同一批里所有炉台的数据。
//   - TDengine 不可用（断网、服务停了）时，数据落盘暂存（spool.go），恢复后按顺序补写；暂存目录满了才丢弃。
package tdengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	taosErrors "github.com/taosdata/driver-go/v3/errors"
	// 注册 taosWS 驱动（WebSocket，经 taosAdapter 6041 端口，无需安装 TDengine 原生库）
	_ "github.com/taosdata/driver-go/v3/taosWS"

	"kx-opc-client/internal/config"
	"kx-opc-client/internal/model"
)

const (
	flushInterval = time.Second
	writeTimeout  = 10 * time.Second
	minRetryDelay = time.Second
	maxRetryDelay = 30 * time.Second
	maxRowBytes   = 48 * 1024  // TDengine 单行上限（3.0.5 之前 48KB，之后 64KB）
	maxSQLBytes   = 512 * 1024 // 单条 INSERT 上限

	drainChunk     = 2000            // 补写暂存数据时每次读多少行
	drainBudget    = 3 * time.Second // 每秒最多花这么久补写暂存数据，不耽误新数据入库
	rejectLogEvery = 10 * time.Minute
	valuePreview   = 80 // 日志里显示值的最大长度
)

// Writer 每秒批量写入 TDengine，实现 storage.Writer。
type Writer struct {
	cfg      config.TDengine
	database string
	db       *sql.DB
	log      *slog.Logger
	spool    *spool // TDengine 不可用时的落盘暂存，未配置时为 nil（不可用期间的数据丢弃）

	// 以下状态只在 Run 所在的协程中访问
	ready          bool
	jumpTableReady bool
	precision      string
	schema         *Schema
	pending        []Row
	tagged         map[string]string // 子表 -> 本次运行已同步的 device_id 标签
	failedAt       time.Time         // 本次不可用的开始时间，零值表示正常
	dropped        int               // 本次不可用期间丢弃的行数
	spooled        int               // 本次不可用期间暂存的行数
	spoolWarned    bool              // 本次不可用期间是否已提示过暂存失败
	retryDelay     time.Duration
	nextAttempt    time.Time
	rejectLogged   map[string]time.Time // 子表 -> 上次记“数据有问题”日志的时间（限频）
	typeLogged     map[string]bool      // 点位|收到的类型 -> 已记过“类型和列不一致”日志（每种只记一次）

	written       atomic.Uint64
	droppedTotal  atomic.Uint64
	rejectedTotal atomic.Uint64 // 数据本身有问题、去掉文本点位也写不进去而跳过的行
	fixedTotal    atomic.Uint64 // 去掉有问题的点位后写入的行
	spooledTotal  atomic.Uint64 // 落盘暂存的行
	replayedTotal atomic.Uint64 // 从暂存补写的行
	nulledTotal   atomic.Uint64 // 类型和列不一致、转换不了而写成 NULL 的值

	mu         sync.Mutex        // 保护下面三项：状态日志在其他协程读取
	byFurnace  map[string]uint64 // 炉台 -> 累计实时写入行数，用来找出“连着却没入库”的炉台
	downSince  time.Time         // TDengine 不可用的开始时间，零值为正常
	spoolBytes int64             // 落盘暂存积压的字节数
}

// Status 是写入端的累计计数和当前状态，供每分钟的状态日志计算增量（可在其他协程调用）。
type Status struct {
	Written, Dropped, Rejected, Fixed, Spooled, Replayed uint64
	Nulled                                               uint64            // 类型和列不一致、转换不了而写成 NULL 的值
	ByFurnace                                            map[string]uint64 // 炉台 -> 累计写入行数
	DownSince                                            time.Time         // TDengine 不可用的开始时间，零值为正常
	SpoolBytes                                           int64             // 落盘暂存积压
}

// Status 返回当前状态的快照。
func (w *Writer) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Status{
		Written: w.written.Load(), Dropped: w.droppedTotal.Load(), Rejected: w.rejectedTotal.Load(), Fixed: w.fixedTotal.Load(),
		Spooled: w.spooledTotal.Load(), Replayed: w.replayedTotal.Load(), Nulled: w.nulledTotal.Load(),
		ByFurnace: maps.Clone(w.byFurnace), DownSince: w.downSince, SpoolBytes: w.spoolBytes,
	}
}

// countWritten 记下每台炉台写进去的行数。
func (w *Writer) countWritten(rows []Row) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range rows {
		w.byFurnace[r.Name]++
	}
}

// updateSpoolBytes 更新暂存积压（每次落盘或补写后）。
func (w *Writer) updateSpoolBytes() {
	if w.spool == nil {
		return
	}
	n := w.spool.size + w.spool.activeBytes()
	w.mu.Lock()
	w.spoolBytes = n
	w.mu.Unlock()
}

// NewWriter 创建写入器。不会立即连接，首次写入时连接并初始化表结构。
func NewWriter(cfg config.TDengine, log *slog.Logger) (*Writer, error) {
	db, err := sql.Open("taosWS", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("打开 TDengine 连接: %w", err)
	}
	db.SetMaxOpenConns(2)
	w := &Writer{cfg: cfg, database: cfg.Database(), db: db, log: log,
		tagged: map[string]string{}, rejectLogged: map[string]time.Time{}, typeLogged: map[string]bool{}, byFurnace: map[string]uint64{}}
	if cfg.SpoolDir != "" {
		// 暂存目录不可用（没有权限、磁盘问题）不能影响采集和正常写库：记错误，按不暂存继续运行
		if sp, err := newSpool(cfg.SpoolDir, cfg.SpoolMaxMB, w.log); err != nil {
			w.log.Error("落盘暂存目录不可用，数据库不可用期间的数据将丢弃", "目录", cfg.SpoolDir, "error", err)
		} else {
			w.spool = sp
			w.updateSpoolBytes()
		}
	}
	return w, nil
}

// Name 实现 storage.Writer。
func (w *Writer) Name() string { return "tdengine" }

// Accept 实现 storage.Writer：TDengine 保存全部炉台。
func (w *Writer) Accept(model.Furnace) bool { return true }

// Run 实现 storage.Writer：in 关闭后把剩余数据写完（写不进去就暂存）再返回。
func (w *Writer) Run(in <-chan model.Snapshot) {
	defer w.db.Close()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case s, ok := <-in:
			if !ok {
				w.nextAttempt = time.Time{}
				w.flush()
				w.spill() // 最后一次还是写不进去的，留到下次启动补写
				if w.spool != nil {
					w.spool.close()
				}
				return
			}
			w.pending = append(w.pending, Row{Table: s.Furnace.Table, DeviceID: s.Furnace.DeviceID, DeviceIP: s.Furnace.IP, Name: s.Furnace.Name(), Time: s.Time, Values: s.Values, HeartbeatJumpSeconds: s.HeartbeatJumpSeconds})
		case <-ticker.C:
			w.flush()
		}
	}
}

func (w *Writer) drop(rows int) {
	w.dropped += rows
	w.droppedTotal.Add(uint64(rows))
}

// flush 写入暂存的新数据；TDengine 可用时再补写一部分落盘暂存的旧数据。
func (w *Writer) flush() {
	if time.Now().Before(w.nextAttempt) {
		w.spill() // 还没到重试时间：新数据先落盘
		return
	}
	if len(w.pending) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		ok, err := w.writeRows(ctx, w.pending)
		cancel()
		if err != nil {
			w.fail(err)
			w.spill()
			return
		}
		w.pending = w.pending[:0]
		w.recovered()
		w.written.Add(uint64(len(ok)))
		w.countWritten(ok)
	}
	w.drain()
}

// spill 把 pending 落盘暂存（没配置暂存或暂存失败时丢弃），并清空 pending。
func (w *Writer) spill() {
	if len(w.pending) == 0 {
		return
	}
	n := 0
	var err error
	if w.spool != nil {
		n, err = w.spool.append(w.pending)
		w.spooled += n
		w.spooledTotal.Add(uint64(n))
		w.updateSpoolBytes()
	}
	if lost := len(w.pending) - n; lost > 0 {
		w.drop(lost)
		if w.spool != nil && !w.spoolWarned {
			w.spoolWarned = true
			w.log.Error("落盘暂存失败，数据丢弃", "目录", w.cfg.SpoolDir, "error", err)
		}
	}
	w.pending = w.pending[:0]
}

// fail 记录一次写入失败，按指数退避安排下次重试。
func (w *Writer) fail(err error) {
	if w.failedAt.IsZero() {
		w.failedAt = time.Now()
		w.mu.Lock()
		w.downSince = w.failedAt
		w.mu.Unlock()
		if w.spool != nil {
			w.log.Warn("数据库写不进去，数据先落盘暂存，恢复后自动补写", "error", err)
		} else {
			w.log.Warn("数据库写不进去，恢复前的数据将丢弃（未配置 spool_dir）", "error", err)
		}
	} else {
		w.log.Debug("TDengine 仍不可用", "error", err)
	}
	w.retryDelay = min(max(w.retryDelay*2, minRetryDelay), maxRetryDelay)
	w.nextAttempt = time.Now().Add(w.retryDelay)
}

func (w *Writer) recovered() {
	if !w.failedAt.IsZero() {
		args := []any{"中断", time.Since(w.failedAt).Round(time.Second).String(), "暂存", w.spooled}
		if w.dropped > 0 {
			args = append(args, "丢弃", w.dropped)
		}
		w.log.Info("数据库写入恢复", args...)
		w.failedAt, w.dropped, w.spooled, w.spoolWarned = time.Time{}, 0, 0, false
		w.mu.Lock()
		w.downSince = time.Time{}
		w.mu.Unlock()
	}
	w.retryDelay = 0
}

// drain 在时间预算内补写落盘暂存的数据（从最旧的开始）。中途又写不进去就停下，下次从这一段开头重写。
func (w *Writer) drain() {
	if w.spool == nil || !w.spool.hasData() {
		return
	}
	if err := w.spool.closeActive(); err != nil {
		w.log.Warn("关闭暂存文件失败", "error", err)
	}
	deadline := time.Now().Add(drainBudget)
	for time.Now().Before(deadline) {
		rows, path, done, err := w.spool.next(drainChunk)
		if err != nil {
			w.log.Warn("读取暂存文件失败", "error", err)
			return
		}
		if path == "" {
			return // 没有了
		}
		if len(rows) > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			ok, err := w.writeRows(ctx, rows)
			cancel()
			if err != nil {
				w.spool.stopReading()
				w.fail(err)
				return
			}
			w.replayedTotal.Add(uint64(len(ok)))
		}
		if done {
			w.spool.finish(path)
			w.updateSpoolBytes()
			if !w.spool.hasData() {
				w.log.Info("落盘暂存的数据已全部补写", "累计补写", w.replayedTotal.Load())
			}
		}
	}
}

// writeRows 写入一批行，返回写进去的行。只有 TDengine 不可用（连不上、超时等）时才返回错误，
// 调用方应把这批行暂存；数据本身的问题在这里拆开处理掉，不返回错误。
func (w *Writer) writeRows(ctx context.Context, rows []Row) ([]Row, error) {
	if !w.ready {
		if err := w.init(ctx); err != nil {
			return nil, err
		}
	}
	if err := w.writeHeartbeatJumps(ctx, rows); err != nil {
		return nil, err
	}
	if err := w.addColumns(ctx, rows); err != nil {
		return nil, err
	}
	w.checkTypes(rows)
	err := w.exec(ctx, rows)
	if err == nil {
		w.syncTags(ctx, rows)
		return rows, nil
	}
	if !isDataError(err) {
		return nil, err
	}
	ok, err := w.isolate(ctx, rows, err)
	w.syncTags(ctx, ok) // 只给写进去的子表更新标签（跳过的行，子表可能还不存在）
	return ok, err
}

// writeHeartbeatJumps 单独保存稀疏事件，随快照暂存和重试，按 (ts, device_ip) 幂等写入。
func (w *Writer) writeHeartbeatJumps(ctx context.Context, rows []Row) error {
	var values strings.Builder
	for _, row := range rows {
		if row.HeartbeatJumpSeconds == 0 {
			continue
		}
		fmt.Fprintf(&values, " (%d, '%s', '%s', %d)", timestamp(row.Time, w.precision),
			escape(truncate(row.DeviceIP, tagLength)), escape(truncate(row.DeviceID, tagLength)), row.HeartbeatJumpSeconds)
	}
	if values.Len() == 0 {
		return nil
	}
	table := fmt.Sprintf("`%s`.`opc_heartbeat_jump`", w.database)
	if !w.jumpTableReady {
		ddl := "CREATE TABLE IF NOT EXISTS " + table + " (ts TIMESTAMP, device_ip VARCHAR(64) PRIMARY KEY, device_id VARCHAR(64), seconds BIGINT)"
		if _, err := w.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("创建心跳跳变表: %w", err)
		}
		w.jumpTableReady = true
	}
	if _, err := w.db.ExecContext(ctx, "INSERT INTO "+table+" (ts, device_ip, device_id, seconds) VALUES"+values.String()); err != nil {
		w.jumpTableReady = false
		return fmt.Errorf("写入心跳跳变: %w", err)
	}
	return nil
}

// exec 把行拼成 INSERT 执行（可能切成多条）。
func (w *Writer) exec(ctx context.Context, rows []Row) error {
	for _, stmt := range buildInserts(w.database, w.cfg.SuperTable, rows, w.schema.Column, w.precision, maxSQLBytes) {
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("INSERT: %w", err)
		}
	}
	return nil
}

// isolate 整批写入报数据错误时，按炉台、再按行拆开重写，找出有问题的行：
// 先尝试去掉这一行的文本点位再写（数值点位保住），还不行才跳过这一行。
// 如果拆开后一行都写不进去，多半不是数据问题而是 TDengine 本身有问题，按不可用处理（返回错误，整批暂存）。
func (w *Writer) isolate(ctx context.Context, rows []Row, batchErr error) ([]Row, error) {
	var ok []Row // 写进去的行
	rejected := 0
	for _, group := range groupByTable(rows) {
		err := w.exec(ctx, group)
		if err == nil {
			ok = append(ok, group...)
			continue
		}
		if !isDataError(err) {
			return ok, err
		}
		for _, row := range group {
			err := w.exec(ctx, []Row{row})
			if err == nil {
				ok = append(ok, row)
				continue
			}
			if !isDataError(err) {
				return ok, err
			}
			fixed, removed, err2 := w.dropBadPoints(ctx, row)
			if err2 != nil {
				return ok, err2
			}
			if removed != nil {
				ok = append(ok, fixed)
				w.fixedTotal.Add(1)
				w.logReject(row, "点位值有问题，只去掉这个点位，其他点位照常入库", err, removed)
				continue
			}
			rejected++
			_, texts := withoutText(row)
			w.logReject(row, "这一行数据写不进去，跳过", err, texts)
		}
	}
	if len(ok) == 0 && len(rows) > 1 {
		return nil, batchErr
	}
	w.rejectedTotal.Add(uint64(rejected))
	return ok, nil
}

// dropBadPoints 一行写不进去时，找出是哪个点位的值有问题，只去掉它，其他点位照常写入：
// 先逐个去掉一个文本点位试写（问题几乎都出在文本值上），哪个去掉后能写进去就是它；
// 都不行再去掉类型和列不一致的点位试写（服务端改了类型时）；还不行（可能不止一个有问题）去掉全部文本点位、
// 最后文本点位和类型不一致的点位一起去掉试写。写进去时返回写入的行和被去掉的点位；
// 都去掉也写不进去（如时间戳超出范围，和点位无关）时 removed 为 nil；TDengine 不可用时返回错误。
func (w *Writer) dropBadPoints(ctx context.Context, row Row) (Row, []string, error) {
	var points []string
	for k, v := range row.Values {
		if _, ok := v.(string); ok {
			points = append(points, k)
		}
	}
	mismatched := w.mismatched(row)
	if len(points) == 0 && len(mismatched) == 0 {
		return row, nil, nil
	}
	slices.Sort(points)
	try := func(r Row) (bool, error) {
		err := w.exec(ctx, []Row{r})
		if err != nil && !isDataError(err) {
			return false, err
		}
		return err == nil, nil
	}
	if len(points) > 1 {
		for _, p := range points {
			r := row
			r.Values = maps.Clone(row.Values)
			delete(r.Values, p)
			ok, err := try(r)
			if err != nil {
				return row, nil, err
			}
			if ok {
				return r, []string{p + "=" + preview(row.Values[p].(string))}, nil
			}
		}
	}
	without := func(r Row, names []string) (Row, []string) {
		r.Values = maps.Clone(r.Values)
		var removed []string
		for _, n := range names {
			removed = append(removed, fmt.Sprintf("%s=%v（%s）", n, r.Values[n], typeName(r.Values[n])))
			delete(r.Values, n)
		}
		return r, removed
	}
	if len(mismatched) > 0 {
		r, removed := without(row, mismatched)
		if ok, err := try(r); err != nil || ok {
			return r, removed, err
		}
	}
	stripped, texts := withoutText(row)
	if len(points) > 0 {
		if ok, err := try(stripped); err != nil || ok {
			return stripped, texts, err
		}
	}
	if len(points) > 0 && len(mismatched) > 0 {
		r, removed := without(stripped, mismatched)
		if ok, err := try(r); err != nil || ok {
			return r, append(texts, removed...), err
		}
	}
	return row, nil, nil
}

// mismatched 返回这一行里类型和列不一致的点位（排好序）。
func (w *Writer) mismatched(row Row) []string {
	var names []string
	for point, v := range row.Values {
		if col, ok := w.schema.Column(point); ok {
			if note, _ := conversion(col, v, w.precision); note != "" {
				names = append(names, point)
			}
		}
	}
	slices.Sort(names)
	return names
}

// checkTypes 检查值和列的类型是否一致（服务端改了点位类型、各炉台服务端版本不一致时会不一致）：
// 每个点位每种收到的类型记一次日志，写明列类型、收到的类型和怎么处理的；有损的（写 NULL、丢精度）记 WARN。
// 转换不了写成 NULL 的值计数，在每分钟的状态日志里显示。
func (w *Writer) checkTypes(rows []Row) {
	for _, row := range rows {
		for point, v := range row.Values {
			col, ok := w.schema.Column(point)
			if !ok {
				continue
			}
			note, lossy := conversion(col, v, w.precision)
			if note == "" {
				continue
			}
			if literal(col, v, w.precision) == "NULL" {
				w.nulledTotal.Add(1)
			}
			key := point + "|" + typeName(v)
			if w.typeLogged[key] {
				continue
			}
			w.typeLogged[key] = true
			args := []any{"炉台", row.Name, "点位", point, "列类型", col.ddl(), "收到类型", typeName(v), "值", fmt.Sprint(v), "处理", note}
			if lossy {
				w.log.Warn("点位类型和列不一致", args...)
			} else {
				w.log.Info("点位类型和列不一致", args...)
			}
		}
	}
}

// preview 用 %q 显示值（能看出不可见字符），太长截断。
func preview(s string) string {
	q := fmt.Sprintf("%q", s)
	if len(q) > valuePreview {
		q = q[:valuePreview] + "…"
	}
	return q
}

// withoutText 返回去掉文本值的行和被去掉的点位（点名=值，值用 %q 显示，能看出不可见字符）。
func withoutText(row Row) (Row, []string) {
	values := make(map[string]any, len(row.Values))
	var texts []string
	for k, v := range row.Values {
		if s, ok := v.(string); ok {
			texts = append(texts, k+"="+preview(s))
			continue
		}
		values[k] = v
	}
	row.Values = values
	return row, texts
}

// logReject 记录有问题的数据：哪台炉台、哪个点位、原始值（%q 显示，能看出不可见字符）、数据库报的错。
// 同一台炉台每 rejectLogEvery 最多记一次，避免刷屏；之后的次数在每分钟的状态日志里计数。
func (w *Writer) logReject(row Row, msg string, err error, points []string) {
	if t, ok := w.rejectLogged[row.Table]; ok && time.Since(t) < rejectLogEvery {
		return
	}
	w.rejectLogged[row.Table] = time.Now()
	w.log.Warn(msg, "炉台", row.Name, "点位", strings.Join(points, " "), "error", err)
}

// isDataError 判断是不是这条 SQL / 这些数据本身的问题（TDengine 返回了语法、数据类错误，如 0x216 语法错误、
// 0x26xx 解析错误、0x060B 时间戳超出范围），而不是连不上、超时、服务端忙这类 TDengine 不可用的问题。
// 不是 TDengine 返回的错误（网络、超时）一律算不可用；TDengine 返回的错误里，下面几类也算不可用：
//
//	0x0000~0x01FF 通用 / RPC（网络中断、内存不足等）  0x0200~0x020F 客户端连接
//	0x03xx mnode  0x05xx vnode（未就绪、只读等）      0x09xx 同步（选主中）
func isDataError(err error) bool {
	var te *taosErrors.TaosError
	if !errors.As(err, &te) {
		return false
	}
	code := te.Code & 0xffff
	switch {
	case code < 0x0210, code&0xff00 == 0x0300, code&0xff00 == 0x0500, code&0xff00 == 0x0900, code == 0xffff:
		return false
	}
	return true
}

// syncTags 同步子表的 device_id 标签。炉台号在连上服务端后才确定，之后也可能变化（如上位机补上了配置），
// 而 USING 自动建表只在首次创建子表时设置标签，所以每张子表在本次运行中首次写入、以及炉台号变化时更新一次。
// 失败只记日志，不影响数据写入。
func (w *Writer) syncTags(ctx context.Context, rows []Row) {
	for _, row := range rows {
		if w.tagged[row.Table] == row.DeviceID {
			continue
		}
		w.tagged[row.Table] = row.DeviceID
		if _, err := w.db.ExecContext(ctx, setTagSQL(w.database, row.Table, deviceIDTag, row.DeviceID)); err != nil {
			w.log.Warn("更新子表标签失败", "子表", row.Table, "炉台号", row.DeviceID, "error", err)
		}
	}
}

// init 创建库和超级表（已存在则跳过）、查询库的时间精度、读取现有列，并把压缩方式不一致的列改过来。
func (w *Writer) init(ctx context.Context) error {
	if err := w.createDatabase(ctx); err != nil {
		return err
	}
	var precision string
	err := w.db.QueryRowContext(ctx, precisionSQL(w.database)).Scan(&precision)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("数据库 %s 不存在，请先创建（或配置 tdengine.database_options 自动建库）", w.database)
	}
	if err != nil {
		return fmt.Errorf("查询库 %s 的时间精度: %w", w.database, err)
	}
	if _, err := w.db.ExecContext(ctx, createSuperTableSQL(w.database, w.cfg.SuperTable, ColumnName(model.HeartbeatPoint), w.cfg.ValueLength, w.compress())); err != nil {
		return fmt.Errorf("创建超级表: %w", err)
	}
	columns, err := w.describe(ctx)
	if err != nil {
		return err
	}
	w.modifyCompression(ctx, columns)

	w.precision = precision
	w.schema = NewSchema(w.cfg.ValueLength, maxRowBytes, columns)
	w.ready = true
	w.log.Info("数据库已连接", "表", w.database+"."+w.cfg.SuperTable, "列数", w.schema.ColumnCount())
	return nil
}

// createDatabase 库不存在时按 database_options 建库。建库要用不带库名的连接：带库名的连接在库不存在时连不上。
func (w *Writer) createDatabase(ctx context.Context) error {
	if w.cfg.DatabaseOptions == "" {
		return nil
	}
	server, err := sql.Open("taosWS", w.cfg.ServerDSN())
	if err != nil {
		return fmt.Errorf("打开 TDengine 连接: %w", err)
	}
	defer server.Close()
	if _, err := server.ExecContext(ctx, createDatabaseSQL(w.database, w.cfg.DatabaseOptions)); err != nil {
		return fmt.Errorf("创建库 %s: %w", w.database, err)
	}
	return nil
}

// compress 返回配置的列压缩选项。
func (w *Writer) compress() string {
	return compressClause(w.cfg.Compress, w.cfg.CompressLevel)
}

// modifyCompression 把压缩方式与配置不一致的已有列改过来（含 ts，不含标签）。失败只记日志，不影响写入。
func (w *Writer) modifyCompression(ctx context.Context, columns []Column) {
	want := w.cfg.Compress + "/" + w.cfg.CompressLevel
	modified := 0
	for _, c := range columns {
		if c.IsTag || c.Compress == "" || c.Compress == want {
			continue
		}
		if _, err := w.db.ExecContext(ctx, modifyCompressSQL(w.database, w.cfg.SuperTable, c.Name, w.compress())); err != nil {
			w.log.Warn("修改列的压缩方式失败，沿用原设置", "列", c.Name, "压缩", c.Compress, "error", err)
			continue
		}
		modified++
	}
	if modified > 0 {
		w.log.Info("已修改列的压缩方式", "列数", modified, "压缩", want)
	}
}

func (w *Writer) describe(ctx context.Context) ([]Column, error) {
	rows, err := w.db.QueryContext(ctx, describeSQL(w.database, w.cfg.SuperTable))
	if err != nil {
		return nil, fmt.Errorf("DESCRIBE: %w", err)
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(names) < 4 {
		return nil, fmt.Errorf("DESCRIBE 返回 %d 列，期望至少 4 列", len(names))
	}
	values := make([]any, len(names))
	pointers := make([]any, len(names))
	for i := range values {
		pointers[i] = &values[i]
	}

	var columns []Column
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("DESCRIBE: %w", err)
		}
		length, _ := strconv.Atoi(toText(values[2]))
		col := Column{
			Name:   toText(values[0]),
			Type:   strings.ToUpper(strings.TrimSpace(toText(values[1]))),
			Length: length,
			IsTag:  toText(values[3]) == "TAG",
		}
		// 3.3 起 DESCRIBE 还有 encode、compress、level 三列
		if len(values) > 6 {
			col.Compress = toText(values[5]) + "/" + toText(values[6])
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// addColumns 为新出现的点位按值的类型新增列。ALTER 失败时下次写入前重新读取表结构（列可能已被其他实例加上）。
func (w *Writer) addColumns(ctx context.Context, rows []Row) error {
	samples := map[string]any{}
	for _, row := range rows {
		for point, value := range row.Values {
			if samples[point] == nil {
				samples[point] = value
			}
		}
	}

	additions, rejections := w.schema.Resolve(samples)
	for _, r := range rejections {
		w.log.Warn("点位无法入库", "点位", r.Point, "原因", r.Reason)
	}
	added := 0
	defer func() {
		if added > 0 {
			w.log.Info("新增点位列", "新增", added, "列数", w.schema.ColumnCount())
		}
	}()
	for _, a := range additions {
		if _, err := w.db.ExecContext(ctx, addColumnSQL(w.database, w.cfg.SuperTable, a.Column, w.compress())); err != nil {
			w.ready = false
			return fmt.Errorf("新增列 %s: %w", a.Column.Name, err)
		}
		w.schema.Added(a)
		added++
		w.log.Debug("新增点位列", "point", a.Point, "column", a.Column.Name, "type", a.Column.ddl())
	}
	return nil
}
