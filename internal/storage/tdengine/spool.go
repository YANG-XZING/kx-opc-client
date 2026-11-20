package tdengine

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// 落盘暂存（spool）：TDengine 写不进去时（断网、服务停了），数据不丢弃，而是追加到暂存目录的分段文件里，
// 恢复后按时间顺序补写，补写完删除文件。程序重启后也会接着补写上次没补完的文件。
//
// 文件是 gob 编码的 Row 序列，每段一个编码器，写满 segmentBytes 换下一段。
// 补写某一段时如果又断了，下次从这一段开头重写：同一子表同一 ts 的行会覆盖，重复写入没有副作用。

const (
	segmentBytes  = 32 << 20 // 每段文件最大 32MB
	segmentPrefix = "seg-"
	segmentSuffix = ".gob"
)

func init() {
	// Row.Values 是 map[string]any，gob 编码接口值需要先注册具体类型；基本类型 gob 已内置，时间需要注册
	gob.Register(time.Time{})
}

type spool struct {
	dir      string
	maxBytes int64
	log      *slog.Logger
	size     int64 // 已写完（不含正在写）的段文件总字节数
	files    int   // 已写完、等待补写的段文件数（每秒都要判断有没有暂存数据，不每次去读目录）

	// 正在写入的段
	active *os.File
	buf    *bufio.Writer
	enc    *gob.Encoder
	count  *countingWriter

	// 正在补写的段
	reading  string
	readFile *os.File
	dec      *gob.Decoder
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func newSpool(dir string, maxMB int, log *slog.Logger) (*spool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("创建暂存目录 %s: %w", dir, err)
	}
	// 启动时就确认能写，免得等到数据库断开、真要暂存时才发现没有权限
	probe := filepath.Join(dir, ".write-test")
	if err := os.WriteFile(probe, nil, 0o640); err != nil {
		return nil, fmt.Errorf("暂存目录 %s 不可写: %w", dir, err)
	}
	_ = os.Remove(probe)

	s := &spool{dir: dir, maxBytes: int64(maxMB) << 20, log: log}
	for _, p := range s.segments() {
		if fi, err := os.Stat(p); err == nil {
			s.size += fi.Size()
			s.files++
		}
	}
	if s.files > 0 {
		log.Info("发现上次没补写完的暂存数据，数据库可用后自动补写", "目录", dir, "文件数", s.files, "大小MB", s.size>>20)
	}
	return s, nil
}

// segments 返回全部段文件，按时间（文件名）排序。
func (s *spool) segments() []string {
	entries, _ := os.ReadDir(s.dir)
	var out []string
	for _, e := range entries {
		if n := e.Name(); strings.HasPrefix(n, segmentPrefix) && strings.HasSuffix(n, segmentSuffix) {
			out = append(out, filepath.Join(s.dir, n))
		}
	}
	slices.Sort(out)
	return out
}

// hasData 返回是否还有没补写的暂存数据（包括正在写的段）。
func (s *spool) hasData() bool {
	return s.active != nil || s.files > 0
}

// full 返回暂存目录是否已到上限。
func (s *spool) full() bool {
	return s.size+s.activeBytes() >= s.maxBytes
}

func (s *spool) activeBytes() int64 {
	if s.count == nil {
		return 0
	}
	return s.count.n
}

// append 把行追加到暂存文件，返回写进去的行数；目录满了或写盘失败时返回已写入的部分。
func (s *spool) append(rows []Row) (int, error) {
	for i := range rows {
		if s.full() {
			return i, errors.New("暂存目录已满")
		}
		if s.active == nil {
			if err := s.openSegment(); err != nil {
				return i, err
			}
		}
		if err := s.enc.Encode(&rows[i]); err != nil {
			return i, fmt.Errorf("写暂存文件: %w", err)
		}
		if s.activeBytes() >= segmentBytes {
			if err := s.closeActive(); err != nil {
				return i + 1, err
			}
		}
	}
	if s.buf != nil {
		if err := s.buf.Flush(); err != nil {
			return len(rows), fmt.Errorf("写暂存文件: %w", err)
		}
	}
	return len(rows), nil
}

func (s *spool) openSegment() error {
	name := fmt.Sprintf("%s%020d%s", segmentPrefix, time.Now().UnixNano(), segmentSuffix)
	f, err := os.OpenFile(filepath.Join(s.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("创建暂存文件: %w", err)
	}
	s.active = f
	s.count = &countingWriter{w: f}
	s.buf = bufio.NewWriterSize(s.count, 256<<10)
	s.enc = gob.NewEncoder(s.buf)
	return nil
}

// closeActive 写完并关闭正在写的段，之后它就可以补写了。
func (s *spool) closeActive() error {
	if s.active == nil {
		return nil
	}
	err := s.buf.Flush()
	if cerr := s.active.Close(); err == nil {
		err = cerr
	}
	s.size += s.count.n
	s.files++
	s.active, s.buf, s.enc, s.count = nil, nil, nil, nil
	return err
}

// next 从最旧的段里读出最多 limit 行；一段读完时 done 为 true（调用方补写成功后再调用 finish 删除它）。
// 没有可补写的数据时返回 nil, "", false。
func (s *spool) next(limit int) (rows []Row, path string, done bool, err error) {
	if s.dec == nil {
		segs := s.segments()
		if s.active != nil && len(segs) > 0 && segs[len(segs)-1] == s.active.Name() {
			segs = segs[:len(segs)-1] // 正在写的段先不补
		}
		s.files = len(segs) // 以目录为准（文件被手工删掉等情况下计数也不会错）
		if len(segs) == 0 {
			return nil, "", false, nil
		}
		f, err := os.Open(segs[0])
		if err != nil {
			// 打不开（没有权限等）：移走，避免每秒重试刷屏，也不挡住后面的文件
			bad := segs[0] + ".bad"
			if rerr := os.Rename(segs[0], bad); rerr == nil {
				s.files--
				s.log.Error("暂存文件打不开，已改名跳过", "文件", bad, "error", err)
			}
			return nil, "", false, fmt.Errorf("打开暂存文件: %w", err)
		}
		s.reading, s.readFile, s.dec = segs[0], f, gob.NewDecoder(bufio.NewReaderSize(f, 256<<10))
	}
	for len(rows) < limit {
		var r Row
		err := s.dec.Decode(&r)
		if errors.Is(err, io.EOF) {
			return rows, s.reading, true, nil
		}
		if err != nil {
			// 文件末尾不完整（如上次写到一半断电）：前面读出的照常补写，这一段到此为止
			s.log.Warn("暂存文件损坏，跳过剩余部分", "文件", s.reading, "error", err)
			return rows, s.reading, true, nil
		}
		rows = append(rows, r)
	}
	return rows, s.reading, false, nil
}

// finish 一段补写完：删除文件。
func (s *spool) finish(path string) {
	s.stopReading()
	if fi, err := os.Stat(path); err == nil {
		s.size -= fi.Size()
	}
	s.files = max(s.files-1, 0)
	if err := os.Remove(path); err != nil {
		s.log.Warn("删除已补写的暂存文件失败", "文件", path, "error", err)
	}
}

// stopReading 中断补写（TDengine 又不可用了）：下次从这一段开头重新补写。
func (s *spool) stopReading() {
	if s.readFile != nil {
		_ = s.readFile.Close()
	}
	s.reading, s.readFile, s.dec = "", nil, nil
}

// close 程序退出时调用：写完正在写的段，关闭正在读的段。
func (s *spool) close() {
	if err := s.closeActive(); err != nil {
		s.log.Warn("关闭暂存文件失败", "error", err)
	}
	s.stopReading()
}
