// Package config 加载并校验两个配置文件：config.yaml（运行参数）和 furnaces.json（炉台清单）。
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// Config 对应 config.yaml。
type Config struct {
	TDengine  TDengine  `yaml:"tdengine"`
	Collector Collector `yaml:"collector"`
	Log       Log       `yaml:"log"`
}

// TDengine 是历史库配置。
type TDengine struct {
	// DSN 使用 driver-go 的 taosWS 格式，如 root:taosdata@ws(10.65.104.171:6041)/kx_opc
	DSN string `yaml:"dsn"`
	// DatabaseOptions 库不存在时自动建库用的参数（CREATE DATABASE 库名 之后的部分）；为空则不自动建库
	DatabaseOptions string `yaml:"database_options"`
	// Compress、CompressLevel 每一列（含 ts）的压缩算法和级别，建表、加列时写进 COMPRESS ... LEVEL ...；
	// 已有列不一致时启动后自动 MODIFY COLUMN
	Compress      string `yaml:"compress"`
	CompressLevel string `yaml:"compress_level"`
	// SuperTable 超级表名，每台炉台一张子表
	SuperTable string `yaml:"super_table"`
	// TablePrefix 子表名前缀，子表名 = 前缀 + IP（点换成下划线）
	TablePrefix string `yaml:"table_prefix"`
	// ValueLength 字符串类点位（OPC 类型为 String）新建 VARCHAR 列的长度（字节），超长截断
	ValueLength int `yaml:"value_length"`
	// SpoolDir TDengine 写不进去时数据落盘暂存的目录，恢复后自动补写；默认 /home/kx-opc-client/data；
	// 相对路径基于 config.yaml 所在目录；为空则不暂存（丢弃）
	SpoolDir string `yaml:"spool_dir"`
	// SpoolMaxMB 暂存目录最多占多少 MB，满了之后新数据丢弃
	SpoolMaxMB int `yaml:"spool_max_mb"`
}

// Collector 是采集配置。
type Collector struct {
	// FurnacesFile 炉台清单，相对路径基于 config.yaml 所在目录
	FurnacesFile string `yaml:"furnaces_file"`
	// FurnacesReloadInterval 重读炉台清单的周期，只新增炉台不减少
	FurnacesReloadInterval time.Duration `yaml:"furnaces_reload_interval"`
	// HeartbeatTimeout 心跳超过该时长不变化即判定停滞（上位机断开），用于状态汇总；0 表示不检查
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
}

// Log 是日志配置。
type Log struct {
	Level string `yaml:"level"` // debug / info / warn / error
	File  string `yaml:"file"`  // 为空则只输出到控制台（journald）
}

// DefaultHeartbeatTimeout 是默认的心跳停滞判定时间。
const DefaultHeartbeatTimeout = 30 * time.Second

func defaults() Config {
	return Config{
		TDengine: TDengine{
			SuperTable:      "opc_data",
			TablePrefix:     "opc_",
			ValueLength:     256,
			DatabaseOptions: "PRECISION 'ms' KEEP 1095d DURATION 10d MAXROWS 40000 MINROWS 1000 COMP 2 CACHEMODEL 'last_row'",
			Compress:        "zstd",
			CompressLevel:   "high",
			SpoolDir:        "/home/kx-opc-client/data",
			SpoolMaxMB:      2048,
		},
		Collector: Collector{
			FurnacesFile:           "furnaces.json",
			FurnacesReloadInterval: 24 * time.Hour,
			HeartbeatTimeout:       DefaultHeartbeatTimeout,
		},
		Log: Log{Level: "info", File: "logs/kx-opc-client.log"},
	}
}

// Load 读取 config.yaml。炉台清单路径、日志路径如为相对路径，转换为基于配置文件目录的绝对路径。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件: %w", err)
	}
	cfg := defaults()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	cfg.Collector.FurnacesFile = resolve(dir, cfg.Collector.FurnacesFile)
	if cfg.Log.File != "" {
		cfg.Log.File = resolve(dir, cfg.Log.File)
	}
	if cfg.TDengine.SpoolDir != "" {
		cfg.TDengine.SpoolDir = resolve(dir, cfg.TDengine.SpoolDir)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("配置文件 %s:\n%w", path, err)
	}
	return &cfg, nil
}

// Database 从 DSN 中解析库名。
func (t TDengine) Database() string {
	dsn, _, _ := strings.Cut(t.DSN, "?")
	if i := strings.LastIndexByte(dsn, '/'); i >= 0 {
		return dsn[i+1:]
	}
	return ""
}

// ServerDSN 返回去掉库名的 DSN，用于库还不存在时连上去建库。
func (t TDengine) ServerDSN() string {
	dsn, params, hasParams := strings.Cut(t.DSN, "?")
	if i := strings.LastIndexByte(dsn, '/'); i >= 0 {
		dsn = dsn[:i+1]
	}
	if hasParams {
		dsn += "?" + params
	}
	return dsn
}

// Address 从 DSN 中解析 TDengine 地址（host:port），用于日志，不含用户名密码。
func (t TDengine) Address() string {
	_, rest, ok := strings.Cut(t.DSN, "(")
	if !ok {
		return ""
	}
	address, _, _ := strings.Cut(rest, ")")
	return address
}

func (c Config) validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	t := c.TDengine
	check(t.DSN != "", "tdengine.dsn 不能为空")
	check(identifierPattern.MatchString(t.Database()), "tdengine.dsn 中必须包含合法的库名，如 .../kayex_data，当前: %q", t.Database())
	check(identifierPattern.MatchString(t.SuperTable), "tdengine.super_table 不合法: %q", t.SuperTable)
	check(t.TablePrefix == "" || identifierPattern.MatchString(t.TablePrefix), "tdengine.table_prefix 不合法: %q", t.TablePrefix)
	check(t.ValueLength >= 1 && t.ValueLength <= 16_384, "tdengine.value_length 必须在 1~16384 之间")
	check(t.SpoolDir == "" || t.SpoolMaxMB >= 10, "tdengine.spool_max_mb 不能小于 10")
	check(!strings.ContainsAny(t.DatabaseOptions, ";`"), "tdengine.database_options 不能包含分号和反引号")
	check(slices.Contains([]string{"lz4", "zlib", "zstd", "xz", "disabled"}, t.Compress), "tdengine.compress 必须是 lz4 / zlib / zstd / xz / disabled，当前: %q", t.Compress)
	check(slices.Contains([]string{"low", "medium", "high"}, t.CompressLevel), "tdengine.compress_level 必须是 low / medium / high，当前: %q", t.CompressLevel)

	col := c.Collector
	check(col.FurnacesReloadInterval >= time.Minute, "collector.furnaces_reload_interval 不能小于 1m")
	check(col.HeartbeatTimeout >= 0, "collector.heartbeat_timeout 不能为负")

	return errors.Join(errs...)
}

func resolve(dir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}
