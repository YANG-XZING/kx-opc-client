// kx-opc-client：多炉台 OPC UA 采集客户端，订阅 furnaces.json 中各炉台的全部点位，服务端心跳每变化一次写一行到 TDengine。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"gopkg.in/natefinch/lumberjack.v2"

	"kx-opc-client/internal/app"
	"kx-opc-client/internal/config"
	"kx-opc-client/internal/model"
	"kx-opc-client/internal/scanner"
)

// version 在构建时通过 -ldflags "-X main.version=..." 注入
var version = "dev"

// 日志文件滚动参数
const (
	logMaxSizeMB  = 100
	logMaxBackups = 30
	logMaxAgeDays = 30
)

func main() {
	configPath := flag.String("config", defaultConfigPath(), "配置文件路径")
	check := flag.Bool("check", false, "只校验配置并列出炉台，不启动采集")
	showVersion := flag.Bool("version", false, "打印版本号")
	probeIP := flag.String("probe", "", "排查用：连这台炉台（IP）读一遍全部点位并打印原始值，不启动采集、不写库")
	probePort := flag.Int("port", 4840, "配合 -probe：炉台 OPC UA 端口")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		exit(err)
	}
	if *probeIP != "" {
		if err := probe(fmt.Sprintf("opc.tcp://%s:%d", *probeIP, *probePort)); err != nil {
			exit(err)
		}
		return
	}
	if *check {
		if err := printFurnaces(cfg); err != nil {
			exit(err)
		}
		return
	}

	log, closeLog := newLogger(cfg.Log)
	defer closeLog()
	log.Info("kx-opc-client 启动", "version", version, "config", *configPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, cfg, log); err != nil {
		log.Error("启动失败", "error", err)
		os.Exit(1)
	}
}

func exit(err error) {
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(2)
}

// defaultConfigPath 默认读取程序同目录下的 config.yaml。
func defaultConfigPath() string {
	exe, err := os.Executable()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(filepath.Dir(exe), "config.yaml")
}

func newLogger(cfg config.Log) (*slog.Logger, func()) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(cfg.Level))); err != nil {
		level = slog.LevelInfo
	}
	var out io.Writer = os.Stdout
	closeLog := func() {}
	if cfg.File != "" {
		file := &lumberjack.Logger{Filename: cfg.File, MaxSize: logMaxSizeMB, MaxBackups: logMaxBackups, MaxAge: logMaxAgeDays, LocalTime: true}
		out = io.MultiWriter(os.Stdout, file)
		closeLog = func() { _ = file.Close() }
	}
	return slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: level})), closeLog
}

// printFurnaces 列出将要采集的炉台；IP 范围会实际探测一次端口，只列出能连通的。
func printFurnaces(cfg *config.Config) error {
	list, err := config.LoadFurnaces(cfg.Collector.FurnacesFile, cfg.TDengine.TablePrefix)
	if err != nil {
		return err
	}
	reachable := scanner.Reachable(context.Background(), list.Ranged, scanner.TCPProbe)
	fmt.Printf("配置校验通过：TDengine %s.%s\n", cfg.TDengine.Database(), cfg.TDengine.SuperTable)
	fmt.Printf("单独写出的炉台 %d 台（一定采集）；IP 范围共 %d 个 IP，端口能连通 %d 台\n\n", len(list.Fixed), len(list.Ranged), len(reachable))
	fmt.Printf("炉台号以上位机配置的设备名为准（连上后确定）；上位机为默认值 %s 时用下表的网段前缀炉台号，没有则就用 %s\n\n", model.DefaultDeviceName, model.DefaultDeviceName)
	fmt.Printf("%-28s %-22s %-14s %s\n", "OPC 端点", "子表", "网段前缀炉台号", "来源")
	row := func(f model.Furnace, from string) {
		areaID := f.AreaDeviceID
		if areaID == "" {
			areaID = "-"
		}
		fmt.Printf("%-28s %-22s %-14s %s\n", f.Endpoint(), f.Table, areaID, from)
	}
	for _, f := range list.Fixed {
		row(f, "单独写出")
	}
	for _, f := range reachable {
		row(f, "IP 范围")
	}
	return nil
}
