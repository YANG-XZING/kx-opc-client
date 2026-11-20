package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"kx-opc-client/internal/opc"
)

// probe 连一台炉台读一遍全部点位的当前值，打印出来后退出（排查用，不写库、不影响正在运行的采集服务）。
// 文本值用 %q 显示，含不可见字符或非法 UTF-8 时再打印十六进制，用来查“到底传了什么字符”。
func probe(endpoint string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := opc.Dial(ctx, endpoint, nil)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	points, err := c.Points(ctx)
	if err != nil {
		return err
	}
	values, err := c.ReadValues(ctx, points)
	if err != nil {
		return err
	}
	fmt.Printf("炉台 %s（%s），%d 个点位\n\n", c.DeviceName(), endpoint, len(points))
	names := make([]string, 0, len(values))
	for n := range values {
		names = append(names, n)
	}
	slices.Sort(names)
	suspicious := 0
	for _, n := range names {
		v := values[n]
		s, isText := v.(string)
		if !isText {
			fmt.Printf("  %-28s %-8T %v\n", n, v, v)
			continue
		}
		bad := !utf8.ValidString(s) || strings.IndexFunc(s, func(r rune) bool { return r != ' ' && unicode.IsControl(r) }) >= 0
		mark := ""
		if bad {
			mark = "  <== 含不可见字符或非法编码"
			suspicious++
		}
		fmt.Printf("  %-28s string   %q（%d 字节）%s\n", n, s, len(s), mark)
		if bad {
			fmt.Printf("  %-28s 十六进制 %s\n", "", hex.EncodeToString([]byte(s)))
		}
	}
	fmt.Printf("\n含不可见字符或非法编码的文本点位：%d 个\n", suspicious)
	return nil
}
