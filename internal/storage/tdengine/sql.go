package tdengine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const tagLength = 64

// Row 是待写入的一行：某台炉台某一采集时刻的全部点位。
type Row struct {
	Table                string
	DeviceID             string
	DeviceIP             string
	Name                 string         // 炉台名（炉台号，没有时为 IP），只用于日志和按炉台计数，不写库
	Time                 time.Time      // 采集时间，写入 ts 列
	Values               map[string]any // 点名 -> 值（保留 OPC 节点类型）
	HeartbeatJumpSeconds int64
}

// compressClause 返回列的压缩选项，如 COMPRESS 'zstd' LEVEL 'high'。
func compressClause(algo, level string) string {
	return fmt.Sprintf("COMPRESS '%s' LEVEL '%s'", algo, level)
}

func createDatabaseSQL(db, options string) string {
	return strings.TrimSpace(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` %s", db, options))
}

// createSuperTableSQL 建超级表，ts 和首列都带压缩选项（标签不支持列压缩）。
func createSuperTableSQL(db, stable, firstColumn string, valueLength int, compress string) string {
	return fmt.Sprintf("CREATE STABLE IF NOT EXISTS `%s`.`%s` (`%s` TIMESTAMP %s, `%s` VARCHAR(%d) %s) TAGS (`%s` VARCHAR(%d), `%s` VARCHAR(%d))",
		db, stable, tsColumn, compress, firstColumn, valueLength, compress, deviceIDTag, tagLength, deviceIPTag, tagLength)
}

func addColumnSQL(db, stable string, col Column, compress string) string {
	return fmt.Sprintf("ALTER STABLE `%s`.`%s` ADD COLUMN `%s` %s %s", db, stable, col.Name, col.ddl(), compress)
}

// modifyCompressSQL 修改已有列的压缩方式，只影响之后落盘的数据。
func modifyCompressSQL(db, stable, column, compress string) string {
	return fmt.Sprintf("ALTER STABLE `%s`.`%s` MODIFY COLUMN `%s` %s", db, stable, column, compress)
}

// setTagSQL 更新已存在子表的标签。
func setTagSQL(db, table, tag, value string) string {
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` SET TAG `%s`='%s'", db, table, tag, escape(truncate(value, tagLength)))
}

func describeSQL(db, stable string) string {
	return fmt.Sprintf("DESCRIBE `%s`.`%s`", db, stable)
}

func precisionSQL(db string) string {
	return fmt.Sprintf("SELECT `precision` FROM information_schema.ins_databases WHERE name = '%s'", escape(db))
}

// buildInserts 生成多表批量 INSERT，按 maxBytes 切分。子表不存在时由 USING 子句自动创建。
//
// 同一子表的多行（如 TDengine 故障恢复后补写的积压数据）合并到一个子句里，列为这些行的并集，缺失的值写 NULL。
// 没有映射到列的点（被拒绝的点）跳过。值按列类型写成对应的字面量。
func buildInserts(db, stable string, rows []Row, column func(point string) (Column, bool), precision string, maxBytes int) []string {
	const head = "INSERT INTO"
	var (
		statements []string
		sql        strings.Builder
		hasRows    bool
	)
	sql.WriteString(head)
	flush := func() {
		statements = append(statements, sql.String())
		sql.Reset()
		sql.WriteString(head)
		hasRows = false
	}

	for _, group := range groupByTable(rows) {
		columns := unionColumns(group, column)
		if len(columns) == 0 {
			continue
		}
		latest := group[len(group)-1]
		header := tableHeader(db, stable, latest, columns)
		headerWritten := false

		for _, row := range group {
			tuple, ok := valuesTuple(row, columns, precision)
			if !ok {
				continue
			}
			needed := len(tuple)
			if !headerWritten {
				needed += len(header)
			}
			if hasRows && sql.Len()+needed > maxBytes {
				flush()
				headerWritten = false
			}
			if !headerWritten {
				sql.WriteString(header)
				headerWritten = true
			}
			sql.WriteString(tuple)
			hasRows = true
		}
	}
	if hasRows {
		statements = append(statements, sql.String())
	}
	return statements
}

// groupByTable 按子表分组（保持子表首次出现的顺序），组内按时间排序。
func groupByTable(rows []Row) [][]Row {
	var (
		order  []string
		groups = map[string][]Row{}
	)
	for _, row := range rows {
		if _, ok := groups[row.Table]; !ok {
			order = append(order, row.Table)
		}
		groups[row.Table] = append(groups[row.Table], row)
	}
	result := make([][]Row, 0, len(order))
	for _, table := range order {
		group := groups[table]
		sort.SliceStable(group, func(i, j int) bool { return group[i].Time.Before(group[j].Time) })
		result = append(result, group)
	}
	return result
}

// unionColumns 返回一组行用到的全部列，按列名排序。
func unionColumns(rows []Row, column func(string) (Column, bool)) []columnRef {
	seen := map[string]bool{}
	var columns []columnRef
	for _, row := range rows {
		for point := range row.Values {
			if col, ok := column(point); ok && !seen[col.Name] {
				seen[col.Name] = true
				columns = append(columns, columnRef{Column: col, point: point})
			}
		}
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
	return columns
}

type columnRef struct {
	Column
	point string
}

func tableHeader(db, stable string, row Row, columns []columnRef) string {
	var b strings.Builder
	fmt.Fprintf(&b, " `%s`.`%s` USING `%s`.`%s` TAGS ('%s','%s') (`%s`",
		db, row.Table, db, stable,
		escape(truncate(row.DeviceID, tagLength)), escape(truncate(row.DeviceIP, tagLength)), tsColumn)
	for _, c := range columns {
		b.WriteString(",`")
		b.WriteString(c.Name)
		b.WriteByte('`')
	}
	b.WriteString(") VALUES")
	return b.String()
}

// valuesTuple 生成一行的 VALUES 元组；整行都没有可写的值时返回 false。
// Schema 保证一列只对应一个点，本行没有该点时写 NULL。
func valuesTuple(row Row, columns []columnRef, precision string) (string, bool) {
	var b strings.Builder
	b.WriteString(" (")
	b.WriteString(strconv.FormatInt(timestamp(row.Time, precision), 10))
	hasValue := false
	for _, c := range columns {
		value, ok := row.Values[c.point]
		b.WriteByte(',')
		if !ok {
			b.WriteString("NULL")
			continue
		}
		b.WriteString(literal(c.Column, value, precision))
		hasValue = true
	}
	b.WriteByte(')')
	return b.String(), hasValue
}

// literal 按列类型生成值的 SQL 字面量。新列按值的类型创建，类型总是一致；
// 已存在的列类型不同时（服务端改了点位类型、各炉台服务端版本不一致）按列类型转换，
// 无法转换或超出列类型范围时写 NULL：越界的值写进去 TDengine 会报错，整行都写不进去。
func literal(col Column, v any, precision string) string {
	if v == nil {
		return "NULL"
	}
	switch {
	case col.isText():
		return "'" + escape(truncate(toText(v), col.Length)) + "'"
	case col.Type == "BOOL":
		if b, ok := toBool(v); ok {
			return strconv.FormatBool(b)
		}
	case col.Type == "TIMESTAMP":
		if t, ok := v.(time.Time); ok {
			return strconv.FormatInt(timestamp(t, precision), 10)
		}
	case col.Type == "FLOAT" || col.Type == "DOUBLE":
		if f, ok := toFloat(v); ok && (col.Type == "DOUBLE" || math.Abs(f) <= math.MaxFloat32) {
			bits := 64
			if _, single := v.(float32); single {
				bits = 32
			}
			return strconv.FormatFloat(f, 'g', -1, bits)
		}
	case strings.Contains(col.Type, "INT"):
		if s, ok := intLiteral(col.Type, v); ok {
			return s
		}
	}
	return "NULL"
}

// intLiteral 把值写成整数列的字面量（非整数四舍五入）；超出列类型范围（如 TINYINT 收到 1000、
// 无符号列收到负数）或无法转换时返回 false。
func intLiteral(colType string, v any) (string, bool) {
	bits := uint(64)
	switch {
	case strings.HasPrefix(colType, "TINYINT"):
		bits = 8
	case strings.HasPrefix(colType, "SMALLINT"):
		bits = 16
	case strings.HasPrefix(colType, "INT"):
		bits = 32
	}
	unsigned := strings.HasSuffix(colType, "UNSIGNED")
	maxU := uint64(math.MaxUint64) >> (64 - bits)
	maxI := int64(maxU >> 1)
	minI := -maxI - 1

	switch x := v.(type) {
	case uint8, uint16, uint32, uint64:
		u, _ := strconv.ParseUint(fmt.Sprint(x), 10, 64)
		if (unsigned && u <= maxU) || (!unsigned && u <= uint64(maxI)) {
			return strconv.FormatUint(u, 10), true
		}
		return "", false
	case int8, int16, int32, int64:
		i, _ := strconv.ParseInt(fmt.Sprint(x), 10, 64)
		if (unsigned && i >= 0 && uint64(i) <= maxU) || (!unsigned && i >= minI && i <= maxI) {
			return strconv.FormatInt(i, 10), true
		}
		return "", false
	}
	f, ok := toFloat(v)
	if !ok {
		return "", false
	}
	f = math.Round(f)
	if unsigned {
		if f < 0 || f > float64(maxU) || (bits == 64 && f >= 1<<64) {
			return "", false
		}
		return strconv.FormatUint(uint64(f), 10), true
	}
	if f < float64(minI) || f > float64(maxI) || (bits == 64 && f >= 1<<63) {
		return "", false
	}
	return strconv.FormatInt(int64(f), 10), true
}

// conversion 判断值和列的类型是否一致；不一致时返回处理方式和是否有损（写了 NULL 或丢了精度）。
// 一致时返回空字符串。
func conversion(col Column, v any, precision string) (note string, lossy bool) {
	if v == nil {
		return "", false
	}
	_, isText := v.(string)
	if col.isText() == isText && (isText || columnFor("", v, 0).Type == col.Type) {
		return "", false
	}
	if literal(col, v, precision) == "NULL" {
		return "无法转换或超出范围，写 NULL", true
	}
	switch x := v.(type) {
	case float64:
		if col.Type == "FLOAT" {
			return "Double 存进 FLOAT 列，精度降为 32 位", true
		}
		if strings.Contains(col.Type, "INT") && x != math.Round(x) {
			return "四舍五入后写入", true
		}
	case float32:
		if strings.Contains(col.Type, "INT") && x != float32(math.Round(float64(x))) {
			return "四舍五入后写入", true
		}
	case string:
		return "字符串按列类型解析后写入", false
	}
	if col.Type == "BOOL" {
		if _, ok := v.(bool); !ok {
			return "数值转成布尔（非 0 为真），数值信息丢失", true
		}
	}
	return "按列类型转换后写入", false
}

// typeName 返回值对应的 OPC 数据类型名，用于日志。
func typeName(v any) string {
	switch v.(type) {
	case bool:
		return "Boolean"
	case int8:
		return "SByte"
	case uint8:
		return "Byte"
	case int16:
		return "Int16"
	case uint16:
		return "UInt16"
	case int32:
		return "Int32"
	case uint32:
		return "UInt32"
	case int64:
		return "Int64"
	case uint64:
		return "UInt64"
	case float32:
		return "Float"
	case float64:
		return "Double"
	case string:
		return "String"
	case time.Time:
		return "DateTime"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// toFloat 转为有限的 float64；布尔为 1/0，字符串按数字解析。
func toFloat(v any) (float64, bool) {
	var f float64
	switch x := v.(type) {
	case bool:
		if x {
			f = 1
		}
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

// toBool 转为布尔；数值非 0 为真，字符串按 true/false/1/0 解析。
func toBool(v any) (bool, bool) {
	if b, ok := v.(bool); ok {
		return b, true
	}
	if s, ok := v.(string); ok {
		b, err := strconv.ParseBool(strings.TrimSpace(s))
		return b, err == nil
	}
	f, ok := toFloat(v)
	return f != 0, ok
}

// toText 转为字符串，用于写入 VARCHAR 列和读取 DESCRIBE 结果。
func toText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return x.Format("2006-01-02 15:04:05.000")
	default:
		return fmt.Sprint(x)
	}
}

// timestamp 按库的时间精度换算 Unix 时间戳，避免依赖时区解析。
func timestamp(t time.Time, precision string) int64 {
	switch precision {
	case "us":
		return t.UnixMicro()
	case "ns":
		return t.UnixNano()
	default:
		return t.UnixMilli()
	}
}

// escape 转义单引号字符串字面量。
// PLC 的字符串常是定长缓冲区，后面补 \0（如 "V6.17.1.1-KA250601-1.1.0\x00\x00…"）：\0 会让 TDengine 认为字符串没结束，
// 整条 INSERT 报 syntax error，同一批里所有炉台的数据都写不进去。所以先去掉控制字符（换行、制表换成空格），
// 再把非法 UTF-8 字节去掉，最后转义。
func escape(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.Map(func(r rune) rune {
		switch {
		case r == '\t' || r == '\n' || r == '\r':
			return ' '
		case r < 0x20 || r == 0x7f:
			return -1
		}
		return r
	}, s)
	s = strings.TrimRight(s, " ")
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// truncate 按 UTF-8 字节数截断（TDengine VARCHAR 长度单位是字节），不截断半个字符。
func truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
