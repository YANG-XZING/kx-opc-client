package tdengine

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	maxColumns          = 4096 // TDengine 单表最大列数（含 ts）
	maxColumnNameLength = 64   // TDengine 列名最大长度
	varcharOverhead     = 2    // 每个变长列额外占用的字节数

	tsColumn       = "ts" // 数据采集时间
	deviceIDTag    = "device_id"
	deviceIPTag    = "device_ip"
	reservedPrefix = "p_"
)

var invalidColumnChars = regexp.MustCompile(`[^A-Za-z0-9_]`)

// ColumnName 把点名转换成列名：保留原始大小写（所有 SQL 中列名都用反引号包裹，TDengine 区分大小写），
// 非法字符替换为下划线；数字开头或与 ts、标签同名（不区分大小写）时加 p_ 前缀；最长 64 字符。
// 无法转换（如纯中文）时返回空。
func ColumnName(point string) string {
	name := invalidColumnChars.ReplaceAllString(point, "_")
	if strings.Trim(name, "_") == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' || strings.EqualFold(name, tsColumn) || strings.EqualFold(name, deviceIDTag) || strings.EqualFold(name, deviceIPTag) {
		name = reservedPrefix + name
	}
	return name[:min(len(name), maxColumnNameLength)]
}

// Column 是超级表的一列。Type 为 TDengine 类型名（大写，不含长度），如 FLOAT、INT UNSIGNED、VARCHAR。
type Column struct {
	Name   string
	Type   string
	Length int // 变长类型（VARCHAR 等）为声明长度，定长类型为占用字节数
	IsTag  bool
	// Compress 现有列的压缩算法/级别，如 zstd/high（DESCRIBE 读出，TDengine 3.3 起才有，读不到为空）
	Compress string
}

// rowBytes 返回该列在一行中占用的字节数。
func (c Column) rowBytes() int {
	switch {
	case c.Type == "NCHAR":
		return c.Length*4 + varcharOverhead
	case c.isText():
		return c.Length + varcharOverhead
	default:
		return c.Length
	}
}

func (c Column) isText() bool {
	switch c.Type {
	case "VARCHAR", "BINARY", "NCHAR", "VARBINARY", "JSON", "GEOMETRY":
		return true
	}
	return false
}

// ddl 返回建列用的类型声明，如 FLOAT、VARCHAR(256)。
func (c Column) ddl() string {
	if c.isText() {
		return fmt.Sprintf("%s(%d)", c.Type, c.Length)
	}
	return c.Type
}

// columnFor 按值的类型确定新列：OPC 节点类型（HMI_Value.XML 的 type）对应的 TDengine 类型。
func columnFor(name string, value any, valueLength int) Column {
	fixed := func(t string, size int) Column { return Column{Name: name, Type: t, Length: size} }
	switch value.(type) {
	case bool:
		return fixed("BOOL", 1)
	case int8:
		return fixed("TINYINT", 1)
	case uint8:
		return fixed("TINYINT UNSIGNED", 1)
	case int16:
		return fixed("SMALLINT", 2)
	case uint16:
		return fixed("SMALLINT UNSIGNED", 2)
	case int32:
		return fixed("INT", 4)
	case uint32:
		return fixed("INT UNSIGNED", 4)
	case int64:
		return fixed("BIGINT", 8)
	case uint64:
		return fixed("BIGINT UNSIGNED", 8)
	case float32:
		return fixed("FLOAT", 4)
	case float64:
		return fixed("DOUBLE", 8)
	case time.Time:
		return fixed("TIMESTAMP", 8)
	default:
		return Column{Name: name, Type: "VARCHAR", Length: valueLength}
	}
}

// Addition 是一个需要新增的列。
type Addition struct {
	Point  string
	Column Column
}

// Rejection 是一个无法入库的点。
type Rejection struct {
	Point  string
	Reason string
}

// Schema 维护超级表当前的数据列以及“点名 → 列”的映射。
type Schema struct {
	valueLength int
	maxRowBytes int
	rowBytes    int
	columns     map[string]Column // 已有的数据列（不含 ts、标签）
	byPoint     map[string]string // 点名 -> 列名
	owner       map[string]string // 列名 -> 点名
	rejected    map[string]bool
}

// NewSchema 根据 DESCRIBE 结果创建 Schema。
func NewSchema(valueLength, maxRowBytes int, described []Column) *Schema {
	s := &Schema{
		valueLength: valueLength,
		maxRowBytes: maxRowBytes,
		columns:     map[string]Column{},
		byPoint:     map[string]string{},
		owner:       map[string]string{},
		rejected:    map[string]bool{},
	}
	for _, c := range described {
		if c.IsTag {
			continue
		}
		s.rowBytes += c.rowBytes()
		if c.Name != tsColumn {
			s.columns[c.Name] = c
		}
	}
	return s
}

// ColumnCount 返回数据列数（含 ts）。
func (s *Schema) ColumnCount() int { return len(s.columns) + 1 }

// Column 返回点对应的列。
func (s *Schema) Column(point string) (Column, bool) {
	name, ok := s.byPoint[point]
	return s.columns[name], ok
}

// Resolve 为尚未映射的点分配列：列已存在的直接映射（沿用已有列的类型）；需要新增的按值的类型建列，
// 作为 Addition 返回，调用方 ALTER 成功后调用 Added。values 为每个点的一个样本值。
// 列名冲突、超出列数或行长度上限的点作为 Rejection 返回，每个点只返回一次。
func (s *Schema) Resolve(values map[string]any) ([]Addition, []Rejection) {
	points := make([]string, 0, len(values))
	for point := range values {
		points = append(points, point)
	}
	sort.Strings(points)

	var (
		additions  []Addition
		rejections []Rejection
		planned    = map[string]string{} // 本批计划新增的列 -> 点名
		columns    = s.ColumnCount()
		rowBytes   = s.rowBytes
	)
	reject := func(point, reason string) {
		s.rejected[point] = true
		rejections = append(rejections, Rejection{Point: point, Reason: reason})
	}

	for _, point := range points {
		// 值为空时无法确定类型，等读到有值时再建列
		if _, ok := s.byPoint[point]; ok || s.rejected[point] || values[point] == nil {
			continue
		}
		name := ColumnName(point)
		other, taken := s.owner[name]
		if !taken {
			other, taken = planned[name]
		}
		col := columnFor(name, values[point], s.valueLength)
		_, exists := s.columns[name]
		switch {
		case name == "":
			reject(point, "点名无法转换为列名")
		case taken:
			reject(point, fmt.Sprintf("与点 %s 转换后的列名 %s 冲突", other, name))
		case exists:
			s.byPoint[point], s.owner[name] = name, point
		case columns+1 > maxColumns:
			reject(point, fmt.Sprintf("超过 TDengine 单表最大列数 %d", maxColumns))
		case rowBytes+col.rowBytes() > s.maxRowBytes:
			reject(point, fmt.Sprintf("超过单行最大长度 %d 字节", s.maxRowBytes))
		default:
			additions = append(additions, Addition{Point: point, Column: col})
			planned[name] = point
			columns++
			rowBytes += col.rowBytes()
		}
	}
	return additions, rejections
}

// Added 记录一个已成功新增的列。
func (s *Schema) Added(a Addition) {
	s.columns[a.Column.Name] = a.Column
	s.rowBytes += a.Column.rowBytes()
	s.byPoint[a.Point], s.owner[a.Column.Name] = a.Column.Name, a.Point
}
