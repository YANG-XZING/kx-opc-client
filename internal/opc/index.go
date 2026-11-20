package opc

import (
	"encoding/json"

	"github.com/gopcua/opcua/ua"

	"kx-opc-client/internal/model"
)

// varNodeIDs 是服务端的变量索引节点：值为全部变量 NodeId 的 JSON 字符串数组，
// 例如 ["ns=2;s=HEART_DATETIME","ns=2;s=Temp"]（见 CustomNodeManagerAbs.cs）。
const varNodeIDs = "VarNodeIds"

// ParseIndex 解析 VarNodeIds 的值，返回去重后的点位。只接受自定义命名空间（非 0）下字符串标识的 NodeId，
// 点名即标识本身。内容非法时返回空。
func ParseIndex(raw string) []model.Point {
	var texts []string
	if err := json.Unmarshal([]byte(raw), &texts); err != nil {
		return nil
	}
	points := make([]model.Point, 0, len(texts))
	seen := make(map[string]bool, len(texts))
	for _, text := range texts {
		nodeID, err := ua.ParseNodeID(text)
		if err != nil || nodeID.Type() != ua.NodeIDTypeString || nodeID.Namespace() == 0 {
			continue
		}
		name := nodeID.StringID()
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		points = append(points, model.Point{Name: name, NodeID: nodeID.String()})
	}
	return points
}
