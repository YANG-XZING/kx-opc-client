package collector

import (
	"sort"
	"sync"

	"kx-opc-client/internal/model"
)

// Registry 登记各炉台的采集状态，用于汇总日志。
type Registry struct {
	mu      sync.Mutex
	entries map[string]*entry // 子表名 -> 炉台
}

type entry struct {
	furnace model.Furnace
	state   model.State
}

// NewRegistry 创建空的登记表。
func NewRegistry() *Registry {
	return &Registry{entries: map[string]*entry{}}
}

// Add 登记一台炉台（初始为 Disconnected），已登记过的返回 false。
func (r *Registry) Add(f model.Furnace) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[f.Table]; ok {
		return false
	}
	r.entries[f.Table] = &entry{furnace: f}
	return true
}

// Has 返回炉台是否已登记（已在采集）。
func (r *Registry) Has(table string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.entries[table]
	return ok
}

// SetDeviceID 记录连上服务端后确定的炉台号。
func (r *Registry) SetDeviceID(table, deviceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[table]; ok {
		e.furnace.DeviceID = deviceID
	}
}

// Set 更新一台炉台的状态。
func (r *Registry) Set(table string, state model.State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[table]; ok {
		e.state = state
	}
}

// ByState 按状态分组返回炉台号（组内排序；炉台号确定前用 IP）。
func (r *Registry) ByState() map[model.State][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	groups := map[model.State][]string{}
	for _, e := range r.entries {
		groups[e.state] = append(groups[e.state], e.furnace.Name())
	}
	for _, ids := range groups {
		sort.Strings(ids)
	}
	return groups
}
