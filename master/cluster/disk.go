package cluster

import (
	"encoding/json"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cubefs/inodedb/proto"
)

const (
	defaultFactor          = 10000
	defaultMaxShardOneDisk = 20000
)

type diskInfo struct {
	DiskID     proto.DiskID     `json:"disk_id"`
	NodeID     proto.NodeID     `json:"node_id"`
	Path       string           `json:"path"`
	Status     proto.DiskStatus `json:"status"`
	DropStatus proto.DropStatus `json:"drop_status"`
	Readonly   bool             `json:"readonly"`
	CreateAt   uint64           `json:"create_at"`
	LastUpdate uint64           `json:"last_update"`
	Used       uint64           `json:"used"`
	Total      uint64           `json:"total"`
	ShardCnt   uint64           `json:"shardCnt"`
}

func (d *diskInfo) encode() ([]byte, error) {
	return json.Marshal(d)
}

func (d *diskInfo) decode(data []byte) error {
	return json.Unmarshal(data, d)
}

func (d *diskInfo) toProtoDisk() *proto.Disk {
	return &proto.Disk{
		DiskID:     d.DiskID,
		NodeID:     d.NodeID,
		Path:       d.Path,
		Status:     d.Status,
		Readonly:   d.Readonly,
		CreateAt:   d.CreateAt,
		LastUpdate: d.LastUpdate,
		DropStatus: d.DropStatus,
		Info: proto.DiskReport{
			ShardCnt: d.ShardCnt,
			Used:     d.Used,
			Total:    d.Total,
		},
	}
}

type disk struct {
	DiskID     proto.DiskID
	info       *diskInfo
	node       *nodeInfo
	shardCount int32
}

func newDisk(info *diskInfo, node *nodeInfo, cnt int32) *disk {
	return &disk{
		DiskID:     info.DiskID,
		info:       info,
		node:       node,
		shardCount: cnt,
	}
}

func (d *disk) GetInfo() *diskInfo {
	return &(*d.info)
}

func (d *disk) GetNode() *nodeInfo {
	return &(*d.node)
}

func (d *disk) GetDiskID() proto.DiskID {
	return d.info.DiskID
}

func (d *disk) AddShardCnt(delta int) {
	atomic.AddInt32(&d.shardCount, int32(delta))
}

func (d *disk) CanAlloc() bool {
	return d.info.Status == proto.DiskStatus_DiskStatusNormal
}

func (d *disk) GetShardCount() int32 {
	return d.shardCount
}

func (d *disk) getWeight() int32 {
	maxCnt := defaultMaxShardOneDisk
	var w1, w2 int
	if d.shardCount < defaultMaxShardOneDisk {
		w1 = (maxCnt - int(d.shardCount)) * defaultFactor / maxCnt
	}

	info := d.info
	if info.Used < info.Total {
		w2 = int(info.Total-info.Used) * defaultFactor / int(info.Total)
	}

	if w1+w2 == 0 {
		return 1
	}

	return int32(w1 + w2)
}

func (d *disk) updateReport(report proto.DiskReport) {
	d.info.ShardCnt = report.ShardCnt
	d.info.Used = report.Used
	d.info.Total = report.Total
}

type diskMgr struct {
	lock  sync.RWMutex
	disks map[proto.DiskID]*disk
}

func (dm *diskMgr) get(id uint32) *disk {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disk, ok := dm.disks[id]
	if !ok {
		return nil
	}
	return disk
}

func (dm *diskMgr) addDisk(id uint32, d *disk) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	dm.addDiskNoLock(id, d)
}

func (dm *diskMgr) addDiskNoLock(id uint32, d *disk) {
	dm.disks[id] = d
}

type SortedDisks []*disk

func (a SortedDisks) Len() int      { return len(a) }
func (a SortedDisks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortedDisks) Less(i, j int) bool {
	return a[i].DiskID < a[j].DiskID
}

func (dm *diskMgr) getSortedDisks() []*disk {
	if dm == nil {
		return make([]*disk, 0)
	}

	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disks := make([]*disk, 0, len(dm.disks))
	for _, disk := range dm.disks {
		disks = append(disks, disk)
	}

	// sort by diskID
	sort.Sort(SortedDisks(disks))
	return disks
}

func protoDiskToInternalDisk(d *proto.Disk) *diskInfo {
	d1 := &diskInfo{
		DiskID:     d.DiskID,
		NodeID:     d.NodeID,
		Path:       d.Path,
		Status:     d.Status,
		DropStatus: d.DropStatus,
		Readonly:   d.Readonly,
		CreateAt:   d.CreateAt,
		LastUpdate: d.LastUpdate,
		Used:       d.Info.Used,
		Total:      d.Info.Total,
		ShardCnt:   d.Info.ShardCnt,
	}
	return d1
}
