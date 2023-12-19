package cluster

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cubefs/inodedb/proto"
)

type diskNode = proto.Disk

const (
	defaultFactor          = 10000
	defaultMaxShardOneDisk = 20000
)

type diskInfo struct {
	disk *diskNode
	node *node

	shardCount int32
	lock       sync.RWMutex
}

func (d *diskInfo) getWeight() int32 {

	maxCnt := defaultMaxShardOneDisk
	var w1, w2 int
	if d.shardCount < defaultMaxShardOneDisk {
		w1 = (maxCnt - int(d.shardCount)) * defaultFactor / maxCnt
	}

	ifo := d.disk.Info
	if ifo.Used < ifo.Total {
		w2 = int(ifo.Total-ifo.Used) * defaultFactor / int(ifo.Total)
	}

	if w1+w2 == 0 {
		return 1
	}

	return int32(w1 + w2)
}

func (d *diskInfo) updateReport(report proto.DiskReport) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.shardCount = int32(report.ShardCnt)
	d.disk.Info = report
}

func (d *diskInfo) GetDiskId() uint32 {
	return d.disk.GetDiskID()
}

func (d *diskInfo) GetNodeId() uint32 {
	return d.disk.GetNodeID()
}

func (d *diskInfo) GetRack() string {
	return d.node.info.Rack
}

func (d *diskInfo) AddShardCnt(delta int) {
	atomic.AddInt32(&d.shardCount, int32(delta))
}

func (d *diskInfo) CanAlloc() bool {
	return d.disk.Status == proto.DiskStatus_DiskStatusNormal
}

func (d *diskInfo) GetShardCount() int32 {
	return d.shardCount
}

func (d *diskInfo) Clone() *diskInfo {
	return &diskInfo{
		disk: &(*d.disk),
		node: d.node,
	}
}

type diskMgr struct {
	lock  sync.RWMutex
	disks map[uint32]*diskInfo
}

func (dm *diskMgr) get(id uint32) *diskInfo {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disk, ok := dm.disks[id]
	if !ok {
		return nil
	}
	return disk
}

func (dm *diskMgr) addDisk(id uint32, ifo *diskInfo) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	dm.addDiskNoLock(id, ifo)
}

func (dm *diskMgr) addDiskNoLock(id uint32, ifo *diskInfo) {
	if dm.disks == nil {
		dm.disks = make(map[uint32]*diskInfo)
	}
	dm.disks[id] = ifo
	ifo.node.dm.disks[id] = ifo
}

type Disks []*diskInfo

func (a Disks) Len() int      { return len(a) }
func (a Disks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a Disks) Less(i, j int) bool {
	return a[i].disk.DiskID < a[j].disk.DiskID
}

func (dm *diskMgr) getSortedDisks() []*diskInfo {
	if dm == nil {
		return make([]*diskInfo, 0)
	}

	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disks := make([]*diskInfo, 0, len(dm.disks))
	for _, disk := range dm.disks {
		disks = append(disks, disk)
	}

	// sort by diskId
	sort.Sort(Disks(disks))
	return disks
}
