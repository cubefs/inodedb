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

type DiskInfo struct {
	disk *diskNode
	node *node

	shardCount int32
	lock       sync.RWMutex
}

func (d *DiskInfo) getWeight() int32 {

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

func (d *DiskInfo) updateReport(report proto.DiskReport) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.shardCount = int32(report.ShardCnt)
	d.disk.Info = report
}

func (d *DiskInfo) GetDiskId() uint32 {
	return d.disk.GetDiskID()
}

func (d *DiskInfo) GetNodeId() uint32 {
	return d.disk.GetNodeID()
}

func (d *DiskInfo) GetRack() string {
	return d.node.info.Rack
}

func (d *DiskInfo) AddShardCnt(delta int) {
	atomic.AddInt32(&d.shardCount, int32(delta))
}

func (d *DiskInfo) CanAlloc() bool {
	return d.disk.Status == proto.DiskStatus_DiskStatusNormal
}

func (d *DiskInfo) GetShardCount() int32 {
	return d.shardCount
}

func (d *DiskInfo) Clone() *DiskInfo {
	return &DiskInfo{
		disk: &(*d.disk),
		node: d.node,
	}
}

type diskMgr struct {
	lock  sync.RWMutex
	disks map[uint32]*DiskInfo
}

func (dm *diskMgr) get(id uint32) *DiskInfo {
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disk, ok := dm.disks[id]
	if !ok {
		return nil
	}
	return disk
}

func (dm *diskMgr) addDisk(id uint32, ifo *DiskInfo) {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	dm.addDiskNoLock(id, ifo)
}

func (dm *diskMgr) addDiskNoLock(id uint32, ifo *DiskInfo) {
	if dm.disks == nil {
		dm.disks = make(map[uint32]*DiskInfo)
	}
	dm.disks[id] = ifo
	ifo.node.disks.addDiskNoLock(id, ifo)
}

type ById []*DiskInfo

func (a ById) Len() int      { return len(a) }
func (a ById) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ById) Less(i, j int) bool {
	return a[i].disk.DiskID < a[j].disk.DiskID
}

func (dm *diskMgr) getSortedDisks() []*DiskInfo {
	if dm == nil {
		return make([]*DiskInfo, 0)
	}
	
	dm.lock.RLock()
	defer dm.lock.RUnlock()
	disks := make([]*DiskInfo, 0, len(dm.disks))
	for _, disk := range dm.disks {
		disks = append(disks, disk)
	}

	// sort by diskId
	sort.Sort(ById(disks))
	return disks
}
