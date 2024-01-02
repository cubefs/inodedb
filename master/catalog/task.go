package catalog

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

const (
	taskTypeCreateSpace = taskType(iota + 1)
	taskTypeExpandSpace
)

type (
	taskType uint8
	taskFunc func(ctx context.Context, sid uint64, args []byte) error

	task struct {
		sid        uint64
		typ        taskType
		args       []byte
		assignedAt time.Time
	}
)

func newTaskMgr(taskPoolNum int) *taskMgr {
	return &taskMgr{
		taskList:       list.New(),
		taskElementMap: make(map[uint64]*list.Element),
		taskPool:       taskpool.New(taskPoolNum, taskPoolNum),
		taskFuncMap:    make(map[taskType]taskFunc),
		notifyC:        make(chan struct{}, 1),
		done:           make(chan struct{}),
	}
}

type taskMgr struct {
	taskList       *list.List
	taskElementMap map[uint64]*list.Element
	taskPool       taskpool.TaskPool
	taskFuncMap    map[taskType]taskFunc

	notifyC chan struct{}
	done    chan struct{}
	lock    sync.RWMutex
}

func (t *taskMgr) Send(ctx context.Context, task *task) {
	span := trace.SpanFromContext(ctx)

	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		t.notify()
	}()

	if t.taskElementMap[task.sid] != nil {
		return
	}

	e := t.taskList.PushBack(task)
	t.taskElementMap[task.sid] = e
	span.Infof("send task[%+v] success", task)
}

func (t *taskMgr) Register(typ taskType, f taskFunc) {
	t.taskFuncMap[typ] = f
}

func (t *taskMgr) Start() {
	go t.run()
}

func (t *taskMgr) Close() {
	close(t.done)
}

func (t *taskMgr) run() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Info("start run task manager")

	for {
		select {
		case <-t.notifyC:
			for {
				ok := func() bool {
					t.lock.Lock()
					defer t.lock.Unlock()

					if t.taskList.Len() == 0 {
						return false
					}

					e := t.taskList.Front()
					task := e.Value.(*task)
					log.Infof("execute task %+v", task)
					if time.Since(task.assignedAt) < 0 {
						log.Info(task, 1)
						return false
					}

					log.Info(task, 2)
					if !t.execute(task) {
						return false
					}

					t.taskList.Remove(e)
					return true
				}()

				if !ok {
					break
				}
			}
		case <-ticker.C:
			t.notify()
		case <-t.done:
			log.Info("done")
			return
		}
	}
}

func (t *taskMgr) execute(task *task) bool {
	return t.taskPool.TryRun(func() {
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		if err := t.taskFuncMap[task.typ](ctx, task.sid, task.args); err != nil {
			span.Warnf("execute task[%+v] failed: %s", task, err)
			// resend
			t.lock.Lock()
			span.Info("get lock")
			e := t.taskList.PushBack(task)
			t.taskElementMap[task.sid] = e
			t.lock.Unlock()
			return
		}

		t.lock.Lock()
		delete(t.taskElementMap, task.sid)
		t.lock.Unlock()

		span.Infof("execute task[%+v] success", task)
	})
}

func (t *taskMgr) notify() {
	select {
	case t.notifyC <- struct{}{}:
	default:
	}
}
