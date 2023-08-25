package catalog

import (
	"container/list"
	"context"
	"sync"
	"time"

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
		tasks:          make(map[uint64]*task),
		taskList:       list.New(),
		taskElementMap: make(map[uint64]*list.Element),
		taskPool:       taskpool.New(taskPoolNum, taskPoolNum),
		taskFuncMap:    make(map[taskType]taskFunc),
		notifyC:        make(chan struct{}),
		done:           make(chan struct{}),
	}
}

type taskMgr struct {
	tasks          map[uint64]*task
	taskList       *list.List
	taskElementMap map[uint64]*list.Element
	taskPool       taskpool.TaskPool
	taskFuncMap    map[taskType]taskFunc

	notifyC chan struct{}
	done    chan struct{}
	lock    sync.RWMutex
}

func (t *taskMgr) Send(ctx context.Context, task *task) {
	t.lock.Lock()
	defer func() {
		t.lock.Unlock()
		t.notify()
	}()

	if t.tasks[task.sid] != nil {
		return
	}

	t.tasks[task.sid] = task
	e := t.taskList.PushBack(task)
	t.taskElementMap[task.sid] = e
}

func (t *taskMgr) Register(typ taskType, f taskFunc) {
	t.taskFuncMap[typ] = f
}

func (t *taskMgr) Start() {
	t.done = make(chan struct{})
	go t.run()
}

func (t *taskMgr) Close() {
	close(t.done)
}

func (t *taskMgr) run() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-t.notifyC:
			for {
				t.lock.Lock()
				e := t.taskList.Front()
				task := e.Value.(*task)
				if time.Since(task.assignedAt) < 0 {
					t.lock.Unlock()
					break
				}
				t.taskList.Remove(e)
				delete(t.taskElementMap, task.sid)
				t.lock.Unlock()

				if !t.execute(task) {
					break
				}
			}
		case <-ticker.C:
			t.notify()
		case <-t.done:
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
			t.Send(ctx, task)
			return
		}

		t.lock.Lock()
		delete(t.tasks, task.sid)
		t.lock.Unlock()

		span.Info("execute task[%+v] success")
	})
}

func (t *taskMgr) notify() {
	select {
	case t.notifyC <- struct{}{}:
	default:
	}
}
