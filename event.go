package timingwheel

import (
	"sync/atomic"
	"unsafe"

	"github.com/ydmxcz/gds/collections/linkedlist"
)

type Event struct {
	expiration int64
	task       func()
	b          unsafe.Pointer
	element    *linkedlist.Element[*Event]
}

func (t *Event) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Event) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

func (t *Event) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		//从bucket（时间格）中移除定时器
		stopped = b.Remove(t)
	}
	return stopped
}
