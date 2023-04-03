package timingwheel

import (
	"sync"
	"sync/atomic"

	"github.com/ydmxcz/gds/collections/linkedlist"
)

type bucket struct {
	expiration int64
	mu         sync.Mutex
	events     *linkedlist.List[*Event]
}

func newBucket() *bucket {
	return &bucket{
		events:     linkedlist.New[*Event](),
		expiration: -1,
	}
}

func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

func (b *bucket) Add(t *Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.events.PushBack(t)
	t.setBucket(b)
	t.element = e

}

// 删除定时器
func (b *bucket) remove(t *Event) bool {
	if t.getBucket() != b {
		return false
	}
	b.events.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

func (b *bucket) Remove(t *Event) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

func (b *bucket) Flush(reinsert func(*Event)) {

	b.mu.Lock()

	for e := b.events.Front(); e != nil; {
		next := e.Next()

		t := e.Value
		b.remove(t)
		// Note that this operation will either execute the timer's task, or
		// insert the timer into another bucket belonging to a lower-level wheel.
		//
		// In either case, no further lock operation will happen to b.mu.
		reinsert(t)

		e = next
	}
	b.SetExpiration(-1) // TODO: Improve the coordination with b.Add()

	b.mu.Unlock()

}
