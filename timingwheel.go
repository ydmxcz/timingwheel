package timingwheel

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ydmxcz/gds/collections/delayqueue"
)

// 分层时间轮
type TimingWheel struct {
	tick      int64 // 每一个时间格的跨度,以毫秒为单位
	wheelSize int64 // 时间格的数量

	interval    int64                           // 总的跨度数 tick * wheelSize，以毫秒为单位
	currentTime int64                           // 当前指针指向的时间，以毫秒为单位
	buckets     []*bucket                       //时间格列表
	queue       *delayqueue.DelayQueue[*bucket] //延迟队列,

	overflowWheel unsafe.Pointer // 上一层时间轮的指针

	ctx context.Context
	cf  context.CancelFunc
}

// 对外暴露的初始化时间轮方法,参数为时间格跨度，和时间格数量
func New(tick time.Duration, wheelSize int64) *TimingWheel {
	//时间格(毫秒)
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}

	//开始时间
	startMs := time.Now().UnixMilli()

	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		delayqueue.NewDelayQueue(func(a, b *bucket) int {
			if a == b {
				return 0
			}
			return 1
		}, 8), //delayqueue
	)
}

func truncate(expiration, tick int64) int64 {
	if tick < 0 {
		return expiration
	}
	return expiration - (expiration % tick)
}

func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *delayqueue.DelayQueue[*bucket]) *TimingWheel {
	//根据时间格数量创建时间格列表
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}
	ctx, cf := context.WithCancel(context.Background())
	return &TimingWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs),
		interval:    tickMs * wheelSize,
		buckets:     buckets,
		queue:       queue,
		ctx:         ctx,
		cf:          cf,
	}
}

func (tw *TimingWheel) Start() {
	go func() {
		for {
			b, ok := tw.queue.PopWithCtx(tw.ctx)
			if !ok {
				fmt.Println("down")
				return
			}
			tw.advanceClock(b.Expiration())
			b.Flush(tw.addOrRun)

		}
	}()
}

func (tw *TimingWheel) add(t *Event) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		return false
	} else if t.expiration < currentTime+tw.interval {

		virtualID := t.expiration / tw.tick

		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		if b.SetExpiration(virtualID * tw.tick) {
			tw.queue.Push(b)
		}

		return true
	} else {
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).add(t)
	}
}

func (tw *TimingWheel) addOrRun(t *Event) {
	if !tw.add(t) {
		go t.task()
	}
}

func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)

	if expiration >= currentTime+tw.tick {

		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// 如果有上层时间轮，那么递归调用上层时间轮的引用
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

func (tw *TimingWheel) Stop() {
	tw.cf()
}

// 添加定时任务到时间轮
func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Event {

	t := &Event{
		expiration: time.Now().Add(d).UnixMilli(), //timeToMs(time.Now().UTC().Add(d)),
		task:       f,
	}
	tw.addOrRun(t)
	return t
}

// Scheduler determines the execution plan of a task.
type Scheduler interface {
	// Next returns the next execution time after the given (previous) time.
	// It will return a zero time if no next time is scheduled.
	//
	// All times must be UTC.
	Next(time.Time) time.Time
}

// ScheduleFunc calls f (in its own goroutine) according to the execution
// plan scheduled by s. It returns a Timer that can be used to cancel the
// call using its Stop method.
//
// If the caller want to terminate the execution plan halfway, it must
// stop the timer and ensure that the timer is stopped actually, since in
// the current implementation, there is a gap between the expiring and the
// restarting of the timer. The wait time for ensuring is short since the
// gap is very small.
//
// Internally, ScheduleFunc will ask the first execution time (by calling
// s.Next()) initially, and create a timer if the execution time is non-zero.
// Afterwards, it will ask the next execution time each time f is about to
// be executed, and f will be called at the next execution time if the time
// is non-zero.
func (tw *TimingWheel) ScheduleFunc(s Scheduler, f func()) (t *Event) {
	expiration := s.Next(time.Now().UTC())
	if expiration.IsZero() {
		// No time is scheduled, return nil.
		return
	}

	t = &Event{
		expiration: expiration.UnixMilli(),
		task: func() {
			// Schedule the task to execute at the next time if possible.
			expiration := s.Next(msToTime(t.expiration))
			if !expiration.IsZero() {
				t.expiration = expiration.UnixMilli()
				tw.addOrRun(t)
			}

			// Actually execute the task.
			f()
		},
	}
	tw.addOrRun(t)

	return
}

// msToTime returns the UTC time corresponding to the given Unix time,
// t milliseconds since January 1, 1970 UTC.
func msToTime(t int64) time.Time {
	return time.Unix(0, t*int64(time.Millisecond)).UTC()
}
