package timingwheel

import "testing"

func TestBucket_Flush(t *testing.T) {
	b := newBucket()

	b.Add(&Event{})
	b.Add(&Event{})
	l1 := b.events.Len()
	if l1 != 2 {
		t.Fatalf("Got (%+v) != Want (%+v)", l1, 2)
	}

	b.Flush(func(*Event) {})
	l2 := b.events.Len()
	if l2 != 0 {
		t.Fatalf("Got (%+v) != Want (%+v)", l2, 0)
	}
}
