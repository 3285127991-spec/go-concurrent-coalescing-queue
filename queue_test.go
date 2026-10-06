package coalescingqueue_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"coalescingqueue"
)

type getResult struct {
	key   string
	value any
	err   error
}

func getAsync(q *coalescingqueue.Queue, ctx context.Context) <-chan getResult {
	ch := make(chan getResult, 1)
	go func() {
		k, v, err := q.Get(ctx)
		ch <- getResult{key: k, value: v, err: err}
	}()
	return ch
}

func recvResult(t *testing.T, ch <-chan getResult) getResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Get to return")
		return getResult{}
	}
}

func waitForWaiting(t *testing.T, q *coalescingqueue.Queue, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for q.Snapshot().Waiting != n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d blocked Get caller(s)", n)
		}
		runtime.Gosched()
	}
}

func mustPut(t *testing.T, q *coalescingqueue.Queue, key string, value any) {
	t.Helper()
	if err := q.Put(key, value); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func assertGet(t *testing.T, q *coalescingqueue.Queue, wantKey string, wantValue any) {
	t.Helper()
	r := recvResult(t, getAsync(q, context.Background()))
	if r.err != nil {
		t.Fatalf("Get: %v", r.err)
	}
	if r.key != wantKey || r.value != wantValue {
		t.Fatalf("Get = (%q, %v), want (%q, %v)", r.key, r.value, wantKey, wantValue)
	}
}

func TestFIFOOrder(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	if err := q.Put("", "x"); !errors.Is(err, coalescingqueue.ErrEmptyKey) {
		t.Fatalf("Put with empty key: got %v, want ErrEmptyKey", err)
	}

	for _, k := range []string{"a", "b", "c"} {
		mustPut(t, q, k, k+"-v")
	}
	if n := q.Len(); n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}
	for _, want := range []string{"a", "b", "c"} {
		assertGet(t, q, want, want+"-v")
	}
	if n := q.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

func TestPutSameKeyCoalescesInPlace(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)
	mustPut(t, q, "a", 3) // merge: latest value, original position

	if n := q.Len(); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	assertGet(t, q, "a", 3)
	assertGet(t, q, "b", 2)
}

func TestPutAfterGetRequeuesAtTail(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	mustPut(t, q, "a", 1)
	assertGet(t, q, "a", 1)

	mustPut(t, q, "b", 2)
	mustPut(t, q, "a", 3) // consumed before: queued again at the tail

	assertGet(t, q, "b", 2)
	assertGet(t, q, "a", 3)
}

func TestGetBlocksUntilPut(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	res := getAsync(q, context.Background())
	waitForWaiting(t, q, 1)

	mustPut(t, q, "k", "v")
	r := recvResult(t, res)
	if r.err != nil || r.key != "k" || r.value != "v" {
		t.Fatalf("Get = (%q, %v, %v), want (k, v, nil)", r.key, r.value, r.err)
	}
}

func TestConcurrentGetsNoDuplicates(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		mustPut(t, q, k, k+"-v")
	}

	var wg sync.WaitGroup
	got := make(chan string, len(keys))
	for range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			k, _, err := q.Get(context.Background())
			if err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			got <- k
		}()
	}
	wg.Wait()
	close(got)

	seen := make(map[string]int)
	for k := range got {
		seen[k]++
	}
	if len(seen) != len(keys) {
		t.Fatalf("got keys %v, want each of %v exactly once", seen, keys)
	}
	for _, k := range keys {
		if seen[k] != 1 {
			t.Fatalf("key %q delivered %d times, want 1", k, seen[k])
		}
	}
}

func TestGetContextCancel(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	res := getAsync(q, ctx)
	waitForWaiting(t, q, 1)
	cancel()

	if r := recvResult(t, res); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("Get err = %v, want context.Canceled", r.err)
	}

	// The cancelled Get must not have consumed anything: a later Put is
	// still delivered to a fresh Get.
	mustPut(t, q, "k", "v")
	assertGet(t, q, "k", "v")
}

func TestCloseDrain(t *testing.T) {
	// A Get blocked on an empty queue is released with ErrClosed by Close.
	q := coalescingqueue.New()
	res := getAsync(q, context.Background())
	waitForWaiting(t, q, 1)
	q.Close()
	q.Close() // idempotent: must not panic
	if r := recvResult(t, res); !errors.Is(r.err, coalescingqueue.ErrClosed) {
		t.Fatalf("Get err = %v, want ErrClosed", r.err)
	}

	// Entries pending before Close still drain in FIFO order.
	q2 := coalescingqueue.New()
	mustPut(t, q2, "a", 1)
	mustPut(t, q2, "b", 2)
	q2.Close()

	if err := q2.Put("c", 3); !errors.Is(err, coalescingqueue.ErrClosed) {
		t.Fatalf("Put after Close: got %v, want ErrClosed", err)
	}
	assertGet(t, q2, "a", 1)
	assertGet(t, q2, "b", 2)
	if r := recvResult(t, getAsync(q2, context.Background())); !errors.Is(r.err, coalescingqueue.ErrClosed) {
		t.Fatalf("Get on drained closed queue: err = %v, want ErrClosed", r.err)
	}
}

func TestSnapshot(t *testing.T) {
	q := coalescingqueue.New()

	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)

	snap := q.Snapshot()
	if len(snap.Items) != 2 || snap.Items[0] != (coalescingqueue.Item{Key: "a", Value: 1}) ||
		snap.Items[1] != (coalescingqueue.Item{Key: "b", Value: 2}) {
		t.Fatalf("Snapshot.Items = %+v, want [{a 1} {b 2}]", snap.Items)
	}
	if snap.Waiting != 0 || snap.Closed {
		t.Fatalf("Snapshot = %+v, want Waiting=0 Closed=false", snap)
	}

	// The snapshot is independent: mutating it must not affect the queue.
	snap.Items[0].Value = "mutated"
	if again := q.Snapshot(); again.Items[0].Value != 1 {
		t.Fatalf("snapshot mutation leaked into queue: %+v", again.Items[0])
	}

	assertGet(t, q, "a", 1)
	assertGet(t, q, "b", 2)

	res := getAsync(q, context.Background())
	waitForWaiting(t, q, 1)
	if snap = q.Snapshot(); snap.Waiting != 1 || len(snap.Items) != 0 {
		t.Fatalf("Snapshot = %+v, want Waiting=1 with no items", snap)
	}

	q.Close()
	if r := recvResult(t, res); !errors.Is(r.err, coalescingqueue.ErrClosed) {
		t.Fatalf("Get err = %v, want ErrClosed", r.err)
	}
	if snap = q.Snapshot(); !snap.Closed || snap.Waiting != 0 {
		t.Fatalf("Snapshot = %+v, want Closed=true Waiting=0", snap)
	}
}
