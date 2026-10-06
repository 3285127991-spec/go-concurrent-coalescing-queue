package coalescingqueue_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"coalescingqueue"
)

// waitFor spins until cond holds, using a deadline only as a failure
// guard, never as a synchronization sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
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

func mustGet(t *testing.T, q *coalescingqueue.Queue) (string, any) {
	t.Helper()
	k, v, err := q.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return k, v
}

func TestFIFOOrder(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	if err := q.Put("", 1); !errors.Is(err, coalescingqueue.ErrEmptyKey) {
		t.Fatalf("Put empty key: got %v, want ErrEmptyKey", err)
	}
	if n := q.Len(); n != 0 {
		t.Fatalf("Len after rejected Put = %d, want 0", n)
	}

	for i, k := range []string{"a", "b", "c"} {
		mustPut(t, q, k, i+1)
	}
	if n := q.Len(); n != 3 {
		t.Fatalf("Len = %d, want 3", n)
	}
	for i, want := range []string{"a", "b", "c"} {
		k, v := mustGet(t, q)
		if k != want || v != i+1 {
			t.Fatalf("Get #%d = (%q, %v), want (%q, %d)", i, k, v, want, i+1)
		}
	}
	if n := q.Len(); n != 0 {
		t.Fatalf("Len after drain = %d, want 0", n)
	}
}

func TestPutSameKeyMergesAndKeepsPosition(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)
	mustPut(t, q, "c", 3)
	mustPut(t, q, "a", 10) // merge: value replaced, position kept
	mustPut(t, q, "c", 30) // merge

	if n := q.Len(); n != 3 {
		t.Fatalf("Len = %d, want 3 (merge must not add entries)", n)
	}
	want := []coalescingqueue.Item{
		{Key: "a", Value: 10},
		{Key: "b", Value: 2},
		{Key: "c", Value: 30},
	}
	for i, w := range want {
		k, v := mustGet(t, q)
		if k != w.Key || v != w.Value {
			t.Fatalf("Get #%d = (%q, %v), want (%q, %v)", i, k, v, w.Key, w.Value)
		}
	}
}

func TestRequeueAfterConsume(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)
	if k, _ := mustGet(t, q); k != "a" {
		t.Fatalf("Get = %q, want %q", k, "a")
	}
	mustPut(t, q, "a", 9) // consumed key re-enters at the tail: queue is [b, a]

	if n := q.Len(); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	if k, v := mustGet(t, q); k != "b" || v != 2 {
		t.Fatalf("Get = (%q, %v), want (b, 2)", k, v)
	}
	if k, v := mustGet(t, q); k != "a" || v != 9 {
		t.Fatalf("Get = (%q, %v), want (a, 9)", k, v)
	}
}

func TestGetBlocksUntilPut(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	type result struct {
		key   string
		value any
		err   error
	}
	done := make(chan result, 1)
	go func() {
		k, v, err := q.Get(context.Background())
		done <- result{k, v, err}
	}()

	waitFor(t, "getter to block", func() bool { return q.Snapshot().Waiting == 1 })
	select {
	case r := <-done:
		t.Fatalf("Get returned before any Put: %+v", r)
	default:
	}

	mustPut(t, q, "k", "v")
	select {
	case r := <-done:
		if r.err != nil || r.key != "k" || r.value != "v" {
			t.Fatalf("Get = (%q, %v, %v), want (k, v, nil)", r.key, r.value, r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get still blocked after Put")
	}
}

func TestConcurrentGetsNoDuplicates(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	const n = 4
	for i := 0; i < n; i++ {
		mustPut(t, q, fmt.Sprintf("k%d", i), i)
	}

	var wg sync.WaitGroup
	got := make(chan string, n)
	for i := 0; i < n; i++ {
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

	seen := make(map[string]bool, n)
	for k := range got {
		if seen[k] {
			t.Fatalf("key %q delivered more than once", k)
		}
		seen[k] = true
	}
	if len(seen) != n {
		t.Fatalf("delivered %d distinct keys, want %d", len(seen), n)
	}
}

func TestContextCancel(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	// Already-canceled context on an empty queue fails fast.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := q.Get(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with canceled context: got %v, want context.Canceled", err)
	}

	// A blocked Get unblocks with the context error on cancel.
	ctx2, cancel2 := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := q.Get(ctx2)
		done <- err
	}()
	waitFor(t, "getter to block", func() bool { return q.Snapshot().Waiting == 1 })
	cancel2()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked Get after cancel: got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Get did not return after cancel")
	}

	// The canceled Get consumed nothing: a later Put is delivered exactly
	// once to a new Get.
	mustPut(t, q, "k", "v")
	if n := q.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}
	if k, v := mustGet(t, q); k != "k" || v != "v" {
		t.Fatalf("Get = (%q, %v), want (k, v)", k, v)
	}
}

func TestCloseDrain(t *testing.T) {
	q := coalescingqueue.New()
	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)
	q.Close()

	if err := q.Put("c", 3); !errors.Is(err, coalescingqueue.ErrClosed) {
		t.Fatalf("Put after Close: got %v, want ErrClosed", err)
	}

	// Items queued before Close still drain in FIFO order.
	for _, want := range []string{"a", "b"} {
		if k, _ := mustGet(t, q); k != want {
			t.Fatalf("drain Get = %q, want %q", k, want)
		}
	}
	// Queue exhausted: Gets now report closed.
	if _, _, err := q.Get(context.Background()); !errors.Is(err, coalescingqueue.ErrClosed) {
		t.Fatalf("Get after drain: got %v, want ErrClosed", err)
	}

	// A Get blocked on an empty queue is released by Close.
	q2 := coalescingqueue.New()
	done := make(chan error, 1)
	go func() {
		_, _, err := q2.Get(context.Background())
		done <- err
	}()
	waitFor(t, "getter to block", func() bool { return q2.Snapshot().Waiting == 1 })
	q2.Close()
	select {
	case err := <-done:
		if !errors.Is(err, coalescingqueue.ErrClosed) {
			t.Fatalf("blocked Get after Close: got %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Get not released by Close")
	}
}

func TestSnapshot(t *testing.T) {
	q := coalescingqueue.New()
	defer q.Close()

	mustPut(t, q, "a", 1)
	mustPut(t, q, "b", 2)
	mustPut(t, q, "a", 3) // merge: value updated, position kept

	snap := q.Snapshot()
	if snap.Closed {
		t.Fatal("Snapshot.Closed = true on open queue")
	}
	if snap.Waiting != 0 {
		t.Fatalf("Snapshot.Waiting = %d, want 0", snap.Waiting)
	}
	want := []coalescingqueue.Item{
		{Key: "a", Value: 3},
		{Key: "b", Value: 2},
	}
	if !reflect.DeepEqual(snap.Items, want) {
		t.Fatalf("Snapshot.Items = %v, want %v", snap.Items, want)
	}

	// The snapshot is independent: mutating it must not affect the queue.
	snap.Items[0].Value = 99
	if again := q.Snapshot(); again.Items[0].Value != 3 {
		t.Fatalf("snapshot shares state with queue: value = %v", again.Items[0].Value)
	}

	// Waiting reflects blocked Get calls; Closed reflects Close.
	q2 := coalescingqueue.New()
	done := make(chan error, 1)
	go func() {
		_, _, err := q2.Get(context.Background())
		done <- err
	}()
	waitFor(t, "getter to block", func() bool { return q2.Snapshot().Waiting == 1 })
	if w := q2.Snapshot().Waiting; w != 1 {
		t.Fatalf("Snapshot.Waiting = %d, want 1", w)
	}
	q2.Close()
	if !q2.Snapshot().Closed {
		t.Fatal("Snapshot.Closed = false after Close")
	}
	if err := <-done; !errors.Is(err, coalescingqueue.ErrClosed) {
		t.Fatalf("blocked Get after Close: got %v, want ErrClosed", err)
	}
}
