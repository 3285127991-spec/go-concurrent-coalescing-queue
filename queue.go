// Package coalescingqueue provides a concurrency-safe FIFO queue that
// coalesces duplicate keys: a key has at most one pending entry, and
// re-putting a pending key only replaces its value in place.
package coalescingqueue

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed is returned by Put and Get after the queue has been closed
	// and, for Get, fully drained.
	ErrClosed = errors.New("coalescingqueue: queue is closed")
	// ErrEmptyKey is returned by Put when the key is an empty string.
	ErrEmptyKey = errors.New("coalescingqueue: key must be a non-empty string")
)

// Item is a single pending entry: a key and its latest value.
type Item struct {
	Key   string
	Value any
}

// Snapshot is an ordered, independent copy of the queue's pending state.
type Snapshot struct {
	Items   []Item // pending entries in FIFO order
	Waiting int    // number of Get callers currently blocked
	Closed  bool   // whether Close has been called
}

type entry struct {
	key   string
	value any
}

// Queue is a concurrency-safe coalescing FIFO queue.
// The zero value is not usable; create one with New.
type Queue struct {
	mu      sync.Mutex
	items   list.List                // FIFO order of *entry
	index   map[string]*list.Element // key -> position in items
	changed chan struct{}            // closed to broadcast a state change
	waiting int
	closed  bool
}

// New returns an empty, open queue.
func New() *Queue {
	return &Queue{
		index:   make(map[string]*list.Element),
		changed: make(chan struct{}),
	}
}

// Put adds key with value to the tail of the queue. If key is already
// pending, only its value is replaced and its position is kept. If key was
// previously consumed, it is queued again at the current tail.
// It returns ErrEmptyKey for an empty key and ErrClosed after Close.
func (q *Queue) Put(key string, value any) error {
	if key == "" {
		return ErrEmptyKey
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if el, ok := q.index[key]; ok {
		el.Value.(*entry).value = value
		return nil
	}
	q.index[key] = q.items.PushBack(&entry{key: key, value: value})
	q.broadcastLocked()
	return nil
}

// Get removes and returns the head entry's key and latest value. It blocks
// while the queue is empty and open. Cancelling ctx aborts only this call;
// if cancellation races with an arriving entry, exactly one of the two
// outcomes happens and no entry is lost or delivered twice. Once the queue
// is closed and drained, Get returns ErrClosed.
func (q *Queue) Get(ctx context.Context) (key string, value any, err error) {
	for {
		q.mu.Lock()
		if q.items.Len() > 0 {
			el := q.items.Front()
			e := el.Value.(*entry)
			q.items.Remove(el)
			delete(q.index, e.key)
			q.mu.Unlock()
			return e.key, e.value, nil
		}
		if q.closed {
			q.mu.Unlock()
			return "", nil, ErrClosed
		}
		changed := q.changed
		q.waiting++
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.waiting--
			q.mu.Unlock()
			return "", nil, ctx.Err()
		case <-changed:
			q.mu.Lock()
			q.waiting--
			q.mu.Unlock()
		}
	}
}

// Len reports the number of pending unique keys.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.index)
}

// Snapshot returns an ordered, independent copy of the pending entries
// along with the number of blocked Get callers and the closed flag.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	snap := Snapshot{
		Items:   make([]Item, 0, len(q.index)),
		Waiting: q.waiting,
		Closed:  q.closed,
	}
	for el := q.items.Front(); el != nil; el = el.Next() {
		e := el.Value.(*entry)
		snap.Items = append(snap.Items, Item{Key: e.key, Value: e.value})
	}
	return snap
}

// Close marks the queue closed. Pending entries may still be drained by
// Get; new Put calls are rejected. It is safe to call Close more than once.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.broadcastLocked()
}

// broadcastLocked wakes every blocked Get so it can re-check the state.
// q.mu must be held.
func (q *Queue) broadcastLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
