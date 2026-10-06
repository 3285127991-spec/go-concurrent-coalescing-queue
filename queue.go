// Package coalescingqueue provides a concurrent coalescing FIFO queue.
//
// Each key has at most one pending item in the queue. Putting a key that
// is already pending replaces its value in place and keeps its original
// position; a key consumed by Get may be Put again and then enters at the
// tail as a new item.
package coalescingqueue

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrEmptyKey is returned by Put when the key is empty.
	ErrEmptyKey = errors.New("coalescingqueue: empty key")
	// ErrClosed is returned by Put after Close, and by Get once the queue
	// is closed and fully drained.
	ErrClosed = errors.New("coalescingqueue: queue closed")
)

// Item is a single pending entry: a key and its latest value.
type Item struct {
	Key   string
	Value any
}

// Snapshot is an independent copy of the queue state at a point in time.
type Snapshot struct {
	Items   []Item // pending items, head first
	Waiting int    // number of Get calls currently blocked
	Closed  bool
}

// Queue is a coalescing FIFO queue safe for concurrent use.
type Queue struct {
	mu      sync.Mutex
	items   *list.List // of Item, head at Front
	index   map[string]*list.Element
	notify  chan struct{} // closed to wake waiters, then replaced
	waiting int
	closed  bool
}

// New returns an empty, open Queue.
func New() *Queue {
	return &Queue{
		items:  list.New(),
		index:  make(map[string]*list.Element),
		notify: make(chan struct{}),
	}
}

// Put adds key with value to the tail of the queue. If key is already
// pending, its value is replaced and its position is kept. Put fails with
// ErrEmptyKey for an empty key and with ErrClosed after Close.
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
		el.Value = Item{Key: key, Value: value}
		return nil
	}
	q.index[key] = q.items.PushBack(Item{Key: key, Value: value})
	q.wakeLocked()
	return nil
}

// Get removes and returns the head item, blocking while the queue is
// empty. Canceling ctx aborts only this call: if a cancellation races
// with an arriving item, exactly one outcome occurs and no item is lost
// or delivered twice. Once the queue is closed and drained, Get returns
// ErrClosed.
func (q *Queue) Get(ctx context.Context) (string, any, error) {
	for {
		q.mu.Lock()
		if el := q.items.Front(); el != nil {
			it := el.Value.(Item)
			q.items.Remove(el)
			delete(q.index, it.Key)
			q.mu.Unlock()
			return it.Key, it.Value, nil
		}
		if q.closed {
			q.mu.Unlock()
			return "", nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return "", nil, err
		}
		ch := q.notify
		q.waiting++
		q.mu.Unlock()

		select {
		case <-ctx.Done():
		case <-ch:
		}

		q.mu.Lock()
		q.waiting--
		q.mu.Unlock()
	}
}

// Snapshot returns an independent copy of the current queue state:
// pending items in FIFO order, the number of blocked Get calls, and
// whether the queue is closed.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]Item, 0, q.items.Len())
	for el := q.items.Front(); el != nil; el = el.Next() {
		items = append(items, el.Value.(Item))
	}
	return Snapshot{Items: items, Waiting: q.waiting, Closed: q.closed}
}

// Len returns the number of pending (unique-key) items.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items.Len()
}

// Close rejects future Puts. Items queued before Close may still be
// drained by Get; once drained, blocked and future Gets return ErrClosed.
// Close is idempotent.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	q.wakeLocked()
}

// wakeLocked wakes all blocked Gets. The caller must hold q.mu.
func (q *Queue) wakeLocked() {
	close(q.notify)
	q.notify = make(chan struct{})
}
