// Command coalesce-demo is a minimal walkthrough of the queue: plain
// Puts, a same-key merge, ordered Gets, and draining after Close.
package main

import (
	"context"
	"fmt"

	"coalescingqueue"
)

func main() {
	q := coalescingqueue.New()
	ctx := context.Background()

	fmt.Println("-- Put alpha=1, beta=2")
	must(q.Put("alpha", 1))
	must(q.Put("beta", 2))

	fmt.Println("-- Put alpha=3 again: value merged, position kept")
	must(q.Put("alpha", 3))
	snap := q.Snapshot()
	fmt.Printf("   Len=%d Items=%v Waiting=%d Closed=%v\n",
		q.Len(), snap.Items, snap.Waiting, snap.Closed)

	fmt.Println("-- Get in FIFO order")
	get(ctx, q) // alpha=3
	get(ctx, q) // beta=2

	fmt.Println("-- Put gamma=4, then Close")
	must(q.Put("gamma", 4))
	q.Close()
	fmt.Println("   Put after Close:", q.Put("delta", 5))

	fmt.Println("-- Items queued before Close still drain")
	get(ctx, q) // gamma=4
	fmt.Println("-- Queue drained:")
	if _, _, err := q.Get(ctx); err != nil {
		fmt.Println("   Get:", err)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func get(ctx context.Context, q *coalescingqueue.Queue) {
	k, v, err := q.Get(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Get -> %s=%v\n", k, v)
}
