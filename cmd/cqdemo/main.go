// Command cqdemo demonstrates the coalescingqueue package: plain Put,
// same-key merge updates, ordered Get, and draining after Close.
package main

import (
	"context"
	"fmt"
	"os"

	"coalescingqueue"
)

func main() {
	q := coalescingqueue.New()
	ctx := context.Background()

	fmt.Println("== Put: alpha=1, beta=2, gamma=3 ==")
	must(q.Put("alpha", 1))
	must(q.Put("beta", 2))
	must(q.Put("gamma", 3))
	printState(q)

	fmt.Println("== Put same key: alpha=99 (merged in place, position kept) ==")
	must(q.Put("alpha", 99))
	printState(q)

	fmt.Println("== Get in FIFO order ==")
	for q.Len() > 0 {
		k, v, err := q.Get(ctx)
		must(err)
		fmt.Printf("Get -> %s=%v\n", k, v)
	}

	fmt.Println("== Close: delta drains, then the queue is exhausted ==")
	must(q.Put("delta", 4))
	q.Close()
	if err := q.Put("omega", 5); err != nil {
		fmt.Println("Put after Close rejected:", err)
	}
	for {
		k, v, err := q.Get(ctx)
		if err != nil {
			fmt.Println("Get after drain:", err)
			break
		}
		fmt.Printf("Get -> %s=%v\n", k, v)
	}
}

func printState(q *coalescingqueue.Queue) {
	snap := q.Snapshot()
	fmt.Printf("  Len=%d Waiting=%d Closed=%v\n", q.Len(), snap.Waiting, snap.Closed)
	for _, it := range snap.Items {
		fmt.Printf("  - %s=%v\n", it.Key, it.Value)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
