# coalescingqueue

Go 实现的并发 Coalescing Queue（合并队列），零第三方依赖，Windows 本地可运行。
仅包含库、极简 CLI 演示和自动化测试。

## 语义

- `Put(key, value)`：key 必须为非空字符串，否则返回 `ErrEmptyKey`。同一 key 在未被消费前最多保留一个待处理项；重复 Put 仅原地替换 value，队列位置不变，不新增条目。
- `Get(ctx)`：按 FIFO（首次进入顺序）原子取出队首的 key 和最新 value；多个并发 Get 不会重复取得同一项。队列为空时阻塞等待；ctx 取消只影响本次调用，取消与元素到达并发时只会有一种结果，不重复交付、不丢元素。
- key 被 Get 消费后，再次 Put 会作为新待处理项排到当前队尾。
- `Close()`：拒绝新的 Put（返回 `ErrClosed`）；已入队条目仍可被 Get 按 FIFO 排空；耗尽后阻塞中的和新的 Get 返回 `ErrClosed`。Close 幂等。
- `Snapshot()`：返回当前待处理项的有序独立副本，以及 `Waiting`（阻塞中的 Get 数）和 `Closed`。
- `Len()`：当前待处理的唯一 key 数量。

## 结构

- `queue.go` — 库（`package coalescingqueue`）
- `queue_test.go` — 自动化测试（FIFO、同 key 合并、重入队、阻塞 Get、并发不重复、ctx 取消、Close 排空、Snapshot）
- `cmd/coalesce-demo/` — 极简 CLI：演示普通 Put、同 key 合并更新、顺序 Get、Close 排空

## 运行

```sh
go test -race ./...
go run ./cmd/coalesce-demo
```

## 示例

```go
q := coalescingqueue.New()
defer q.Close()

q.Put("a", 1)
q.Put("b", 2)
q.Put("a", 3) // 合并：a 的 value 变为 3，位置不变

k, v, err := q.Get(context.Background()) // "a", 3
k, v, err = q.Get(context.Background())  // "b", 2
```
