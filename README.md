# coalescing-queue

Go 实现的并发安全 Coalescing Queue(合并队列):同一个 key 在未被消费前最多只有一个待处理项,重复 `Put` 只更新 value 并保持原队列位置。仅依赖标准库,无第三方服务、无后台 goroutine。

## 语义

- **FIFO**:按 key 首次入队的顺序消费;`Get` 原子移除队首,并发 `Get` 不会重复取得同一项。
- **合并**:对已在队列中的 key 再次 `Put`,仅替换 value,位置不变,不新增条目;key 被 `Get` 消费后再 `Put` 则排到当前队尾。
- **阻塞与取消**:队列为空时 `Get` 等待;context 取消只影响该次调用,取消与元素到达并发时只会有一种结果,不重复交付、不丢元素。
- **关闭**:`Close` 后拒绝新的 `Put`;存量条目继续按 FIFO 排空,耗尽后 `Get` 返回 `ErrClosed`。

## API

```go
q := coalescingqueue.New()
err := q.Put(key, value)            // key 为空返回 ErrEmptyKey;关闭后返回 ErrClosed
key, value, err := q.Get(ctx)       // 空队列阻塞;关闭且耗尽后返回 ErrClosed
n := q.Len()                        // 当前待处理的唯一 key 数量
snap := q.Snapshot()                // 有序独立副本:Items / Waiting / Closed
q.Close()                           // 幂等
```

## 示例

```go
q := coalescingqueue.New()
q.Put("a", 1)
q.Put("b", 2)
q.Put("a", 3) // 合并:a 仍为队首,value 变为 3

k, v, _ := q.Get(context.Background()) // "a", 3
k, v, _ = q.Get(context.Background())  // "b", 2
```

## CLI 演示

```
go run ./cmd/cqdemo
```

依次演示普通 Put、同 key 合并更新、按序 Get,以及 Close 后排空。

## 测试

```
go test -race ./...
```
