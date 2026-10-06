// lockd 是本地可运行的演示程序：使用真实时钟与文件快照，
// 演示获取/续租/释放、等待唤醒与重启恢复。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lechandonga/lease-lock-coordinator-go/clock"
	"github.com/lechandonga/lease-lock-coordinator-go/coordinator"
)

func main() {
	snap := flag.String("snapshot", "/tmp/lockd-snapshot.json", "快照文件路径")
	flag.Parse()

	logger := log.New(os.Stdout, "[lockd] ", log.Ltime|log.Lmicroseconds)
	c, err := coordinator.New(coordinator.Config{
		Clock:  clock.Real(),
		Store:  coordinator.NewStore(*snap),
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	stop := c.StartReaper(100 * time.Millisecond)
	defer stop()

	run := fmt.Sprintf("%d", time.Now().UnixNano()) // 每轮使用唯一 requestID
	c.RegisterSession("alice")
	c.RegisterSession("bob")

	res, err := c.Acquire("alice", "resource-1", "alice-acq-"+run, 2*time.Second, false)
	if err != nil {
		log.Fatal(err)
	}
	g := res.Grant
	fmt.Printf("alice 获得锁, token=%d\n", g.Token)

	wait, _ := c.Acquire("bob", "resource-1", "bob-acq-"+run, 30*time.Second, true)
	fmt.Println("bob 排队等待...")

	time.AfterFunc(1500*time.Millisecond, func() {
		if _, err := c.Renew("alice", "resource-1", "alice-renew-"+run, g.Token, 2*time.Second); err != nil {
			fmt.Println("alice 续租:", err)
		}
	})
	// alice 只续租一次，之后租约到期，bob 被自动唤醒。
	grant := <-wait.WaitCh
	fmt.Printf("bob 被唤醒获得锁, token=%d（严格递增）\n", grant.Token)

	if err := c.Release("bob", "resource-1", "bob-rel-"+run, grant.Token); err != nil {
		fmt.Println("bob 释放:", err)
	}
	fmt.Printf("状态已持久化到 %s，重启本程序可观察恢复日志\n", *snap)
}
