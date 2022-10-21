# distributedlocker

分布式锁

## 例子

```golang
package main

import (
	"context"
	"fmt"
	"github.com/sandwich-go/distributedlocker"
	"github.com/sandwich-go/distributedlocker/redis"
	"github.com/sandwich-go/redisson"
)

func main() {
	distributedlocker.MustNewDefaultLockerBuilder(redis.NewRedisson(&redisson.Conf{Resp: redisson.RESP2, Addrs: []string{"127.0.0.1:6379"}}))

	ctx := context.Background()
	key0 := "123"
	
	mu0 := distributedlocker.NewRWMutex(key0)
	if err := mu0.RLock(ctx); err != nil {
		fmt.Println(err)
		return
	}
	
	mu1 := distributedlocker.NewRWMutex(key0)
	if err := mu1.RLock(ctx); err != nil {
		fmt.Println(err)
		return
	}

	if err := mu0.RUnLock(ctx); err != nil {
		fmt.Println(err)
		return
	}

	if err := mu1.RUnLock(ctx); err != nil {
		fmt.Println(err)
		return
	}

	mu2 := distributedlocker.NewRWMutex(key0)
	if err := mu2.Lock(ctx); err != nil {
		fmt.Println(err)
		return
	}
	if err := mu2.UnLock(ctx); err != nil {
		fmt.Println(err)
		return
	}

	key1 := "456"
	mu3 := distributedlocker.NewMutex(key1)
	if err := mu3.Lock(ctx); err != nil {
		fmt.Println(err)
		return
	}
	if err := mu3.UnLock(ctx); err != nil {
		fmt.Println(err)
		return
	}
}
```