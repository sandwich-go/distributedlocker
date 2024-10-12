package distributedlocker

import (
	"context"
	"testing"
	"time"

	"github.com/sandwich-go/redisson"
	. "github.com/smartystreets/goconvey/convey"
)

func setupForTest() {
	MustNewDefaultLockerBuilder(redisson.MustNewClient(redisson.NewConf(redisson.WithAddrs("127.0.0.1:6379"))))
}

func Test_Lock(t *testing.T) {
	setupForTest()
	Convey("testLock Lock()", t, func() {
		ctx := context.Background()
		key := "123"
		mu := NewMutex(key)
		err := mu.Lock(ctx)
		So(err, ShouldBeNil)
		err = mu.UnLock(ctx)
		So(err, ShouldBeNil)

		mu2 := NewMutex(key)
		err = mu2.Lock(ctx)
		So(err, ShouldBeNil)

		mu3 := NewMutex(key)
		err = mu3.Lock(ctx)
		So(err, ShouldEqual, ErrAcquireLockFailed)

		err = mu2.UnLock(ctx)
		So(err, ShouldBeNil)
	})
}

func Test_RWLock(t *testing.T) {
	setupForTest()
	Convey("testLock RWLock()", t, func() {
		ctx := context.Background()
		key := "123"
		mu := NewRWMutex(key)
		err := mu.Lock(ctx)
		So(err, ShouldBeNil)
		t.Log("until", mu.Until())
		err = mu.UnLock(ctx)
		So(err, ShouldBeNil)

		mu2 := NewRWMutex(key)
		err = mu2.RLock(ctx)
		So(err, ShouldBeNil)

		mu3 := NewRWMutex(key)
		err = mu3.RLock(ctx)
		So(err, ShouldBeNil)

		mu4 := NewRWMutex(key)
		err = mu4.Lock(ctx)
		So(err, ShouldEqual, ErrAcquireLockFailed)

		err = mu2.UnLock(ctx)
		So(err, ShouldEqual, ErrUnLockUnLocked)

		err = mu2.RUnLock(ctx)
		So(err, ShouldBeNil)

		err = mu3.RUnLock(ctx)
		So(err, ShouldBeNil)

		err = mu4.Lock(ctx)
		So(err, ShouldBeNil)

		err = mu3.RLock(ctx)
		So(err, ShouldEqual, ErrAcquireLockFailed)

		err = mu4.UnLock(ctx)
		So(err, ShouldBeNil)
	})
}

func Test_Do(t *testing.T) {
	setupForTest()
	errFun := func(err error) error {
		if err != nil && err != ErrAcquireLockFailed {
			return err
		}
		return nil
	}
	count := 10
	ctx := context.Background()
	key := "123"
	Convey("testLock Do()", t, func() {
		calc := func() {
			Convey("testLock sub Do()", t, func() {
				for i := 0; i < 20; i++ {
					//test DO()
					err := Do(ctx, key, func() {
						if count > 0 {
							time.Sleep(1 * time.Nanosecond)
							count -= 1
						}
					})
					So(errFun(err), ShouldBeNil)
				}
			})
		}
		go calc()
		go calc()
		go calc()
		time.Sleep(5 * time.Second)
		err := Do(ctx, key, func() {
			t.Log("ok")
		})
		So(err, ShouldBeNil)
		So(count, ShouldEqual, 0)
	})
}
