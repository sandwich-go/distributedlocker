package distributedlocker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var emptyCancelFunc = func() {}

type baseMutex struct {
	key    string
	locker Locker

	mx         sync.Mutex // protects following fields
	m          lockerMode
	cancel     context.CancelFunc
	identifier string
}

func wrapDeadlineContext(ctx context.Context, expiration time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); !ok {
		return context.WithDeadline(ctx, time.Now().Add(expiration))
	}
	return ctx, emptyCancelFunc
}

func watchDog(ctx context.Context, key string, locker Locker) {
	time.Sleep(locker.options().GetExpiration() >> 1)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			//续期
			if err := locker.renew(ctx, key); err != nil {
				errLog.Print(fmt.Sprintf("distributed lock, watch dog renew failed, key: %s, err: %v", key, err.Error()))
			}
			time.Sleep(locker.options().GetExpiration() >> 1)
		}
	}
}

func (r *baseMutex) lock(ctx context.Context, m lockerMode) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	if r.locker == nil {
		return ErrMustInitLocker
	}
	if len(r.identifier) > 0 {
		return ErrLockLocked
	}
	var cancel context.CancelFunc
	ctx, cancel = wrapDeadlineContext(ctx, r.locker.options().GetExpiration())
	defer cancel()
	identifier, err := r.locker.tryLock(ctx, m, r.key)
	if err != nil {
		return err
	}
	//A new context is required to manage the dog
	dogCtx, dogCancel := context.WithCancel(context.Background())
	r.cancel = dogCancel
	go watchDog(dogCtx, r.key, r.locker)
	r.identifier = identifier
	r.m = m
	return nil
}

func (r *baseMutex) unLock(ctx context.Context, m lockerMode) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	if r.locker == nil {
		return ErrMustInitLocker
	}
	if len(r.identifier) == 0 {
		return ErrUnLockUnLocked
	}
	if r.m != m {
		return ErrUnLockUnLocked
	}
	var cancel context.CancelFunc
	ctx, cancel = wrapDeadlineContext(ctx, r.locker.options().GetExpiration())
	defer cancel()
	r.cancel()
	err := r.locker.unLock(ctx, m, r.key, r.identifier)
	r.identifier = ""
	return err
}

func Do(ctx context.Context, key string, fn func()) (err error) {
	if defaultLockerBuilder == nil {
		panic(ErrMustInitDefaultLLockerBuilder)
	}
	locker := defaultLockerBuilder.Build()
	var cancel context.CancelFunc
	ctx, cancel = wrapDeadlineContext(ctx, locker.options().GetExpiration())
	defer func() {
		cancel()
	}()
	var identifier string
	identifier, err = locker.tryLock(ctx, lockerModeWrite, key)
	if err != nil {
		return
	}
	dogCtx, dogCancel := context.WithCancel(context.Background())
	go watchDog(dogCtx, key, locker)
	defer func() {
		if r := recover(); r != nil {
			errLog.Print(fmt.Sprintf("distributed lock, execute fn occur panic, key: %s, recover:%v", key, r))
		}
		if err0 := locker.unLock(ctx, lockerModeWrite, key, identifier); err0 != nil {
			errLog.Print(fmt.Sprintf("distributed lock, unlock error, key: %s, err:%v", key, err0.Error()))
		}
		dogCancel()
	}()
	fn()
	return
}

type mutex struct {
	*baseMutex
}

func NewMutex(key string) Mutex {
	if defaultLockerBuilder == nil {
		panic(ErrMustInitDefaultLLockerBuilder)
	}
	return NewMutexWithLockerBuilder(key, defaultLockerBuilder)
}

func NewMutexWithLockerBuilder(key string, builder LockerBuilder) Mutex {
	return &mutex{&baseMutex{key: key, locker: builder.Build()}}
}

func (r *mutex) Lock(ctx context.Context) error   { return r.lock(ctx, lockerModeWrite) }
func (r *mutex) UnLock(ctx context.Context) error { return r.unLock(ctx, lockerModeWrite) }

type rwMutex struct {
	*baseMutex
}

func NewRWMutex(key string) RWMutex {
	if defaultLockerBuilder == nil {
		panic(ErrMustInitDefaultLLockerBuilder)
	}
	return NewRWMutexWithLockerBuilder(key, defaultLockerBuilder)
}

func NewRWMutexWithLockerBuilder(key string, builder LockerBuilder) RWMutex {
	return &rwMutex{&baseMutex{key: key, locker: builder.Build()}}
}

func (r *rwMutex) RLock(ctx context.Context) error   { return r.lock(ctx, lockerModeRead) }
func (r *rwMutex) Lock(ctx context.Context) error    { return r.lock(ctx, lockerModeWrite) }
func (r *rwMutex) RUnLock(ctx context.Context) error { return r.unLock(ctx, lockerModeRead) }
func (r *rwMutex) UnLock(ctx context.Context) error  { return r.unLock(ctx, lockerModeWrite) }
