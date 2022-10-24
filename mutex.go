package distributedlocker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type baseMutex struct {
	key    string
	locker Locker

	mx         sync.RWMutex // protects following fields
	m          lockerMode
	cancel     context.CancelFunc
	identifier string
	until      time.Time
}

func (r *baseMutex) Name() string                    { return r.key }
func (r *baseMutex) Value() string                   { return r.identifier }
func (r *baseMutex) Until() time.Time                { return r.until }
func (r *baseMutex) Renew(ctx context.Context) error { return r.renew(ctx, r.m) }

func (r *baseMutex) watchDog(ctx context.Context) {
	if !r.locker.options().GetAutoRenew() {
		return
	}
	go func() {
		var sleep = func() bool {
			now := time.Now()
			t := r.until.Sub(now)
			if t > 0 {
				time.Sleep(t >> 1)
			} else {
				errLog.Print(fmt.Sprintf("distributed lock, watch dog renew failed, key: %s, err: until: %s before now: %s", r.key, r.until.String(), now.String()))
			}
			return t > 0
		}
		time.Sleep(r.until.Sub(time.Now()) >> 1)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				//续期
				if err := r.renew(ctx, r.m); err != nil {
					errLog.Print(fmt.Sprintf("distributed lock, watch dog renew failed, key: %s, err: %v", r.key, err.Error()))
				}
				if !sleep() {
					return
				}
			}
		}
	}()
}

func (r *baseMutex) renew(ctx context.Context, m lockerMode) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	if r.locker == nil {
		return ErrMustInitLocker
	}
	if len(r.identifier) == 0 || r.m != m {
		return ErrUnLockUnLocked
	}
	until, err := r.locker.renew(ctx, m, r.key, r.identifier)
	if err != nil {
		return err
	}
	r.until = until
	return nil
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
	identifier, until, err := r.locker.tryLock(ctx, m, r.key)
	if err != nil {
		return err
	}
	//A new context is required to manage the dog
	dogCtx, dogCancel := context.WithCancel(context.Background())
	r.cancel = dogCancel
	r.identifier = identifier
	r.until = until
	r.m = m
	r.watchDog(dogCtx)
	return nil
}

func (r *baseMutex) unLock(ctx context.Context, m lockerMode) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	if r.locker == nil {
		return ErrMustInitLocker
	}
	if len(r.identifier) == 0 || r.m != m {
		return ErrUnLockUnLocked
	}
	r.cancel()
	err := r.locker.unLock(ctx, m, r.key, r.identifier)
	r.identifier = ""
	r.until = time.Now()
	return err
}

func Do(ctx context.Context, key string, fn func()) (err error) {
	mx := NewMutex(key)
	err = mx.Lock(ctx)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			errLog.Print(fmt.Sprintf("distributed lock, execute fn occur panic, key: %s, recover:%v", key, r))
		}
		if err0 := mx.UnLock(ctx); err0 != nil {
			errLog.Print(fmt.Sprintf("distributed lock, unlock error, key: %s, err:%v", key, err0.Error()))
		}
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
