package distributedlocker

import (
	"bitbucket.org/funplus/sandwich/base/retry"
	"context"
	"fmt"
	"github.com/rs/xid"
	"github.com/sandwich-go/redisson"
)

type Locker interface {
	tryLock(ctx context.Context, m lockerMode, key string) (identifier string, err error)
	unLock(ctx context.Context, m lockerMode, key string, value interface{}) error
	renew(ctx context.Context, key string) error
	options() OptionsVisitor
}

var (
	writeFieldName = "_w_"
	scripts        = map[lockerMode]struct {
		acquire string
		release string
	}{
		lockerModeCommon: {
			acquire: `
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX') == false then
	return 0
end
return 1
`,
			release: `
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`,
		},
		lockerModeRead: {
			acquire: fmt.Sprintf(`
if redis.call('HEXISTS', KEYS[1], '%s') == 1 then
	return 0
end
redis.call('HSET', KEYS[1], ARGV[1], 1)
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`, writeFieldName),
			release: `
return redis.call('HDEL', KEYS[1], ARGV[1])
`,
		},
		lockerModeWrite: {
			acquire: fmt.Sprintf(`
if redis.call('HLEN', KEYS[1]) > 1 then
	return 0
end
local value = redis.call('HSETNX', KEYS[1], '%s', ARGV[1])
if value > 0 then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return value
`, writeFieldName),
			release: fmt.Sprintf(`
if redis.call('HGET', KEYS[1], '%s') == ARGV[1] then
	return redis.call('HDEL', KEYS[1], '%s')
end
return 0
`, writeFieldName, writeFieldName),
		},
	}
)

type redisLockerBuilder struct {
	locker *redisLocker
}

func MustNewDefaultLockerBuilder(cmd redisson.Cmdable, opts ...Option) LockerBuilder {
	defaultLockerBuilder = NewRedisLockerBuilder(cmd, opts...)
	return defaultLockerBuilder
}

func NewRedisLockerBuilder(cmd redisson.Cmdable, opts ...Option) LockerBuilder {
	return &redisLockerBuilder{locker: newRedisLocker(cmd, opts...)}
}

func (b *redisLockerBuilder) Build() Locker { return b.locker }

type redisLockerScript struct {
	acquire redisson.Scripter
	release redisson.Scripter
}

type redisLocker struct {
	cmd     redisson.Cmdable
	visitor OptionsInterface

	scripts map[lockerMode]redisLockerScript
}

func newRedisLocker(cmd redisson.Cmdable, opts ...Option) *redisLocker {
	visitor := NewOptions(opts...)
	if visitor.GetExpiration() < minExpiration {
		errLog.Print(fmt.Sprintf("distributed lock, expiration can not less than %s!!", minExpiration.String()))
		visitor.ApplyOption(WithExpiration(minExpiration))
	}
	ss := make(map[lockerMode]redisLockerScript)
	for k, v := range scripts {
		ss[k] = redisLockerScript{
			acquire: cmd.CreateScript(v.acquire),
			release: cmd.CreateScript(v.release),
		}
	}
	return &redisLocker{
		cmd:     cmd,
		visitor: visitor,
		scripts: ss,
	}
}

func (r *redisLocker) options() OptionsVisitor { return r.visitor }
func (r *redisLocker) renew(ctx context.Context, key string) error {
	return r.cmd.PExpire(ctx, r.genLockerKey(key), r.visitor.GetExpiration()).Err()
}

func (r *redisLocker) genLockerKey(key string) string {
	if len(r.visitor.GetPrefix()) > 0 {
		return fmt.Sprintf("%s:%s", r.visitor.GetPrefix(), key)
	}
	return key
}

func (r *redisLocker) retry(fn func(uint) error) error {
	return retry.Do(fn,
		retry.WithLimit(uint(r.visitor.GetRetryTimes())),
		retry.WithDelay(r.visitor.GetRetryInterval()),
		retry.WithLastErrorOnly(true),
	)
}

func (r *redisLocker) tryLock(ctx context.Context, m lockerMode, key string) (identifier string, err error) {
	script, exist := r.scripts[m]
	if !exist {
		err = fmt.Errorf("distributed lock, unknown mode, %s", m.String())
		return
	}
	keys := []string{r.genLockerKey(key)}
	identifier = xid.New().String()
	expiration := r.visitor.GetExpiration().Milliseconds()
	err = r.retry(func(uint) error {
		ok, err0 := script.acquire.Run(ctx, keys, identifier, expiration).Bool()
		if err0 != nil || ok {
			return err0
		}
		return ErrTryLockMaxNum
	})
	return
}

func (r *redisLocker) unLock(ctx context.Context, m lockerMode, key string, value interface{}) error {
	script, exist := r.scripts[m]
	if !exist {
		return fmt.Errorf("distributed lock, unknown mode, %s", m.String())
	}
	ok, err := script.release.Run(ctx, []string{r.genLockerKey(key)}, value).Bool()
	if err != nil {
		return err
	}
	if !ok {
		return ErrUnLock
	}
	return nil
}
