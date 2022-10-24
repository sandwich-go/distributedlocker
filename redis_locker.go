package distributedlocker

import (
	"context"
	"fmt"
	"github.com/rs/xid"
	"github.com/sandwich-go/distributedlocker/redis"
	"math/rand"
	"strings"
	"time"
)

type Locker interface {
	tryLock(ctx context.Context, m lockerMode, key string) (identifier string, until time.Time, err error)
	unLock(ctx context.Context, m lockerMode, key string, identifier string) error
	renew(ctx context.Context, m lockerMode, key string, identifier string) (until time.Time, err error)
	options() OptionsVisitor
}

var (
	writeFieldName = "_w_"
	scripts        = map[lockerMode]struct {
		acquire, release, renew string
	}{
		lockerModeRead: {
			// 请求读锁
			// keys: [locker key]
			// argvs: [identifier, expiration]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 存在写锁，则直接返回失败
			// 2. 设置读锁，属性key为identifier，属性值为1
			// 3. 设置锁过期时间为expiration
			acquire: fmt.Sprintf(`
if redis.call('HEXISTS', KEYS[1], '%s') == 1 then
	return 0
end
redis.call('HSET', KEYS[1], ARGV[1], 1)
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return 1
`, writeFieldName),

			// 释放读锁
			// keys: [locker key]
			// argvs: [identifier]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 删除读锁中属性key为identifier的属性
			release: `
return redis.call('HDEL', KEYS[1], ARGV[1])
`,

			// 续约读锁
			// keys: [locker key]
			// argvs: [expiration]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 设置锁过期时间为expiration
			renew: `
return redis.call('PEXPIRE', KEYS[1], ARGV[1])
`,
		},

		lockerModeWrite: {
			// 请求写锁
			// keys: [locker key]
			// argvs: [identifier, expiration]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 如果锁的属性数量多于1，则表示存在读锁，直接返回失败
			// 2. 设置写锁，属性key为'writeFieldName'，属性值为identifier
			// 3. 设置锁过期时间为expiration
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

			// 释放写锁
			// keys: [locker key]
			// argvs: [identifier]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 删除属性key为'writeFieldName'，属性值为identifier的属性
			release: fmt.Sprintf(`
if redis.call('HGET', KEYS[1], '%s') == ARGV[1] then
	return redis.call('HDEL', KEYS[1], '%s')
end
return 0
`, writeFieldName, writeFieldName),

			// 续约读锁
			// keys: [locker key]
			// argvs: [expiration]
			// return:
			//		ok: 1
			// 		fail: 0
			// 1. 设置锁过期时间为expiration
			renew: `
return redis.call('PEXPIRE', KEYS[1], ARGV[1])
`,
		},
	}
)

type redisLockerBuilder struct {
	locker *redisLocker
}

func MustNewDefaultLockerBuilder(cmd redis.Cmdable, opts ...Option) LockerBuilder {
	defaultLockerBuilder = NewRedisLockerBuilder(cmd, opts...)
	return defaultLockerBuilder
}

func NewRedisLockerBuilder(cmd redis.Cmdable, opts ...Option) LockerBuilder {
	return &redisLockerBuilder{locker: newRedisLocker(cmd, opts...)}
}

func (b *redisLockerBuilder) Build() Locker { return b.locker }

type redisLockerScript struct {
	acquire, release, renew *redisScript
}

type redisScript struct {
	redis.Scripter
}

func (r *redisScript) Run(ctx context.Context, keys []string, args ...interface{}) (interface{}, error) {
	res, err := r.Scripter.EvalSha(ctx, keys, args...)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		res, err = r.Scripter.Eval(ctx, keys, args...)
	}
	return res, err
}

type redisLocker struct {
	visitor OptionsInterface
	cmd     redis.Cmdable
	scripts map[lockerMode]*redisLockerScript
}

func newRedisLocker(cmd redis.Cmdable, opts ...Option) *redisLocker {
	visitor := NewOptions(opts...)
	if visitor.GetExpiration() < minExpiration {
		errLog.Print(fmt.Sprintf("distributed lock, expiration can not less than %s!!", minExpiration.String()))
		visitor.ApplyOption(WithExpiration(minExpiration))
	}
	if visitor.GetMaxRetryInterval() < maxRetryInterval {
		errLog.Print(fmt.Sprintf("distributed lock, max retry interval can not less than %s!!", maxRetryInterval.String()))
		visitor.ApplyOption(WithMaxRetryInterval(maxRetryInterval))
	}
	if visitor.GetMinRetryInterval() < minRetryInterval {
		errLog.Print(fmt.Sprintf("distributed lock, min retry interval can not less than %s!!", minRetryInterval.String()))
		visitor.ApplyOption(WithMinRetryInterval(minRetryInterval))
	}
	ss := make(map[lockerMode]*redisLockerScript)
	for k, v := range scripts {
		ss[k] = &redisLockerScript{
			acquire: &redisScript{cmd.CreateScript(v.acquire)},
			release: &redisScript{cmd.CreateScript(v.release)},
			renew:   &redisScript{cmd.CreateScript(v.renew)},
		}
	}
	return &redisLocker{
		visitor: visitor,
		cmd:     cmd,
		scripts: ss,
	}
}

func (r *redisLocker) options() OptionsVisitor { return r.visitor }
func (r *redisLocker) genLockerKey(key string) string {
	if p := r.visitor.GetPrefix(); len(p) > 0 {
		return fmt.Sprintf("%s:{%s}", p, key)
	}
	return key
}

func (r *redisLocker) genLockerTimeoutKey(key, identifier string) string {
	if p := r.visitor.GetPrefix(); len(p) > 0 {
		return fmt.Sprintf("%s:{%s}:%s:rwlock_timeout", p, key, identifier)
	}
	return fmt.Sprintf("{%s}:%s:rwlock_timeout", key, identifier)
}

func (r *redisLocker) genLockerKeyPrefix(identifier, lockerTimeoutKey string) string {
	return strings.TrimSuffix(strings.Split(lockerTimeoutKey, identifier)[0], ":")
}

func (r *redisLocker) genWriteIdentifier(identifier string) string {
	return fmt.Sprintf("%s:write", identifier)
}

func (r *redisLocker) getScript(m lockerMode) (*redisLockerScript, error) {
	script, exist := r.scripts[m]
	if !exist {
		return nil, fmt.Errorf("distributed lock, unknown mode, %s", m.String())
	}
	return script, nil
}

func (r *redisLocker) delay() time.Duration {
	return time.Duration(
		int64(rand.Intn(int(r.visitor.GetMaxRetryInterval().Milliseconds()-r.visitor.GetMinRetryInterval().Milliseconds())))+
			r.visitor.GetMinRetryInterval().Milliseconds()) * time.Millisecond
}

func (r *redisLocker) calUntil(start time.Time) (time.Time, bool) {
	now := time.Now()
	until := now.Add(r.visitor.GetExpiration() - now.Sub(start) - time.Duration(int64(float64(r.visitor.GetExpiration())*r.visitor.GetDriftFactor())))
	return until, now.Before(until)
}

func (r *redisLocker) tryLock(ctx context.Context, m lockerMode, key string) (identifier string, until time.Time, err error) {
	var script *redisLockerScript
	script, err = r.getScript(m)
	if err != nil {
		return
	}
	identifier = xid.New().String()
	var keys = []string{r.genLockerKey(key)}
	var args = []interface{}{identifier, r.visitor.GetExpiration().Milliseconds()}

	tries := r.visitor.GetRetryTimes()
	for i := 0; i < tries; i++ {
		if i != 0 {
			select {
			case <-ctx.Done():
				// Exit early if the context is done.
				err = ErrAcquireLockFailed
				return
			case <-time.After(r.delay()):
				// Fall-through when the delay timer completes.
			}
		}
		start := time.Now()
		ctx0, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(r.visitor.GetExpiration())*r.visitor.GetTimeoutFactor())))
		status, err0 := script.acquire.Run(ctx0, keys, args...)
		cancel()
		if err0 != nil || status.(int64) == 0 {
			if i == tries-1 && err0 != nil {
				errLog.Print(fmt.Sprintf("distributed lock, try lock error, %s", err0.Error()))
			}
			continue
		}
		var ok bool
		if until, ok = r.calUntil(start); ok {
			return
		}
	}
	err = ErrAcquireLockFailed
	return
}

func (r *redisLocker) unLock(ctx context.Context, m lockerMode, key string, identifier string) error {
	script, err := r.getScript(m)
	if err != nil {
		return err
	}
	var keys = []string{r.genLockerKey(key)}
	var args = []interface{}{identifier}
	_, err = script.release.Run(ctx, keys, args)
	if err != nil {
		errLog.Print(fmt.Sprintf("distributed lock, unlock error, %s", err.Error()))
		return ErrUnLockFailed
	}
	return nil
}

func (r *redisLocker) renew(ctx context.Context, m lockerMode, key, _ string) (until time.Time, err error) {
	var script *redisLockerScript
	script, err = r.getScript(m)
	if err != nil {
		return
	}
	var keys = []string{r.genLockerKey(key)}
	var args = []interface{}{r.visitor.GetExpiration().Milliseconds()}
	start := time.Now()
	var status interface{}
	status, err = script.renew.Run(ctx, keys, args)
	if err != nil {
		return
	}
	if status.(int64) == 0 {
		err = ErrRenewFailed
		return
	}
	var ok bool
	if until, ok = r.calUntil(start); !ok {
		err = ErrRenewFailed
		return
	}
	return
}
