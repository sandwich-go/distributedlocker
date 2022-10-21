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
	scripts = map[lockerMode]struct {
		acquire, release, renew string
	}{
		lockerModeBase: {
			// 请求锁
			// keys: genLockerKey(key)
			// args: expiration, identifier
			// return:
			//	ok: nil
			//	fail: ttl
			acquire: `
if (redis.call('EXISTS', KEYS[1]) == 0) then
	redis.call('HINCRBY', KEYS[1], ARGV[2], 1);
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	return nil;
end;
if (redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1) then
	redis.call('HINCRBY', KEYS[1], ARGV[2], 1);
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	return nil;
end;	
return redis.call('PTTL', KEYS[1]);
`,
			// 释放锁
			// keys: genLockerKey(key)
			// args: expiration, identifier
			// return:
			//	ok: 0/1, 1 表示不再有任何人持有该锁
			//	fail: nil
			release: `
if (redis.call('HEXISTS', KEYS[1], ARGV[2]) == 0) then
	return nil;
end;		
local counter = redis.call('HINCRBY', KEYS[1], ARGV[2], -1);
if (counter > 0) then
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	return 0;	
else
	redis.call('DEL', KEYS[1]);
	return 1;
end;	
return nil;
`,
			// 续约
			// keys: genLockerKey(key)
			// args: expiration, identifier
			// ok: 1
			// fail: 0
			renew: `
if (redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1) then
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	return 1;	
end;
return 0;	
`,
		},

		lockerModeRead: {
			// 请求读锁
			// keys: genLockerKey(key), {genLockerKey(key)}:identifier:rwlock_timeout
			// args: expiration, identifier, identifier:write
			// return:
			//	ok: nil
			//	fail: ttl
			acquire: `
local mode = redis.call('HGET', KEYS[1], 'mode');
if (mode == false) then
	local key = KEYS[2] .. ':' .. '1';
	redis.call('HSET', KEYS[1], 'mode', 'read');
	redis.call('HSET', KEYS[1], ARGV[2], 1);
	redis.call('SET', key, 1);
	redis.call('PEXPIRE', key, ARGV[1]);
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	return nil; 
end;
if (mode == 'read') or (mode == 'write' and redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1) then
	local ind = redis.call('HINCRBY', KEYS[1], ARGV[2], 1);
	local key = KEYS[2] .. ':' .. ind;
	redis.call('SET', key, 1);
	redis.call('PEXPIRE', key, ARGV[1]);
	local remainTime = redis.call('PTTL', KEYS[1]);
	redis.call('PEXPIRE', KEYS[1], math.max(remainTime, ARGV[1]));
	return nil; 
end;	
return redis.call('PTTL', KEYS[1]);
`,
			// 释放读锁
			// keys: genLockerKey(key), {genLockerKey(key)}:identifier:rwlock_timeout, {genLockerKey(key)}
			// args: expiration, identifier
			// return:
			//	ok: 0/1, 1 表示不再有任何人持有该锁
			//	fail: nil
			release: `
local mode = redis.call('HGET', KEYS[1], 'mode');
if (mode == false) then
	return 1;
end;	
local lockExists = redis.call('HEXISTS', KEYS[1], ARGV[2]);
if (lockExists == 0) then
	return nil;
end;
local counter = redis.call('HINCRBY', KEYS[1], ARGV[2], -1);
if (counter == 0) then
	redis.call('HDEL', KEYS[1], ARGV[2]);
end;	
redis.call('DEL', KEYS[2] .. ':' .. (counter+1));
if (redis.call('HLEN', KEYS[1]) > 1) then
	local maxRemainTime = -3;
	local keys = redis.call('HKEYS', KEYS[1]);
	for n, key in ipairs(keys) do
		counter = tonumber(redis.call('HGET', KEYS[1], key));
		if type(counter) == 'number' then
			for i=counter, 1, -1 do
				local remainTime = redis.call('PTTL', KEYS[3] .. ':' .. key .. ':rwlock_timeout:' .. i);
				maxRemainTime = math.max(remainTime, maxRemainTime);
			end;	
		end;	
	end;	
	if maxRemainTime > 0 then
		redis.call('PEXPIRE', KEYS[1], maxRemainTime);
		return 0;
	end;	
	if mode == 'write' then
		return 0;
	end;	
end;	
redis.call('DEL', KEYS[1]);
return 1;
`,
			// 续约
			// keys: genLockerKey(key), {genLockerKey(key)}
			// args: expiration, identifier
			// ok: 1
			// fail: 0
			renew: `
local counter = redis.call('HGET', KEYS[1], ARGV[2]);
if (counter ~= false) then
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	if (redis.call('HLEN', KEYS[1]) > 1) then
		local keys = redis.call('HKEYS', KEYS[1]);
		for n, key in ipairs(keys) do
			counter = tonumber(redis.call('HGET', KEYS[1], key));
			if type(counter) == 'number' then
				for i=counter, 1, -1 do
					redis.call('PEXPIRE', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]);
				end;	
			end;	
		end;	
	end;	
	return 1;
end;	
return 0;
`,
		},

		lockerModeWrite: {
			// 请求写锁
			// keys: genLockerKey(key)
			// args: expiration, identifier:write
			// return:
			//	ok: nil
			//	fail: ttl
			acquire: `
local mode = redis.call('HGET', KEYS[1], 'mode');
if (mode == false) then
	redis.call('HSET', KEYS[1], 'mode', 'write');
    redis.call('HSET', KEYS[1], ARGV[2], 1);
    redis.call('PEXPIRE', KEYS[1], ARGV[1]);
    return nil;
end;
if (mode == 'write') then
	if (redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1) then
		redis.call('HINCRBY', KEYS[1], ARGV[2], 1);
		local currentExpire = redis.call('PTTL', KEYS[1]);
		redis.call('PEXPIRE', KEYS[1], currentExpire + ARGV[1]);
		return nil;
	end	
end;	
return redis.call('PTTL', KEYS[1]);
`,
			// 释放写锁
			// keys: genLockerKey(key)
			// args: expiration, identifier:write
			// return:
			//	ok: 0/1, 1 表示不再有任何人持有该锁
			//	fail: nil
			release: `
local mode = redis.call('HGET', KEYS[1], 'mode');
if (mode == false) then
	return 1;
end;
if (mode == 'write') then
	local lockExists = redis.call('HEXISTS', KEYS[1], ARGV[2]);
	if (lockExists == 0) then
		return nil;
	else
		local counter = redis.call('HINCRBY', KEYS[1], ARGV[2], -1); 
		if (counter > 0) then
			redis.call('PEXPIRE', KEYS[1], ARGV[1]);
			return 0;
		else
			redis.call('HDEL', KEYS[1], ARGV[2]);	
			if (redis.call('HLEN', KEYS[1]) == 1) then
				redis.call('DEL', KEYS[1]);
			else
				redis.call('HSET', KEYS[1], 'mode', 'read');
			end;	
			return 1;
		end;	
	end;	
end;
return nil;	
`,
			// 续约
			// keys: genLockerKey(key), {genLockerKey(key)}
			// args: expiration, identifier
			// ok: 1
			// fail: 0
			renew: `
local counter = redis.call('HGET', KEYS[1], ARGV[2]);
if (counter ~= false) then
	redis.call('PEXPIRE', KEYS[1], ARGV[1]);
	if (redis.call('HLEN', KEYS[1]) > 1) then
		local keys = redis.call('HKEYS', KEYS[1]);
		for n, key in ipairs(keys) do
			counter = tonumber(redis.call('HGET', KEYS[1], key));
			if type(counter) == 'number' then
				for i=counter, 1, -1 do
					redis.call('PEXPIRE', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]);
				end;	
			end;	
		end;	
	end;	
	return 1;
end;	
return 0;
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
	expiration := r.visitor.GetExpiration().Milliseconds()
	var keys = make([]string, 0, 2)
	var args = make([]interface{}, 0, 3)
	switch m {
	case lockerModeBase:
		keys = append(keys, r.genLockerKey(key))
		args = append(args, expiration, identifier)
	case lockerModeRead:
		keys = append(keys, r.genLockerKey(key), r.genLockerTimeoutKey(key, identifier))
		args = append(args, expiration, identifier, r.genWriteIdentifier(identifier))
	case lockerModeWrite:
		keys = append(keys, r.genLockerKey(key))
		args = append(args, expiration, r.genWriteIdentifier(identifier))
	}
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
		ttl, err0 := script.acquire.Run(ctx0, keys, args...)
		cancel()
		if ttl == nil && err0 != nil && r.cmd.IsNil(err0) {
			var ok bool
			if until, ok = r.calUntil(start); ok {
				return
			}
		}
		ctx0, cancel = context.WithTimeout(ctx, time.Duration(int64(float64(r.visitor.GetExpiration())*r.visitor.GetTimeoutFactor())))
		_ = r.unLock(ctx0, m, key, identifier)
		cancel()
	}
	err = ErrAcquireLockFailed
	return
}

func (r *redisLocker) unLock(ctx context.Context, m lockerMode, key string, identifier string) error {
	script, err := r.getScript(m)
	if err != nil {
		return err
	}
	expiration := r.visitor.GetExpiration().Milliseconds()
	var keys = make([]string, 0, 3)
	var args = make([]interface{}, 0, 2)
	switch m {
	case lockerModeBase:
		keys = append(keys, r.genLockerKey(key))
		args = append(args, expiration, identifier)
	case lockerModeRead:
		timeoutKey := r.genLockerTimeoutKey(key, identifier)
		keys = append(keys, r.genLockerKey(key), timeoutKey, r.genLockerKeyPrefix(identifier, timeoutKey))
		args = append(args, expiration, identifier)
	case lockerModeWrite:
		keys = append(keys, r.genLockerKey(key))
		args = append(args, expiration, r.genWriteIdentifier(identifier))
	}
	_, err = script.release.Run(ctx, keys, args)
	if err != nil {
		return ErrUnLockFailed
	}
	return nil
}

func (r *redisLocker) renew(ctx context.Context, m lockerMode, key, identifier string) (until time.Time, err error) {
	var script *redisLockerScript
	script, err = r.getScript(m)
	if err != nil {
		return
	}
	expiration := r.visitor.GetExpiration().Milliseconds()
	var keys = make([]string, 0, 2)
	var args = make([]interface{}, 0, 2)
	switch m {
	case lockerModeBase:
		keys = append(keys, r.genLockerKey(key))
		args = append(args, expiration, identifier)
	case lockerModeRead, lockerModeWrite:
		timeoutKey := r.genLockerTimeoutKey(key, identifier)
		keys = append(keys, r.genLockerKey(key), timeoutKey, r.genLockerKeyPrefix(identifier, timeoutKey))
		args = append(args, expiration, identifier)
	}
	start := time.Now()
	var status interface{}
	status, err = script.renew.Run(ctx, keys, args)
	if err != nil {
		return
	}
	var ok bool
	if until, ok = r.calUntil(start); !ok || status == 0 {
		err = ErrRenewFailed
		return
	}
	return
}
