package redis

import (
	"context"
	"github.com/sandwich-go/redisson"
)

type RedissonScript struct {
	redisson.Scripter
}

func (r *RedissonScript) Eval(ctx context.Context, keys []string, args ...interface{}) (interface{}, error) {
	return r.Scripter.Eval(ctx, keys, args...).Result()
}

func (r *RedissonScript) EvalSha(ctx context.Context, keys []string, args ...interface{}) (interface{}, error) {
	return r.Scripter.EvalSha(ctx, keys, args...).Result()
}

type Redisson struct {
	redisson.Cmdable
}

func NewRedisson(conf *redisson.Conf) Cmdable {
	return NewRedissonWithClient(redisson.MustNewClient(conf))
}

func NewRedissonWithClient(cmd redisson.Cmdable) Cmdable {
	return &Redisson{Cmdable: cmd}
}

func (r *Redisson) IsNil(err error) bool { return redisson.IsNil(err) }
func (r *Redisson) CreateScript(src string) Scripter {
	return &RedissonScript{r.Cmdable.CreateScript(src)}
}
