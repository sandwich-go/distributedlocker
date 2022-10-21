package redis

import "context"

type Scripter interface {
	Eval(ctx context.Context, keys []string, args ...interface{}) (interface{}, error)
	EvalSha(ctx context.Context, keys []string, args ...interface{}) (interface{}, error)
}

type Cmdable interface {
	CreateScript(src string) Scripter
	IsNil(err error) bool
}
