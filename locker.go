package distributedlocker

import (
	"context"
	"errors"
	"log"
	"os"
)

var (
	ErrTryLockMaxNum                 = errors.New("distributed lock, try lock failed, wait for the max limit")
	ErrUnLock                        = errors.New("distributed lock, unlock failed")
	ErrMustInitLocker                = errors.New("distributed lock, must init locker")
	ErrLockLocked                    = errors.New("distributed lock, lock of locked mutex")
	ErrUnLockUnLocked                = errors.New("distributed lock, unlock of unlocked mutex")
	ErrMustInitDefaultLLockerBuilder = errors.New("distributed lock, default locker builder is nil, use MustNewDefaultLockerBuilder first")
)

type Mutex interface {
	Lock(ctx context.Context) (err error)
	UnLock(ctx context.Context) (err error)
}

type RWMutex interface {
	Lock(ctx context.Context) (err error)
	UnLock(ctx context.Context) (err error)
	RLock(ctx context.Context) (err error)
	RUnLock(ctx context.Context) (err error)
}

var defaultLockerBuilder LockerBuilder

type LockerBuilder interface {
	Build() Locker
}

var errLog = Logger(log.New(os.Stderr, "[distributed locker] ", log.Ldate|log.Ltime|log.Lshortfile))

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	errLog = logger
	return nil
}
