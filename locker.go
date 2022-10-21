package distributedlocker

import (
	"context"
	"errors"
	"log"
	"os"
	"time"
)

var (
	ErrAcquireLockFailed             = errors.New("distributed lock, failed to acquire lock")
	ErrUnLockFailed                  = errors.New("distributed lock, failed to unlock")
	ErrRenewFailed                   = errors.New("distributed lock, failed to renew")
	ErrLockLocked                    = errors.New("distributed lock, lock of locked mutex")
	ErrMustInitLocker                = errors.New("distributed lock, must init locker")
	ErrUnLockUnLocked                = errors.New("distributed lock, unlock of unlocked mutex")
	ErrMustInitDefaultLLockerBuilder = errors.New("distributed lock, default locker builder is nil, use MustNewDefaultLockerBuilder first")
)

type Mutex interface {
	// Name returns mutex name (i.e. the Redis key).
	Name() string
	// Value returns the current xid value. The value will be empty until a lock is acquired.
	Value() string
	// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
	Until() time.Time
	// Renew resets the mutex's expiry
	Renew(ctx context.Context) error
	// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
	Lock(ctx context.Context) error
	// UnLock unlocks m.
	UnLock(ctx context.Context) error
}

type RWMutex interface {
	Mutex

	// RLock locks m for reading. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
	RLock(ctx context.Context) (err error)
	// RUnLock unlocks m for reading.
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
