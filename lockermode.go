package distributedlocker

//go:generate stringer -type=lockerMode
type lockerMode int

const (
	lockerModeRead  lockerMode = 0
	lockerModeWrite lockerMode = 1
)
