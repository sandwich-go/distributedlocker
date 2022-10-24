package distributedlocker

//go:generate stringer -type=lockerMode
type lockerMode int

const (
	lockerModeRead  lockerMode = 1
	lockerModeWrite lockerMode = 2
)
