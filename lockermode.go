package distributedlocker

//go:generate stringer -type=lockerMode
type lockerMode int

const (
	lockerModeBase  lockerMode = 0
	lockerModeRead  lockerMode = 1
	lockerModeWrite lockerMode = 2
)
