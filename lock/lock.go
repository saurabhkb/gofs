package lock

import "sync"

var LOCK *sync.Mutex

/* initialize the lock */
func Init() {
	LOCK = &sync.Mutex{}
}

