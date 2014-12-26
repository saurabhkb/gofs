all:
	go run flusher.go control.go myfs.go mynode.go chunker.go stableStorage.go main.go -debug /tmp/mount /tmp/db

# /tmp/mount = mount point
# /tmp/db = database location
