package fsys

import (
	"time"
	"p4/storage"
	"p4/lock"
	"p4/util"
	"fmt"
	"encoding/json"
)


var dirtyNodesList map[string]MyNode

func init() {
	dirtyNodesList = make(map[string]MyNode)
}

const SLEEP_SECONDS int = 5

func ClearDirtyNodesList() {
	for k := range dirtyNodesList {
		delete(dirtyNodesList, k)
	}
}

func Flush(quit chan bool, f *MyFS) {
	for {
		select {
			case <-quit: {
				storage.Close()
				return
			}
			default: {
				lock.LOCK.Lock()
				FlushFilesystem(f)
				if len(dirtyNodesList) > 0 {
					for k := range dirtyNodesList {
						util.P_out("dirty: %s => %s", k, dirtyNodesList[k].Name)
					}
					SendUpdateMessage(dirtyNodesList)
					ClearDirtyNodesList()
				}
				lock.LOCK.Unlock()
				time.Sleep(time.Duration(SLEEP_SECONDS) * time.Second)
			}
		}
	}
}


func FlushFilesystem(f *MyFS) {
	r, _ := f.Root()
	mynoder, _ := r.(*MyNode)
	writeBack(mynoder)
}

func writeBack(root *MyNode) {
	// if root isn't dirty, no child is dirty since when a child is updated, the changes always propagate up to the root
	if root == nil || !root.dirty {
		if root != nil {
		}
		return
	}

	if root.Attrib.Mode.IsDir() {
		AssertExpanded(root)
		// if im a dir, recursively save children, then save myself (postorder)
		for _, v := range root.children {
			writeBack(v);
		}
	} else {
		root.WriteBackData()
	}

	originalVid := root.Vid
	root.Vid = GenerateVersionId(root)	/* update vid */
	d := *root;
	util.P_out("in dirty list: %s => %v", originalVid, d)
	dirtyNodesList[originalVid] = d

	// ive been updated, save me
	nodestr, _ := json.Marshal(root)
	storage.Put([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, root.Vid)), nodestr)

	RegisterNodeVersion(root.NodeID, root.Vid)

	//util.P_out("write back %s", root.Name)
	root.dirty = false

	// save the state as well (only once at the very end)
	if root.Name == "/" {
		statestr, _ := json.Marshal(State)
		storage.Put([]byte(STATE_KEY), statestr)
	}
}
