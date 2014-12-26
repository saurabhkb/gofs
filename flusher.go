package main

import (
	"fmt"
	"time"
	"encoding/json"
)

/*
=======================
FLUSHER
=======================
*/

const SLEEP_SECONDS int = 5

type Flusher struct {}

/*
	Flusher checks for quit messages only every SLEEP_SECONDS, but thats ok for now
	In the worst case, the user will have to wait for SLEEP_SECONDS before the program ends completely
*/
func (Flusher) flush(quit chan bool, fs *MyFS) {
	for {
		select {
			case <-quit: {
				P_out("CLOSING FLUSHER!")
				db.Close()
				return
			}
			default: {
				P_out("FLUSHER: attempting to lock")
				LOCK.Lock()
				P_out("FLUSHER: lock successful")
				generalRoot, _ := fs.Root()
				P_out("got fs.Root()")
				root, _ = generalRoot.(*MyNode)	// ignore error for now, ideally there shouldnt be any (maybe panic if there is)
				P_out("made sure it was a *MyNode")
				writeBack(root)
				LOCK.Unlock()
				P_out("FLUSHER: unlock successful")
				time.Sleep(time.Duration(SLEEP_SECONDS) * time.Second)
			}
		}
	}
}

func writeBack(root *MyNode) {
	// if root isn't dirty, no child is dirty since when a child is updated, the changes always propagate up to the root
	if root == nil || !root.dirty {
		return
	}

	if root.isDir() {
		assertExpanded(root)
		// if im a dir, recursively save children, then save myself (postorder)
		for _, v := range root.children {
			writeBack(v);
		}
	} else {
		assertExpanded(root)
		// if im a file, write out my data blocks and hashes
		fmt.Println("LENGTHS:", len(root.DataBlocks), len(root.BlockOffsets), len(root.BlockLengths))
		for i := 0; i < len(root.DataBlocks); i++ {
			str := root.DataBlocks[i]
			off := root.BlockOffsets[i]
			ret := root.BlockLengths[i]
			db.Put([]byte(fmt.Sprintf("%s:%v", DATA_KEY, str)), root.data[off:off + ret])
		}
	}

	// ive been updated, save me
	nodestr, _ := json.Marshal(root)
	db.Put([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, root.Vid)), nodestr)

	// save the state as well (only once at the very end)
	if root.parent == nil {
		statestr, _ := json.Marshal(state)
		err := db.Put([]byte(STATE_KEY), statestr)
		P_out("Saving state error: %v", err)
	}
}