package main

import (
	"fmt"
	"encoding/json"
	"os"
	"sync"
)

/* namespaces for the data store */
const DATA_KEY string = "DATA_KEY"
const NODE_VERSION_KEY string = "NDV"
const NODE_VERSION_LIST string = "NDVL"
const STATE_KEY = "STATE"


/* a structure to store state */
type STATE struct {
	Root_version_bootstrap int	/* latest root version */
	NextInode uint64			/* next available inode number */
	NextVId int					/* next available version number */
	NextNId int					/* next available node id */
}

/* important variables */
var db *MyStore
var state STATE
var LOCK *sync.Mutex

func InitDatabase(dbpath string) {
	db = &MyStore{}
	db.Init(dbpath)
}

func LoadState() {
	statestr, _ := db.Get([]byte(STATE_KEY))
	json.Unmarshal(statestr, &state)
	state.NextInode++
}

/* places an fs object in the memory pointed to by the argument */
func LoadFS(fs *MyFS) {
	/* state.Root_version_bootstrap is Vid of root of filesystem */
	str := fmt.Sprintf("%s:%d", NODE_VERSION_KEY, state.Root_version_bootstrap)
	rootdirstr, err := db.Get([]byte(str))
	if err != nil {
		/* key most likely doesn't exist */
		fs.RootDir = new(MyNode)
		fs.RootDir.Init("/", os.ModeDir | 0755, nil)
		SaveNodeVersion(fs.RootDir)
	} else {
		json.Unmarshal(rootdirstr, &fs.RootDir)
	}
	assertExpanded(fs.RootDir)
}

/* initialize the lock */
func InitLock() {
	LOCK = &sync.Mutex{}
}

/* self explanatory */
func GetAvailableInode() uint64 {
	state.NextInode++
	return state.NextInode
}


/* allocate globally unique version numbers */
func GetAvailableVersionId() int {
	state.NextVId++
	return state.NextVId
}

func GetAvailableUid() int {
	state.NextNId++
	return state.NextNId
}


/* "saves" a node corresponding to a new version -> basically sets its dirty flag */
func SaveNodeVersion(node *MyNode) bool {
	if node.parent == nil { /* root. update the bootstrap value */
		state.Root_version_bootstrap = node.Vid
	}
	node.dirty = true
	return true
}

/* load node */
func LoadNodeVersion(Vid int) *MyNode {
	var x MyNode
	nodestr, _ := db.Get([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, Vid)))
	json.Unmarshal(nodestr, &x)
	return &x
}

/* load data */
func LoadDataChunk(hash string) []byte {
	ret, _ := db.Get([]byte(fmt.Sprintf("%s:%v", DATA_KEY, hash)))
	return ret
}


/* expands this node and loads its children (if it hasn't already been done) */
func assertExpanded(node *MyNode) {
	if !node.expanded {
		/* dirty, children, data */
		node.dirty = false;

		if(node.isDir()) {
			/* children, not data */
			node.children = make(map[string]*MyNode)
			for name, Vid := range node.ChildVids {
				node.children[name] = LoadNodeVersion(Vid)
				node.children[name].parent = node
			}
		} else {
			/* data, not children */
			for i := 0; i < len(node.DataBlocks); i++ {
				loadedData := LoadDataChunk(node.DataBlocks[i])
				for j := 0; j < len(loadedData); j++ {
					node.data = append(node.data, loadedData[j])
				}
			}
		}

		node.expanded = true
	}
}


/* versioning */
func RegisterNodeVersion(nodeID int, versionID int) {
	key := []byte(fmt.Sprintf("%s:%d", NODE_VERSION_LIST, nodeID))
	existingListStr, err := db.Get(key)
	if err != nil { /* key not present */
		valstr, _ := json.Marshal([]int{versionID})
		db.Put(key, []byte(valstr))
	} else {
		var existingList []int
		json.Unmarshal(existingListStr, &existingList)

		/* defensive => make sure that new version is older than last version in the last otherwise ignore */
		if versionID > existingList[len(existingList) - 1] {
			existingList = append(existingList, versionID)
			newListStr, _ := json.Marshal(existingList)
			db.Put(key, newListStr)
		}
	}
}

func GetNodeVersions(nodeID int) []int {
	key := []byte(fmt.Sprintf("%s:%d", NODE_VERSION_LIST, nodeID))
	lstr, err := db.Get(key)
	if err != nil {
		return []int{}
	} else {
		var versionList []int
		json.Unmarshal(lstr, &versionList)
		P_out("%d versions=> %v", nodeID, versionList)
		return versionList
	}
}
