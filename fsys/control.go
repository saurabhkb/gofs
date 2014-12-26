package fsys

import (
	"fmt"
	"encoding/json"
	"os"
	"p4/storage"
	"p4/util"
	"math/rand"
)

/* namespaces for the data store */
const DATA_KEY string = "DATA_KEY"
const NODE_VERSION_KEY string = "NDV"
const NODE_VERSION_LIST string = "NDVL"
const STATE_KEY = "STATE"

var hash2mynode map[string]MyNode


/* a structure to store State */
type STATE struct {
	Root_version_bootstrap string	/* latest root version */
	NextInode uint64			/* next available inode number */
	NextNId int					/* next available node id */
}

/* important variables */
var State STATE

func LoadState() {
	statestr, _ := storage.Get([]byte(STATE_KEY))
	json.Unmarshal(statestr, &State)
	State.NextInode++
}

/* places an fs object in the memory pointed to by the argument */
func LoadFS(fs *MyFS) {

	hash2mynode = make(map[string]MyNode)

	/* State.Root_version_bootstrap is Vid of root of filesystem */
	str := fmt.Sprintf("%s:%s", NODE_VERSION_KEY, State.Root_version_bootstrap)
	util.P_out("key: %s", str)
	rootdirstr, err := storage.Get([]byte(str))
	if err != nil {
		util.P_out("creating filesystem!")
		/* key most likely doesn't exist */
		fs.RootDir = new(MyNode)
		fs.RootDir.Init("/", os.ModeDir | 0755, nil)
		fs.RootDir.Vid = GenerateVersionId(fs.RootDir)
		updateAncestors(fs.RootDir)
	} else {
		util.P_out("loading filesystem!")
		json.Unmarshal(rootdirstr, &fs.RootDir)
	}
	AssertExpanded(fs.RootDir)

	rand.Seed(int64(Pid))
}

/* self explanatory */
func GetAvailableInode() uint64 {
	return uint64(rand.Int63())
	State.NextInode++
	return State.NextInode
}


func GetAvailableUid() int {
	State.NextNId++
	return State.NextNId
}


/* "saves" a node corresponding to a new version -> basically sets its dirty flag */
func SaveNodeVersion(node *MyNode) bool {
	if node.parent == nil { /* root. update the bootstrap value */
		State.Root_version_bootstrap = node.Vid
	}
	node.dirty = true
	return true
}

/* load node */
func LoadNodeVersion(Vid string, lastWriter int) (*MyNode, error) {
	var x MyNode
	/* first check if I have it */
	nodestr, err := storage.Get([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, Vid)))
	if err != nil {
		/* if I don't and I'm the last writer, panic. Otherwise fetch it */
		if lastWriter == GetMyPid() {
			panic("I wrote last but I still dont have the metadata! I have an idea: lets abort!")
		} else {
			dest := util.GetEndpointFromPid(lastWriter)
			completeNode := PerformMetaDataRequest(Vid, dest.RepTcpFormat())
			str, _ := json.Marshal(&completeNode)
			storage.Put([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, Vid)), str)
			return &completeNode, nil
		}
	}
	json.Unmarshal(nodestr, &x)
	return &x, err
}

/* load data */
func loadDataChunk(hash string, lastWriter int) ([]byte, error) {
	ret, err := storage.Get([]byte(fmt.Sprintf("%s:%v", DATA_KEY, hash)))
	if err != nil {
		if lastWriter == GetMyPid() {
			panic("I wrote last but I still dont have the data! I have an idea: lets abort!")
		} else {
			dest := util.GetEndpointFromPid(lastWriter)
			dataSlices := PerformDataRequest(hash, dest.RepTcpFormat())
			storage.Put([]byte(fmt.Sprintf("%s:%v", DATA_KEY, hash)), dataSlices)
			return dataSlices, nil
		}
	}
	return ret, nil
}


/* expands this node and loads its children (if it hasn't already been done) */
func AssertExpanded(node *MyNode) {
	if !node.expanded {
		/* dirty, children, data */
		//node.dirty = false;
		if(node.Attrib.Mode.IsDir()) {
			/* children, not data */
			util.P_out("%s children are:", node.Name)
			node.children = make(map[string]*MyNode)
			for name, childStubs := range node.Kids {
				node.children[name], _ = LoadNodeVersion(childStubs.Vid, childStubs.LastWriter)
				node.children[name].parent = node
				util.P_out("%v", node.children[name])
			}
		} else {
			/* data, not children */
			node.data = []byte{}
			for i := 0; i < len(node.DataBlocks); i++ {
				loadedData, _ := loadDataChunk(node.DataBlocks[i], node.LastWriter)
				for j := 0; j < len(loadedData); j++ {
					node.data = append(node.data, loadedData[j])
				}
			}
		}
		node.expanded = true
	}
}


/* versioning */
func RegisterNodeVersion(nodeID int, versionID string) {
	key := []byte(fmt.Sprintf("%s:%d", NODE_VERSION_LIST, nodeID))
	existingListStr, err := storage.Get(key)
	if err != nil { /* key not present */
		valstr, _ := json.Marshal([]string{versionID})
		storage.Put(key, []byte(valstr))
	} else {
		var existingList []string
		json.Unmarshal(existingListStr, &existingList)

		existingList = append(existingList, versionID)
		newListStr, _ := json.Marshal(existingList)
		storage.Put(key, newListStr)
	}
}

func GetNodeVersions(nodeID int) []string {
	key := []byte(fmt.Sprintf("%s:%d", NODE_VERSION_LIST, nodeID))
	lstr, err := storage.Get(key)
	if err != nil {
		return []string{}
	} else {
		var versionList []string
		json.Unmarshal(lstr, &versionList)
		return versionList
	}
}




func Merge(versions map[string]MyNode, fs *MyFS) {
	/* create a map from Vid (hash) to corresponding node */
	for k := range versions {
		hash2mynode[k] = versions[k]
		temp := versions[k]
		nodestr, _ := json.Marshal(&temp)
		storage.Put([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, versions[k].Vid)), nodestr)
	}
	general, _ := fs.Root()
	r := general.(*MyNode)

	val, found := hash2mynode[r.Vid]
	if found {
		State.Root_version_bootstrap = val.Vid
		statestr, _ := json.Marshal(State)
		storage.Put([]byte(STATE_KEY), statestr)
	}
}
