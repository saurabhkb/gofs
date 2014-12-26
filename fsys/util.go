package fsys

import (
	"encoding/hex"
	"encoding/json"
	"crypto/sha1"
	"time"
	"p4/util"
)


/* utility function */
func updateAncestors(node *MyNode) {
	current := node
	for current != nil {
		current.Vid = GenerateVersionId(current)	/* update vid */
		current.LastWriter = GetMyPid()				/* update last writer */
		if current.parent != nil {
			current.parent.Kids[current.Name] = &Stub{}
			current.parent.Kids[current.Name].NodeID = current.NodeID
			current.parent.Kids[current.Name].Vid = current.Vid
			current.parent.Kids[current.Name].Name = current.Name
			current.parent.Kids[current.Name].Attrib = current.Attrib
			current.parent.Kids[current.Name].LastWriter = GetMyPid()
		}
		SaveNodeVersion(current)
		util.P_out("%s dirty = %v", current.Name, current.dirty)
		dirtyNodesList[node.Vid] = *node
		current = current.parent
	}
}

func GenerateVersionId(node *MyNode) string {
	path := ""
	current := node
	for current != nil {
		path += current.Name
		path += "/"
		current = current.parent
	}
	util.P_out("version for %s = %s", node.Name, path)
	return path
	str, _ := json.Marshal(*node)
	util.P_out("json=%s", str)
	hash := sha1.Sum(str)
	return hex.EncodeToString(hash[:])
}

func parseTime(date string) time.Time {
	t, boolerr := peteTime(date)
	var finalTime time.Time
	if boolerr {
		/* its of the form: foo@-1m */
		duration, _ := time.ParseDuration(date)
		finalTime = time.Now().Add(duration)
		util.P_out("duration based final time: %v", finalTime)
	} else {
		/* its of the form: foo@2014-09-18 9:20 */
		finalTime = t
	}
	return finalTime
}

func peteTime(s string) (time.Time, bool) {
	timeFormats := []string{
		"2006-1-2 15:04:05",
		"2006-1-2 15:04",
		"2006-1-2",
		"1-2-2006 15:04:05",
		"1-2-2006 15:04",
		"1-6-2006",
		"2006/1/2 15:04:05",
		"2006/1/2 15:04",
		"2006/1/2",
		"1/2/2006 15:04:05",
		"1/2/2006 15:04",
		"1/2/2006",
	}
	loc, _ := time.LoadLocation("Local")

	for _,v := range timeFormats {
		if tm, terr := time.ParseInLocation(v, s, loc); terr == nil {
			return tm, false
		}
	}
	return time.Time{}, true
}

func isAllowed(node *MyNode, operation string) bool {
	switch(operation) {
		case "w": {
			current := node.parent
			for current != nil {
				if current.archive {
					return false
				}
				current = current.parent
			}
			return true
			if (node.Attrib.Mode.Perm() & 0x00000092) <= 0 {
				util.P_out("%s => %v", node.Name, node.Attrib.Mode.Perm())
				util.P_out(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> WRITE PERM DENIED!")
			}
			return (node.Attrib.Mode.Perm() & 0x00000092) > 0
		}
		case "r": {
			return true
			return node.Attrib.Mode.Perm() & 0x00000124 > 0
		}
	}
	return false
}
