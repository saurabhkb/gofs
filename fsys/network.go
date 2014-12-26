package fsys

import (
	"github.com/pebbe/zmq4"
	"fmt"
	"encoding/json"
	"p4/util"
	"p4/lock"
	"p4/storage"
)

const (
	UPDATE_BROADCAST = iota
	DATA_REQUEST
	DATA_REPLY
	METADATA_REQUEST
	METADATA_REPLY
	INVALID
)


type Message struct {
	Type int
	From int

	Versions map[string]MyNode

	RequestedHash string
	ReturnedData []byte

	RequestedMetadata string
	ReturnedMetadata MyNode
}

func (m Message) String() string {
	s := "Message Versions\n"
	for i := range m.Versions {
		temp := m.Versions[i]
		s += fmt.Sprintf("%s\n", &temp)
	}
	return s
}

var ServerName string
var Pid	int
var MountPoint string
var DbPath string
var HostAddress util.Endpoint

var PubSocket *zmq4.Socket
var SubSocket *zmq4.Socket
var RepSocket *zmq4.Socket
var ReqSocket *zmq4.Socket


func SetMyPid(pid int) {
	Pid = pid
}

func GetMyPid() int {
	return Pid
}

func Print() string {
	return fmt.Sprintf("[ServerName=%s, Pid=%d, MountPoint=%s, DbPath=%s, Endpoint=%v]", ServerName, Pid, MountPoint, DbPath, HostAddress.Tcpformat())
}

func Init(sname string, pid int, mountpoint string, dbpath string, hostaddr util.Endpoint) {
	ServerName = sname
	Pid = pid
	MountPoint = mountpoint
	DbPath = dbpath
	HostAddress = hostaddr
}

/* start publish socket */
func StartPub() error {
	var err error
	PubSocket, err = zmq4.NewSocket(zmq4.PUB)
	if err == nil {
		PubSocket.Bind(HostAddress.Tcpformat())
		util.P_out("PUB started on %s", HostAddress.Tcpformat())
		return nil
	} else {
		util.P_out("err: %v", err)
		return err
	}
}

/* start reply socket */
func StartRep() error {
	go func() error {
		var err error
		RepSocket, err = zmq4.NewSocket(zmq4.REP)
		if err == nil {
			RepSocket.Bind(HostAddress.RepTcpFormat())
			util.P_out("REP started on %s", HostAddress.RepTcpFormat())

			for true {
				req, _ := RepSocket.Recv(0)

				var msg Message
				json.Unmarshal([]byte(req), &msg)

				util.P_out("RECEIVED DATA REQUEST! %v", msg.RequestedHash)

				var tosend Message

				if msg.Type == DATA_REQUEST {
					tosend.Type = DATA_REPLY
					ret, e := storage.Get([]byte(fmt.Sprintf("%s:%v", DATA_KEY, msg.RequestedHash))) /* ignore error for now */
					util.P_out("error: %v", e)
					tosend.ReturnedData = ret
				} else if msg.Type == METADATA_REQUEST {
					tosend.Type = METADATA_REPLY
					ret, _ := storage.Get([]byte(fmt.Sprintf("%s:%v", NODE_VERSION_KEY, msg.RequestedMetadata)))
					tosend.ReturnedMetadata = MyNode{}
					json.Unmarshal(ret, &tosend.ReturnedMetadata)
				} else {
					tosend.Type = INVALID
				}
				str, _ := json.Marshal(tosend)
				RepSocket.Send(string(str[:]), 0)
			}

			return nil
		} else {
			return err
		}
	}()
	return nil
}

func StartReq() error {
	var err error
	ReqSocket, err = zmq4.NewSocket(zmq4.REQ)
	util.P_out("REQ started on %s", HostAddress.RepTcpFormat())
	return err
}

/* start subscribe socket */
func StartSub(endpointList []util.Endpoint, fs *MyFS) error {
	go func() error {
		var err error
		SubSocket, err = zmq4.NewSocket(zmq4.SUB)
		SubSocket.SetSubscribe("")
		if err == nil {
			for i := 0; i < len(endpointList); i++ {
				/* don't subscribe to self */
				if HostAddress != endpointList[i] {
					util.P_out("connecting to: %s", endpointList[i].Tcpformat())
					SubSocket.Connect(endpointList[i].Tcpformat())
				}
			}
			util.P_out("SUB started")
			for true {
				s, _ := SubSocket.Recv(0)
				var msg Message
				json.Unmarshal([]byte(s), &msg)
				util.P_out("received!: %v", msg)

				if msg.Type == UPDATE_BROADCAST {
					/* race for lock */
					lock.LOCK.Lock()
					Merge(msg.Versions, fs)
					lock.LOCK.Unlock()
				}
			}
			return nil
		} else {
			util.P_out("err: %v", err)
			return err
		}
	}()
	return nil
}


func SendUpdateMessage(versions map[string]MyNode) {
	m := Message{}
	m.Type = UPDATE_BROADCAST
	m.From = Pid
	m.Versions = versions
	/* publish */
	str, _ := json.Marshal(m)
	PubSocket.Send(string(str[:]), zmq4.DONTWAIT)
}


func PerformDataRequest(hash string, destination string) []byte {
	m := Message{}
	m.Type = DATA_REQUEST
	m.From = Pid
	m.RequestedHash = hash

	str, _ := json.Marshal(m)
	util.P_out("requesting data!: %s", string(str[:]))

	ReqSocket.Connect(destination)
	ReqSocket.Send(string(str[:]), 0)


	reply, _ := ReqSocket.Recv(0)
	var msg Message
	json.Unmarshal([]byte(reply), &msg)

	if msg.Type == DATA_REPLY {
		util.P_out("received 1 data slice")
		return msg.ReturnedData
	} else {
		util.P_out("SCREW UP!")
		return []byte{}
	}
}


func PerformMetaDataRequest(Vid string, destination string) MyNode {
	m := Message{}
	m.Type = METADATA_REQUEST
	m.From = Pid
	m.RequestedMetadata = Vid

	str, _ := json.Marshal(m)
	util.P_out("requesting metadata!: %s", string(str[:]))

	ReqSocket.Connect(destination)
	ReqSocket.Send(string(str[:]), 0)

	reply, _ := ReqSocket.Recv(0)
	var msg Message
	json.Unmarshal([]byte(reply), &msg)

	if msg.Type == METADATA_REPLY {
		util.P_out("received 1 metadata!")
		return msg.ReturnedMetadata
	} else {
		util.P_out("SCREW UP!")
		return MyNode{}
	}
}

func Close() {
	PubSocket.Close()
	SubSocket.Close()
	RepSocket.Close()
}
