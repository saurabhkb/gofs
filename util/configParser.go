package util

import (
	"os"
	"io"
	"encoding/csv"
	"strconv"
	"errors"
	"fmt"
	"net"
	"log"
)

type ConfigFileEntry struct {
	Name string
	Pid int
	MountPoint string
	Dbpath string
	HostAddress Endpoint
}

var configFileStructure []ConfigFileEntry

func SetConfigFile(configfile string) {
	configFileStructure = make([]ConfigFileEntry, 0)
	file, fileErr := os.Open("config.txt")	/* assume it succeeds for now */
	if fileErr != nil {
		log.Fatal("Could not open file: config.txt")
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','

	lineNumber := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if len(record) != 6 {
			if record[0][0] != '#' {
				panic(fmt.Sprintf("invalid configuration at line %d:%s", lineNumber, record))
			}
		} else {
			pid, _ := strconv.Atoi(record[1])
			port, _ := strconv.Atoi(record[5])
			c := ConfigFileEntry{record[0], pid, record[2], record[3], Endpoint{record[4], port}}
			configFileStructure = append(configFileStructure, c)
		}
		lineNumber += 1
	}
}

func ReadAllEndpoints() []Endpoint {
	endpoints := make([]Endpoint, 0)
	for i := 0; i < len(configFileStructure); i++ {
		e := configFileStructure[i].HostAddress
		endpoints = append(endpoints, e)
	}
	return endpoints
}

func GetConfigDetailsFromName(ServerName string) (error, string, int, string, string, Endpoint) {
	var searchBy string
	var searchString string

	if ServerName == "auto" {
		searchBy = "ip"
		var neterr error
		searchString, neterr = getIP()
		if neterr != nil {
			panic("no network ip address detected!")
		}
	} else {
		searchBy = "name"
		searchString = ServerName
	}

	for i := 0; i < len(configFileStructure); i++ {
		if (searchBy == "name" && configFileStructure[i].Name == searchString) || (searchBy == "ip" && configFileStructure[i].HostAddress.Ipaddr == searchString) {
				return nil, configFileStructure[i].Name, configFileStructure[i].Pid, configFileStructure[i].MountPoint, configFileStructure[i].Dbpath, configFileStructure[i].HostAddress
		}
	}
	return errors.New(fmt.Sprintf("entry not found for given %s: %s", searchBy, searchString)), "", 0, "", "", Endpoint{"", 0}
}

func GetEndpointFromPid(pid int) Endpoint {
	for i := 0; i < len(configFileStructure); i++ {
		if pid == configFileStructure[i].Pid {
			return configFileStructure[i].HostAddress
		}
	}
	return Endpoint{}
}

func getIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("no network connection detected")
}
