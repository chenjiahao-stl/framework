package netutil

import (
	"net"
	"strconv"
	"strings"
)

// GetLocalIP
// returns the local network IP address.
func GetLocalIP() string {
	// The address is meaningless, and it can be any address.
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// JoinHostPort
// combines host and port into a group of address.
func JoinHostPort(host string, port uint64) []string {
	var addrs []string
	for _, v := range strings.Split(host, ",") {
		_, _, err := net.SplitHostPort(v)
		if e, ok := err.(*net.AddrError); ok && e.Err == "missing port in address" {
			addrs = append(addrs, net.JoinHostPort(v, strconv.FormatUint(port, 10)))
		} else if err == nil {
			addrs = append(addrs, v)
		}
	}
	return addrs
}
