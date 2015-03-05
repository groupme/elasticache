package elasticache

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var Debug = false

type ConfigPoller struct {
	Endpoint string
	Timeout  time.Duration
}

type ClusterConfig struct {
	Version int
	Nodes   []Node
}

type Node struct {
	Host string
	IP   string
	Port int
}

var (
	resultPrefixConfig = []byte("CONFIG")
	resultEnd          = []byte("END\r\n")
)

func (c ConfigPoller) Get() (ClusterConfig, error) {
	if c.Timeout == 0 {
		c.Timeout = time.Second
	}

	nc, err := net.DialTimeout("tcp", c.Endpoint, c.Timeout)
	if err != nil {
		return ClusterConfig{}, err
	}
	defer nc.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc))

	// request
	_, err = rw.Write([]byte("config get cluster\r\n"))
	if err != nil {
		return ClusterConfig{}, err
	}
	if err := rw.Flush(); err != nil {
		return ClusterConfig{}, err
	}

	// response
	var config ClusterConfig
	err = c.parseResponse(rw.Reader, &config)
	if err != nil {
		return ClusterConfig{}, err
	}

	return config, nil
}

func (c ConfigPoller) parseResponse(r *bufio.Reader, cfg *ClusterConfig) error {
	// config
	configLine, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(configLine, resultPrefixConfig) {
		return fmt.Errorf("expected prefix", resultPrefixConfig)
	}

	// version
	versionLine, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	cfg.Version, err = strconv.Atoi(strings.Trim(versionLine, "\r\n"))
	if err != nil {
		return err
	}

	// nodes
	nodeLine, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	nodeLine = strings.Trim(nodeLine, "\n")
	for _, ns := range strings.Split(nodeLine, " ") {
		parts := strings.Split(ns, "|")
		if len(parts) != 3 {
			return fmt.Errorf("expected 3 parts in %#v", parts)
		}
		var node Node
		node.Host = parts[0]
		node.IP = parts[1]
		node.Port, err = strconv.Atoi(parts[2])
		if err != nil {
			return err
		}
		cfg.Nodes = append(cfg.Nodes, node)
	}
	return nil
}
