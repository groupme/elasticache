package elasticache

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Servers returns a list of cache node URLs for use with a memcache client
func Servers(configEndpoint string) ([]string, error) {
	var poller ConfigPoller
	poller.Endpoint = configEndpoint

	config, err := poller.Get()
	if err != nil {
		return []string{}, err
	}

	var servers []string
	for _, n := range config.Nodes {
		servers = append(servers, n.URL())
	}
	return servers, nil
}

// ConfigPoller contacts an ElastiCache configuration endpoint for cluster state
type ConfigPoller struct {
	Endpoint string
	Timeout  time.Duration
}

// ClusterConfig describes cluster state
type ClusterConfig struct {
	Version int
	Nodes   []Node
}

// Node is an ElastiCache machine
type Node struct {
	Host string
	IP   string
	Port int
}

func (n Node) URL() string {
	return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

var (
	configPrefix = []byte("CONFIG")
	configEnd    = []byte("END\r\n")
)

// Get queries the configuration endpoint for cluster state
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

	// flush
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
	line, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(line, configPrefix) {
		return fmt.Errorf("expected %v got %s", configPrefix, line)
	}

	// version
	line, err = r.ReadSlice('\n')
	if err != nil {
		return err
	}
	line = bytes.Trim(line, "\r\n")
	cfg.Version, err = strconv.Atoi(string(line))
	if err != nil {
		return err
	}

	// nodes
	line, err = r.ReadSlice('\n')
	if err != nil {
		return err
	}
	line = bytes.Trim(line, "\n")
	for _, ns := range bytes.Split(line, []byte(" ")) {
		parts := bytes.Split(ns, []byte("|"))
		if len(parts) != 3 {
			return fmt.Errorf("expected 3 parts in %#v", parts)
		}
		var node Node
		node.Host = string(parts[0])
		node.IP = string(parts[1])
		node.Port, err = strconv.Atoi(string(parts[2]))
		if err != nil {
			return err
		}
		cfg.Nodes = append(cfg.Nodes, node)
	}

	// burn extra newline
	r.ReadSlice('\n')

	// end
	line, err = r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if !bytes.Equal(line, configEnd) {
		return fmt.Errorf("expected %s got %s", configEnd, line)
	}
	return nil
}
