package elasticache

import (
	"fmt"
	"net"
	"reflect"
	"testing"
)

func TestPackage(t *testing.T) {
	// FakeServer
	endpoint := "127.0.0.1:2345"
	var server FakeServer
	defer server.Close()
	server.Listen(endpoint)

	// ConfigPoller
	var poller ConfigPoller
	poller.Endpoint = endpoint

	config, err := poller.Get()
	if err != nil {
		t.Error(err)
	}

	if config.Version != 12 {
		t.Error("want 12")
		t.Error("got", config.Version)
	}

	want := []Node{
		Node{
			Host: "myCluster.pc4ldq.0001.use1.cache.amazonaws.com",
			IP:   "10.82.235.120",
			Port: 11211,
		},
		Node{
			Host: "myCluster.pc4ldq.0002.use1.cache.amazonaws.com",
			IP:   "10.80.249.27",
			Port: 11211,
		},
	}
	if !reflect.DeepEqual(want, config.Nodes) {
		t.Error("want", want)
		t.Error("got ", config.Nodes)
	}

	// Servers
	servers, err := Servers(endpoint)
	if err != nil {
		t.Error(err)
	}
	wantServers := []string{
		"10.82.235.120:11211",
		"10.80.249.27:11211",
	}
	if !reflect.DeepEqual(wantServers, servers) {
		t.Error("want", wantServers)
		t.Error("got ", servers)
	}

}

type FakeServer struct {
	ln net.Listener
}

var FakeServerResponse = []byte(
	"CONFIG cluster 0 147\r\n" +
		"12\r\n" +
		"myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n\r\n" +
		"END\r\n",
)

func (s FakeServer) Listen(addr string) error {
	var err error
	s.ln, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			_, err = conn.Write(FakeServerResponse)
			if err != nil {
				fmt.Println("conn.Write error", err)
			}
		}
	}(s.ln)

	return nil
}

func (s FakeServer) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}
