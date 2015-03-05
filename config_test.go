package elasticache

import (
	"fmt"
	"net"
	"reflect"
	"testing"
)

func TestClusterConfig(t *testing.T) {
	addr := "127.0.0.1:2345"

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			_, err = conn.Write(fakeConfigServerResponse)
			if err != nil {
				t.Error("conn.Write error", err)
			}
		}
	}(listener)

	var poller ConfigPoller
	poller.Endpoint = addr

	config, err := poller.Get()
	if err != nil {
		t.Error(err)
	}

	if config.Version != 12 {
		t.Error("want 12")
		t.Error("err", config.Version)
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
}

type fakeConfigServer struct {
	ln net.Listener
}

var fakeConfigServerResponse = []byte(
	"CONFIG cluster 0 147\r\n" +
		"12\r\n" +
		"myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n\r\n" +
		"END\r\n",
)

func (s fakeConfigServer) Listen(addr string) error {
	var err error
	s.ln, err = net.Listen("tcp", addr)
	return err
}

func (s fakeConfigServer) Accept() error {
	conn, err := s.ln.Accept()
	if err != nil {
		fmt.Println("accept error", err)
		return err
	}
	fmt.Println("accept ok")

	var input []byte
	n, err := conn.Read(input)
	if err != nil {
		fmt.Println("read error", err)
	}
	fmt.Printf("read %d bytes", n)

	n, err = conn.Write(fakeConfigServerResponse)
	if err != nil {
		fmt.Println("write error", err)
		return err
	}
	fmt.Printf("wrote %n bytes", n)
	return nil
}

func (s fakeConfigServer) Close() error {
	return s.ln.Close()
}
