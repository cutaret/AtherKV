package tcp

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cutaret/aetherkv/sim"
)

// Transport implements sim.Transport over real length-prefixed TCP connections.
type Transport struct {
	mu       sync.RWMutex
	nodeID   string
	listener net.Listener
	peers    map[string]string // nodeID -> host:port
	handler  func(sim.Message)
	closed   bool
}

func NewTransport(nodeID string, listenAddr string, handler func(sim.Message)) (*Transport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	t := &Transport{
		nodeID:   nodeID,
		listener: ln,
		peers:    make(map[string]string),
		handler:  handler,
	}
	go t.acceptLoop()
	return t, nil
}

func (t *Transport) Addr() string {
	return t.listener.Addr().String()
}

func (t *Transport) SetPeers(peers map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range peers {
		t.peers[k] = v
	}
}

func (t *Transport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			return
		}
		go t.handleConn(conn)
	}
}

func (t *Transport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen == 0 || msgLen > 32*1024*1024 {
			return
		}
		payload := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}
		var msg sim.Message
		if err := json.Unmarshal(payload, &msg); err != nil {
			return
		}
		t.mu.RLock()
		closed := t.closed
		h := t.handler
		t.mu.RUnlock()
		if !closed && h != nil {
			h(msg)
		}
	}
}

// Send frames and transmits msg over TCP to msg.To.
func (t *Transport) Send(msg sim.Message) {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return
	}
	addr, ok := t.peers[msg.To]
	t.mu.RUnlock()
	if !ok {
		return
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return
	}

	go func() {
		conn, err := net.DialTimeout("tcp", addr, 150*time.Millisecond)
		if err != nil {
			return
		}
		defer conn.Close()
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(raw)))
		_, _ = conn.Write(lenBuf[:])
		_, _ = conn.Write(raw)
	}()
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return t.listener.Close()
}
