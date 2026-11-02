package udpfsd_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/pcm720/udpfsd/internal/fs"
	"github.com/pcm720/udpfsd/internal/udpfsd"
	"github.com/pcm720/udpfsd/udpfs"
	"github.com/pcm720/udpfsd/udprdma"
)

var loopback = net.IPv4(127, 0, 0, 1)

// console plays the PS2 side of UDPRDMA and UDPFS against a server on the loopback interface
type console struct {
	t    *testing.T
	conn *net.UDPConn
	disc *net.UDPAddr // where the server listens for DISCOVERY
	data *net.UDPAddr // where the server sent INFORM from
	seq  uint16       // sequence number of the next request
	rseq uint16       // sequence number of the next reply
}

// startServer starts a server for a directory holding OPL.ELF and returns a console for it
func startServer(t *testing.T) *console {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "OPL.ELF"), []byte("ELF"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend, err := fs.NewBackend(fs.WithFSRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	// The discovery port can't be 0, so take one that is free
	l, err := net.ListenUDP("udp4", &net.UDPAddr{IP: loopback})
	if err != nil {
		t.Fatal(err)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()

	srv, err := udpfsd.New(udpfsd.WithFS(backend), udpfsd.WithDiscoveryPort(port), udpfsd.WithDataIP(loopback.String()))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: loopback})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &console{t: t, conn: conn, disc: &net.UDPAddr{IP: loopback, Port: port}}
}

// read returns the next packet from the server and its source address
func (c *console) read(what string) ([]byte, *net.UDPAddr) {
	c.t.Helper()
	buf := make([]byte, 2048)
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, from, err := c.conn.ReadFromUDP(buf)
	if err != nil {
		c.t.Fatalf("no %s from the server: %v", what, err)
	}
	return buf[:n], from
}

func (c *console) send(pkt []byte, to *net.UDPAddr) {
	c.t.Helper()
	if _, err := c.conn.WriteToUDP(pkt, to); err != nil {
		c.t.Fatal(err)
	}
}

// discover sends DISCOVERY and waits for INFORM
func (c *console) discover() {
	c.t.Helper()
	pkt := make([]byte, 6)
	udprdma.Header{PacketType: udprdma.PacketDiscovery}.Pack(pkt)
	udprdma.DiscHeader{ServiceID: uint16(udprdma.Service_UDPFS)}.Pack(pkt[2:])
	c.send(pkt, c.disc)
	for {
		pkt, from := c.read("INFORM")
		if hdr, err := udprdma.UnpackHeader(pkt); err == nil && hdr.PacketType == udprdma.PacketInform {
			c.data = from
			return
		}
	}
}

// reset starts a new session the way the IRX does when it is loaded again,
// after an IOP reboot: it discovers the server, and both sides start from
// sequence number 0
func (c *console) reset() {
	c.t.Helper()
	c.discover()
	c.seq, c.rseq = 0, 0
}

// request sends req in the next DATA packet and returns the reply of type want,
// which it checks for the next sequence number and acknowledges
func (c *console) request(req []byte, want udpfs.MsgType) []byte {
	c.t.Helper()
	padded := (len(req) + 3) &^ 3
	pkt := make([]byte, 6+padded)
	udprdma.Header{PacketType: udprdma.PacketData, SeqNr: c.seq}.Pack(pkt)
	udprdma.DataHeader{DataByteCount: uint16(padded)}.Pack(pkt[2:])
	copy(pkt[6:], req)
	c.seq = (c.seq + 1) & 0xFFF
	c.send(pkt, c.data)

	for {
		reply, _ := c.read("reply to " + udpfs.MsgType(req[0]).String())
		hdr, err := udprdma.UnpackHeader(reply)
		if err != nil || hdr.PacketType != udprdma.PacketData || len(reply) <= 6 {
			continue // INFORM, or the ACK of the request
		}
		if hdr.SeqNr != c.rseq {
			c.t.Fatalf("reply to %v with sequence number %d, want %d", udpfs.MsgType(req[0]), hdr.SeqNr, c.rseq)
		}
		c.rseq = (c.rseq + 1) & 0xFFF
		ack := make([]byte, 6)
		udprdma.Header{PacketType: udprdma.PacketData, SeqNr: c.seq}.Pack(ack)
		udprdma.DataHeader{SeqNrAck: hdr.SeqNr, Flags: uint8(udprdma.DataFlagACK)}.Pack(ack[2:])
		c.send(ack, c.data)
		if r := reply[6:]; len(r) >= 8 && udpfs.MsgType(r[0]) == want {
			return bytes.Clone(r)
		}
	}
}

// open opens a file for reading and returns the handle or error from OPEN_REPLY
func (c *console) open(path string) int32 {
	c.t.Helper()
	req := make([]byte, 8)
	req[0] = byte(udpfs.MsgOpenReq)
	binary.LittleEndian.PutUint16(req[2:], 0x0001) // FIO_O_RDONLY
	reply := c.request(append(append(req, path...), 0), udpfs.MsgOpenReply)
	return int32(binary.LittleEndian.Uint32(reply[4:8]))
}

// getstat sends GETSTAT, which is answered with or without the fix
func (c *console) getstat(path string) {
	c.t.Helper()
	req := []byte{byte(udpfs.MsgGetstatReq), 0, 0, 0}
	c.request(append(append(req, path...), 0), udpfs.MsgGetstatReply)
}

// A console reset after a single request starts the new session with the
// sequence number the server took for a retransmission of that request
func TestResetAfterOneRequest(t *testing.T) {
	c := startServer(t)
	c.reset()
	h := c.open("OPL.ELF")
	if h <= 0 {
		t.Fatalf("OPEN: %d", h)
	}

	c.reset()
	if h2 := c.open("OPL.ELF"); h2 != h {
		t.Fatalf("OPEN after the console was reset: handle %d, want the handle %d left open by the reset", h2, h)
	}
}

// A console reset right after its sequence number wrapped around starts the new
// session with the sequence number the server expects next in the old one
func TestResetAfterSequenceWrap(t *testing.T) {
	c := startServer(t)
	c.reset()
	h := c.open("OPL.ELF")
	if h <= 0 {
		t.Fatalf("OPEN: %d", h)
	}
	for c.seq != 0 {
		c.getstat("OPL.ELF")
	}

	c.reset()
	if h2 := c.open("OPL.ELF"); h2 != h {
		t.Fatalf("OPEN after the console was reset: handle %d, want the handle %d left open by the reset", h2, h)
	}
}

// A DISCOVERY during a session, such as one the console sent again because the
// INFORM was late, doesn't end it, either at the next request or when the
// sequence number wraps around to 0
func TestDiscoveryDuringSession(t *testing.T) {
	c := startServer(t)
	c.reset()
	var handles []int32
	newHandle := func(when string) {
		t.Helper()
		h := c.open("OPL.ELF")
		if h <= 0 || slices.Contains(handles, h) {
			t.Fatalf("OPEN %s: %d, want a handle other than %v", when, h, handles)
		}
		handles = append(handles, h)
	}
	newHandle("before the DISCOVERY")

	c.discover()
	newHandle("after the DISCOVERY")
	for c.seq != 0 {
		c.getstat("OPL.ELF")
	}
	newHandle("after the sequence number wrapped around")
}
