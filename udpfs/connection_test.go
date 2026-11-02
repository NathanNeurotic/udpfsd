package udpfs_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/pcm720/udpfsd/internal/fs"
	"github.com/pcm720/udpfsd/udpfs"
	"github.com/pcm720/udpfsd/udprdma"
)

// Open flags as the PS2 sends them in OPEN_REQ
const (
	fioRdonly = 0x0001
	fioWronly = 0x0002
	fioCreat  = 0x0200
	fioTrunc  = 0x0400
)

// peer plays the PS2 side of a connection to a Backend serving a temporary
// directory. Requests go through the UDPRDMA session and then to
// Connection.HandlePayload, as in the server's data handler.
type peer struct {
	t    *testing.T
	root string
	addr *net.UDPAddr
	sess *udprdma.Session
	conn *udpfs.Connection
	seq  uint16 // UDPRDMA sequence number of the next request

	mu      sync.Mutex
	replies [][]byte
}

func newPeer(t *testing.T) *peer {
	t.Helper()
	p := &peer{
		t:    t,
		root: t.TempDir(),
		addr: &net.UDPAddr{IP: net.IPv4(192, 168, 0, 10), Port: udprdma.UDPFSPort},
	}
	backend, err := fs.NewBackend(fs.WithFSRoot(p.root))
	if err != nil {
		t.Fatal(err)
	}
	p.sess = udprdma.NewSession(*p.addr, p.receive, nil)
	p.conn = udpfs.NewConnection(p.sess, backend, false, false)
	t.Cleanup(p.conn.Close)
	return p
}

// receive collects the UDPFS payload of every packet the server sends
func (p *peer) receive(_ *net.UDPAddr, pkt []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replies = append(p.replies, bytes.Clone(pkt[6:])) // skip the UDPRDMA and DATA headers
}

// reboot makes the peer start again from sequence number 0, as the IRX does
// after an IOP reboot. The server resets the session on the next request,
// unless it expects sequence number 1: then it takes the request for a
// retransmission, so send at least two requests before calling this.
func (p *peer) reboot() {
	p.seq = 0
}

// request sends req and returns the last reply of type want
func (p *peer) request(req []byte, want udpfs.MsgType) []byte {
	p.t.Helper()
	padded := (len(req) + 3) &^ 3
	pkt := make([]byte, 6+padded)
	udprdma.Header{PacketType: udprdma.PacketData, SeqNr: p.seq}.Pack(pkt)
	udprdma.DataHeader{DataByteCount: uint16(padded)}.Pack(pkt[2:])
	copy(pkt[6:], req)
	p.seq = (p.seq + 1) & 0xFFF

	p.mu.Lock()
	p.replies = nil
	p.mu.Unlock()

	payload, err := p.sess.ProcessDataPacket(pkt)
	if err != nil || payload == nil {
		p.t.Fatalf("%v request not accepted by the session: %v", udpfs.MsgType(req[0]), err)
	}
	p.conn.HandlePayload(p.addr, payload)

	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.replies) - 1; i >= 0; i-- {
		if r := p.replies[i]; len(r) >= 8 && udpfs.MsgType(r[0]) == want {
			return r
		}
	}
	p.t.Fatalf("no reply 0x%02x to %v request", uint8(want), udpfs.MsgType(req[0]))
	return nil
}

// result returns the handle or result field of a reply
func result(reply []byte) int32 {
	return int32(binary.LittleEndian.Uint32(reply[4:8]))
}

// withPath appends a null-terminated path, padded to 4 bytes, to a request header
func withPath(hdr []byte, path string) []byte {
	req := append(append(hdr, path...), 0)
	for len(req)%4 != 0 {
		req = append(req, 0)
	}
	return req
}

func (p *peer) open(path string, flags uint16) int32 {
	p.t.Helper()
	req := make([]byte, 8)
	req[0] = byte(udpfs.MsgOpenReq)
	binary.LittleEndian.PutUint16(req[2:], flags)
	return result(p.request(withPath(req, path), udpfs.MsgOpenReply))
}

func (p *peer) close(handle int32) int32 {
	p.t.Helper()
	req := make([]byte, 8)
	req[0] = byte(udpfs.MsgCloseReq)
	binary.LittleEndian.PutUint32(req[4:], uint32(handle))
	return result(p.request(req, udpfs.MsgCloseReply))
}

// write sends data, which must fit into one chunk, the way the IRX does:
// WRITE_REQ and the first WRITE_DATA header in one packet
func (p *peer) write(handle int32, data []byte) int32 {
	p.t.Helper()
	req := make([]byte, 20, 20+len(data))
	req[0] = byte(udpfs.MsgWriteReq)
	binary.LittleEndian.PutUint32(req[4:], uint32(handle))
	binary.LittleEndian.PutUint32(req[8:], uint32(len(data)))
	req[12] = byte(udpfs.MsgWriteData)
	binary.LittleEndian.PutUint16(req[16:], uint16(len(data))) // chunk_size of chunk 0
	binary.LittleEndian.PutUint16(req[18:], 1)                 // total_chunks
	return result(p.request(append(req, data...), udpfs.MsgWriteDone))
}

// read reads up to size bytes, which must fit into one reply packet
func (p *peer) read(handle int32, size uint32) []byte {
	p.t.Helper()
	req := make([]byte, 12)
	req[0] = byte(udpfs.MsgReadReq)
	binary.LittleEndian.PutUint32(req[4:], uint32(handle))
	binary.LittleEndian.PutUint32(req[8:], size)
	reply := p.request(req, udpfs.MsgResultReply)
	n := result(reply)
	if n < 0 {
		p.t.Fatalf("READ handle %d: %d", handle, n)
	}
	return reply[8 : 8+n]
}

// pathOp sends MKDIR, REMOVE or RMDIR and returns the result
func (p *peer) pathOp(msg udpfs.MsgType, path string) int32 {
	p.t.Helper()
	return result(p.request(withPath([]byte{byte(msg), 0, 0, 0}, path), udpfs.MsgResultReply))
}

// startWrite opens path for writing and writes data into it, leaving it open
func (p *peer) startWrite(path string, data []byte) int32 {
	p.t.Helper()
	h := p.open(path, fioWronly|fioCreat|fioTrunc)
	if h <= 0 {
		p.t.Fatalf("OPEN %s for writing: %d", path, h)
	}
	if n := p.write(h, data); n != int32(len(data)) {
		p.t.Fatalf("WRITE %s: %d", path, n)
	}
	return h
}

// writeFile writes a file with OPEN (O_WRONLY|O_CREAT|O_TRUNC), WRITE and CLOSE
func (p *peer) writeFile(path string, data []byte) {
	p.t.Helper()
	h := p.startWrite(path, data)
	if r := p.close(h); r != 0 {
		p.t.Fatalf("CLOSE %s: %d", path, r)
	}
}

func (p *peer) hostPath(path string) string {
	return filepath.Join(p.root, filepath.FromSlash(path))
}

// wantFile checks that path holds exactly want
func (p *peer) wantFile(path string, want []byte) {
	p.t.Helper()
	got, err := os.ReadFile(p.hostPath(path))
	if err != nil {
		p.t.Fatalf("after all requests succeeded: %v", err)
	}
	if !bytes.Equal(got, want) {
		p.t.Fatalf("%s holds %d bytes %s, want the %d bytes %s written last", path, len(got), abbrev(got), len(want), abbrev(want))
	}
}

// abbrev quotes the first and the last few bytes of b
func abbrev(b []byte) string {
	if len(b) <= 8 {
		return strconv.Quote(string(b))
	}
	return strconv.Quote(string(b[:4])) + "..." + strconv.Quote(string(b[len(b)-4:]))
}

// A console reset during a write, then the copy retried from the start
// (REMOVE, then OPEN with O_CREAT|O_TRUNC): the new data must end up in the
// file at the path, not in the removed one the old handle still points to.
func TestRewriteAfterResetAndRemove(t *testing.T) {
	p := newPeer(t)
	p.startWrite("GAMES/OPL.ELF", bytes.Repeat([]byte{'O'}, 100))
	p.reboot()

	if r := p.pathOp(udpfs.MsgRemoveReq, "GAMES/OPL.ELF"); r != 0 {
		t.Fatalf("REMOVE: %d", r)
	}
	data := bytes.Repeat([]byte{'N'}, 256)
	p.writeFile("GAMES/OPL.ELF", data)
	p.wantFile("GAMES/OPL.ELF", data)
}

// The same without the REMOVE: O_TRUNC must apply to the reopened file
func TestRewriteAfterResetTruncates(t *testing.T) {
	p := newPeer(t)
	p.startWrite("save.psu", bytes.Repeat([]byte{'O'}, 300))
	p.reboot()

	data := bytes.Repeat([]byte{'N'}, 100)
	p.writeFile("save.psu", data)
	p.wantFile("save.psu", data)
}

// A file the console had open for reading when it was reset can be rewritten
func TestRewriteAfterResetWhileOpenForReading(t *testing.T) {
	p := newPeer(t)
	if err := os.WriteFile(p.hostPath("OPL.ELF"), bytes.Repeat([]byte{'O'}, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	h := p.open("OPL.ELF", fioRdonly)
	if h <= 0 {
		t.Fatalf("OPEN for reading: %d", h)
	}
	p.read(h, 8)
	p.reboot()

	data := bytes.Repeat([]byte{'N'}, 100)
	p.writeFile("OPL.ELF", data)
	p.wantFile("OPL.ELF", data)
}

// A file the console was writing when it was reset can be read afterwards
func TestReadAfterResetWhileOpenForWriting(t *testing.T) {
	p := newPeer(t)
	data := bytes.Repeat([]byte{'D'}, 200)
	p.startWrite("save.psu", data)
	p.reboot()

	h := p.open("save.psu", fioRdonly)
	if h <= 0 {
		t.Fatalf("OPEN for reading: %d", h)
	}
	if got := p.read(h, 512); !bytes.Equal(got, data) {
		t.Fatalf("READ returned %d bytes, want the %d bytes written", len(got), len(data))
	}
}

// A console reset while reading a file (a game, say) gets the same handle
// back when it opens the file for reading again, positioned at the start
func TestReopenForReadingAfterResetReusesHandle(t *testing.T) {
	p := newPeer(t)
	data := []byte("game data at the start of the file")
	if err := os.WriteFile(p.hostPath("game.iso"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.hostPath("SYSTEM.CNF"), []byte("BOOT2"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Leave a lower handle free, so that opening the file afresh would return another handle
	cnf := p.open("SYSTEM.CNF", fioRdonly)
	h := p.open("game.iso", fioRdonly)
	if cnf <= 0 || h <= 0 {
		t.Fatalf("OPEN: %d, %d", cnf, h)
	}
	if r := p.close(cnf); r != 0 {
		t.Fatalf("CLOSE: %d", r)
	}
	p.read(h, 8)
	p.reboot()

	if h2 := p.open("game.iso", fioRdonly); h2 != h {
		t.Fatalf("OPEN after reset returned handle %d, want the cached handle %d", h2, h)
	}
	if got := p.read(h, uint32(len(data))); !bytes.Equal(got, data) {
		t.Fatalf("READ after reset returned %q, want %q", got, data)
	}
}
