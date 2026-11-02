package udpfs_test

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pcm720/udpfsd/internal/fs"
	"github.com/pcm720/udpfsd/udpfs"
	"github.com/pcm720/udpfsd/udprdma"
	"github.com/pierrec/lz4/v4"
)

// Open flags as the PS2 sends them in OPEN_REQ
const (
	fioRdonly = 0x0001
	fioWronly = 0x0002
	fioRdwr   = 0x0003
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

func newPeer(t *testing.T, opts ...fs.BackendOptFunc) *peer {
	t.Helper()
	return newPeerConn(t, false, opts...)
}

// newPeerConn is newPeer with the connection's metrics collection on or off
func newPeerConn(t *testing.T, metrics bool, opts ...fs.BackendOptFunc) *peer {
	t.Helper()
	return newPeerWrapped(t, metrics, nil, opts...)
}

// newPeerWrapped is newPeerConn with the connection using wrap(backend) when wrap is not nil
func newPeerWrapped(t *testing.T, metrics bool, wrap func(udpfs.FS) udpfs.FS, opts ...fs.BackendOptFunc) *peer {
	t.Helper()
	p := &peer{
		t:    t,
		root: filepath.Join(t.TempDir(), "root"), // the parent is free for files outside the root
		addr: &net.UDPAddr{IP: net.IPv4(192, 168, 0, 10), Port: udprdma.UDPFSPort},
	}
	if err := os.Mkdir(p.root, 0o755); err != nil {
		t.Fatal(err)
	}
	backend, err := fs.NewBackend(append([]fs.BackendOptFunc{fs.WithFSRoot(p.root)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	var fsys udpfs.FS = backend
	if wrap != nil {
		fsys = wrap(backend)
	}
	p.sess = udprdma.NewSession(*p.addr, p.receive, nil)
	p.conn = udpfs.NewConnection(p.sess, fsys, false, metrics)
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
	replies := p.send(req)
	for i := len(replies) - 1; i >= 0; i-- {
		if r := replies[i]; len(r) >= 8 && udpfs.MsgType(r[0]) == want {
			return r
		}
	}
	p.t.Fatalf("no reply 0x%02x to %v request", uint8(want), udpfs.MsgType(req[0]))
	return nil
}

// send sends req and returns the UDPFS payloads of all packets the server sent in reply
// (empty for an ACK)
func (p *peer) send(req []byte) [][]byte {
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
	return p.replies
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
	h, _ := p.openStat(path, flags)
	return h
}

// openStat sends OPEN and returns the handle and the stat fields of the reply
// (OPEN_REPLY has no attr and atime)
func (p *peer) openStat(path string, flags uint16) (int32, udpfs.StatInfo) {
	p.t.Helper()
	req := make([]byte, 8)
	req[0] = byte(udpfs.MsgOpenReq)
	binary.LittleEndian.PutUint16(req[2:], flags)
	reply := p.request(withPath(req, path), udpfs.MsgOpenReply)
	var st udpfs.StatInfo
	st.Mode = binary.LittleEndian.Uint32(reply[8:])
	st.Size = binary.LittleEndian.Uint32(reply[12:])
	st.Hisize = binary.LittleEndian.Uint32(reply[16:])
	copy(st.Ctime[:], reply[20:])
	copy(st.Mtime[:], reply[28:])
	return result(reply), st
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

// getstat sends GETSTAT and returns the result and the stat fields of the reply
func (p *peer) getstat(path string) (int32, udpfs.StatInfo) {
	p.t.Helper()
	reply := p.request(withPath([]byte{byte(udpfs.MsgGetstatReq), 0, 0, 0}, path), udpfs.MsgGetstatReply)
	var st udpfs.StatInfo
	st.Mode = binary.LittleEndian.Uint32(reply[8:])
	st.Attr = binary.LittleEndian.Uint32(reply[12:])
	st.Size = binary.LittleEndian.Uint32(reply[16:])
	st.Hisize = binary.LittleEndian.Uint32(reply[20:])
	copy(st.Ctime[:], reply[24:])
	copy(st.Atime[:], reply[32:])
	copy(st.Mtime[:], reply[40:])
	return result(reply), st
}

// dirEntry is a directory entry as DREAD reports it
type dirEntry struct {
	name string
	mode uint32
	size uint32
}

func (p *peer) opendir(path string) int32 {
	p.t.Helper()
	req := make([]byte, 8)
	req[0] = byte(udpfs.MsgOpenReq)
	req[1] = 1 // is_dir
	h := result(p.request(withPath(req, path), udpfs.MsgOpenReply))
	if h <= 0 {
		p.t.Fatalf("OPEN directory %q: %d", path, h)
	}
	return h
}

// readdir sends DREAD until the end of the directory and closes it
func (p *peer) readdir(handle int32) []dirEntry {
	p.t.Helper()
	var entries []dirEntry
	for {
		req := make([]byte, 8)
		req[0] = byte(udpfs.MsgDreadReq)
		binary.LittleEndian.PutUint32(req[4:], uint32(handle))
		reply := p.request(req, udpfs.MsgDreadReply)
		if r := result(reply); r < 0 {
			p.t.Fatalf("DREAD: %d", r)
		} else if r == 0 {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(reply[2:]))
		entries = append(entries, dirEntry{
			name: string(reply[48 : 48+nameLen]),
			mode: binary.LittleEndian.Uint32(reply[8:]),
			size: binary.LittleEndian.Uint32(reply[16:]),
		})
	}
	if r := p.close(handle); r != 0 {
		p.t.Fatalf("CLOSE directory: %d", r)
	}
	return entries
}

// list returns the entries of a directory in the order DREAD reports them
func (p *peer) list(path string) []dirEntry {
	p.t.Helper()
	return p.readdir(p.opendir(path))
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
// back when it opens the file for reading again, positioned at the start,
// and the same stat in OPEN_REPLY as when it opened the file
func TestReopenForReadingAfterResetReusesHandle(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	data := []byte("game data at the start of the file")
	if err := os.WriteFile(p.hostPath("game.iso"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.hostPath("SYSTEM.CNF"), []byte("BOOT2"), 0o644); err != nil {
		t.Fatal(err)
	}
	p.copyTestImage("test.cso", "game2.cso")
	// Leave a lower handle free, so that opening the file afresh would return another handle
	cnf := p.open("SYSTEM.CNF", fioRdonly)
	h, st := p.openStat("game.iso", fioRdonly)
	h2, st2 := p.openStat("game2.cso.iso", fioRdonly)
	if cnf <= 0 || h <= 0 || h2 <= 0 {
		t.Fatalf("OPEN: %d, %d, %d", cnf, h, h2)
	}
	if st.Mode != udpfs.FIO_S_IFREG || st.Size != uint32(len(data)) || st2.Mode != udpfs.FIO_S_IFREG || st2.Size == 0 {
		t.Fatalf("OPEN_REPLY: %+v and %+v, want regular files of %d bytes and of the ISO image's size", st, st2, len(data))
	}
	if r := p.close(cnf); r != 0 {
		t.Fatalf("CLOSE: %d", r)
	}
	p.read(h, 8)
	p.reboot()

	for _, f := range []struct {
		path   string
		handle int32
		stat   udpfs.StatInfo
	}{{"game.iso", h, st}, {"game2.cso.iso", h2, st2}} {
		if got, gotStat := p.openStat(f.path, fioRdonly); got != f.handle || gotStat != f.stat {
			t.Errorf("OPEN %s after reset: handle %d, %+v; want the cached handle %d, %+v", f.path, got, gotStat, f.handle, f.stat)
		}
	}
	if got := p.read(h, uint32(len(data))); !bytes.Equal(got, data) {
		t.Fatalf("READ after reset returned %q, want %q", got, data)
	}
}

// A file the console was reading when it was reset and then removed is gone:
// the handle the reset left must not keep it visible to GETSTAT and OPEN
func TestRemoveAfterResetWhileOpenForReading(t *testing.T) {
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

	if r := p.pathOp(udpfs.MsgRemoveReq, "OPL.ELF"); r != 0 {
		t.Fatalf("REMOVE: %d", r)
	}
	if _, err := os.Stat(p.hostPath("OPL.ELF")); !os.IsNotExist(err) {
		t.Fatalf("file after REMOVE: %v", err)
	}
	if res, st := p.getstat("OPL.ELF"); res != -udpfs.ENOENT {
		t.Errorf("GETSTAT after REMOVE: result %d, size %d; want %d", res, st.Size, -udpfs.ENOENT)
	}
	if h2 := p.open("OPL.ELF", fioRdonly); h2 != -udpfs.ENOENT {
		t.Errorf("OPEN for reading after REMOVE: %d, want %d", h2, -udpfs.ENOENT)
	}
}

// A file the console had open for reading and writing (a VMC image, say) when
// it was reset is opened afresh when the console opens it again, as the file
// at the path may have been replaced since
func TestReopenForReadWriteAfterReset(t *testing.T) {
	p := newPeer(t)
	if err := os.WriteFile(p.hostPath("vmc.bin"), []byte("old card"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := p.open("vmc.bin", fioRdwr)
	if h <= 0 {
		t.Fatalf("OPEN for reading and writing: %d", h)
	}
	p.read(h, 4)
	p.reboot()

	// Replace the file, as a tool on the PC would
	if err := os.WriteFile(p.hostPath("vmc.new"), []byte("new card"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(p.hostPath("vmc.new"), p.hostPath("vmc.bin")); err != nil {
		t.Skipf("cannot replace a file that is open: %v", err)
	}
	h = p.open("vmc.bin", fioRdwr)
	if h <= 0 {
		t.Fatalf("OPEN for reading and writing after reset: %d", h)
	}
	if got := p.read(h, 64); string(got) != "new card" {
		t.Fatalf("READ after reset: %q, want %q", got, "new card")
	}
}

// noRemoveWhileOpen refuses to remove a file while a handle is open on it, as Windows does
type noRemoveWhileOpen struct {
	udpfs.FS
	mu   sync.Mutex
	open map[int32]string // path of every open file handle
}

func (n *noRemoveWhileOpen) Open(path string, flag int, isDir bool) (int32, udpfs.StatInfo, error) {
	h, st, err := n.FS.Open(path, flag, isDir)
	if err == nil && h > 0 && !isDir {
		n.mu.Lock()
		n.open[h] = path
		n.mu.Unlock()
	}
	return h, st, err
}

func (n *noRemoveWhileOpen) Close(handle int32) error {
	n.mu.Lock()
	delete(n.open, handle)
	n.mu.Unlock()
	return n.FS.Close(handle)
}

func (n *noRemoveWhileOpen) Remove(path string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, p := range n.open {
		if p == path {
			return os.ErrPermission
		}
	}
	return n.FS.Remove(path)
}

// REMOVE of a file the console was reading when it was reset closes the
// handle the reset left before it removes the file, which Windows refuses
// to remove while it is open
func TestRemoveAfterResetClosesHandleFirst(t *testing.T) {
	p := newPeerWrapped(t, false, func(backend udpfs.FS) udpfs.FS {
		return &noRemoveWhileOpen{FS: backend, open: map[int32]string{}}
	})
	if err := os.WriteFile(p.hostPath("OPL.ELF"), bytes.Repeat([]byte{'O'}, 300), 0o644); err != nil {
		t.Fatal(err)
	}
	h := p.open("OPL.ELF", fioRdonly)
	if h <= 0 {
		t.Fatalf("OPEN for reading: %d", h)
	}
	p.read(h, 8)
	p.reboot()

	if r := p.pathOp(udpfs.MsgRemoveReq, "OPL.ELF"); r != 0 {
		t.Fatalf("REMOVE: %d, want 0", r)
	}
	if _, err := os.Stat(p.hostPath("OPL.ELF")); !os.IsNotExist(err) {
		t.Fatalf("file after REMOVE: %v", err)
	}
}

// GETSTAT answers for any path, with the same fields as for an open file
func TestGetstatClosedFile(t *testing.T) {
	p := newPeer(t)
	path := p.hostPath("APPS/OPL.ELF")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 5000), 0o644); err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2024, time.March, 5, 6, 7, 8, 0, time.Local)
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	ps2Time := [8]byte{0, 8, 7, 6, 5, 3, 0xE8, 0x07} // 2024-03-05 06:07:08
	want := udpfs.StatInfo{Mode: udpfs.FIO_S_IFREG, Size: 5000, Ctime: ps2Time, Atime: ps2Time, Mtime: ps2Time}

	if res, st := p.getstat("APPS/OPL.ELF"); res != 0 || st != want {
		t.Fatalf("GETSTAT of a closed file: result %d, %+v; want 0, %+v", res, st, want)
	}
	if h := p.open("APPS/OPL.ELF", fioRdonly); h <= 0 {
		t.Fatalf("OPEN: %d", h)
	}
	if res, st := p.getstat("APPS/OPL.ELF"); res != 0 || st != want {
		t.Fatalf("GETSTAT of an open file: result %d, %+v; want 0, %+v", res, st, want)
	}
}

func TestGetstatDirectory(t *testing.T) {
	p := newPeer(t)
	if err := os.Mkdir(p.hostPath("APPS"), 0o755); err != nil {
		t.Fatal(err)
	}
	if res, st := p.getstat("APPS"); res != 0 || st.Mode != udpfs.FIO_S_IFDIR {
		t.Fatalf("GETSTAT of a directory: result %d, mode 0x%x; want 0, mode 0x%x", res, st.Mode, udpfs.FIO_S_IFDIR)
	}
}

func TestGetstatMissingPath(t *testing.T) {
	p := newPeer(t)
	if res, st := p.getstat("APPS/MISSING.ELF"); res != -udpfs.ENOENT || st != (udpfs.StatInfo{}) {
		t.Fatalf("GETSTAT of a missing file: result %d, %+v; want %d", res, st, -udpfs.ENOENT)
	}
}

// GETSTAT reports nothing about files outside the served directory
func TestGetstatOutsideRoot(t *testing.T) {
	p := newPeer(t)
	if err := os.WriteFile(filepath.Join(p.root, "..", "outside.elf"), make([]byte, 5000), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../outside.elf", "APPS/../../outside.elf"} {
		if res, st := p.getstat(path); res >= 0 || st != (udpfs.StatInfo{}) {
			t.Errorf("GETSTAT %s: result %d, %+v; want an error", path, res, st)
		}
	}
}

// A compressed image is listed and opened as <name>.iso, so GETSTAT finds it the same way
func TestGetstatCompressedImage(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	cso, err := os.ReadFile("../internal/fs/compression/test/test.cso")
	if err != nil {
		t.Fatal(err)
	}
	iso, err := os.Stat("../internal/fs/compression/test/test.iso")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.hostPath("game.cso"), cso, 0o644); err != nil {
		t.Fatal(err)
	}
	if res, st := p.getstat("game.cso.iso"); res != 0 || st.Mode != udpfs.FIO_S_IFREG || st.Size != uint32(iso.Size()) || st.Hisize != 0 {
		t.Fatalf("GETSTAT of a compressed image: result %d, %+v; want 0, a regular file of %d bytes", res, st, iso.Size())
	}

	// Without decompression, OPEN doesn't find the image as <name>.iso, and neither does GETSTAT
	p = newPeer(t)
	if err := os.WriteFile(p.hostPath("game.cso"), cso, 0o644); err != nil {
		t.Fatal(err)
	}
	if res, st := p.getstat("game.cso.iso"); res != -udpfs.ENOENT {
		t.Errorf("GETSTAT of a compressed image without decompression: result %d, %+v; want %d", res, st, -udpfs.ENOENT)
	}
	if h := p.open("game.cso.iso", fioRdonly); h != -udpfs.ENOENT {
		t.Errorf("OPEN of a compressed image without decompression: %d, want %d", h, -udpfs.ENOENT)
	}
}

// An empty path asks for the block device, never for the served directory
func TestGetstatEmptyPath(t *testing.T) {
	p := newPeer(t)
	if res, st := p.getstat(""); res >= 0 {
		t.Errorf("GETSTAT of an empty path without a block device: result %d, %+v; want an error", res, st)
	}

	disk := filepath.Join(t.TempDir(), "disk.img")
	if err := os.WriteFile(disk, make([]byte, 64*512), 0o644); err != nil {
		t.Fatal(err)
	}
	p = newPeer(t, fs.WithBlockDevice(disk))
	if res, st := p.getstat(""); res != 0 || st.Size != 64*512 || st.Hisize != 0 {
		t.Errorf("GETSTAT of an empty path with a block device: result %d, %+v; want 0, size %d", res, st, 64*512)
	}
}

const enotdir = 20 // ENOTDIR

// RMDIR removes nothing but directories
func TestRmdirNotADirectory(t *testing.T) {
	p := newPeer(t)
	if err := os.WriteFile(p.hostPath("OPL.ELF"), []byte("ELF"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := p.pathOp(udpfs.MsgRmdirReq, "OPL.ELF"); r != -enotdir {
		t.Errorf("RMDIR of a file: %d, want %d", r, -enotdir)
	}
	if _, err := os.Stat(p.hostPath("OPL.ELF")); err != nil {
		t.Fatalf("RMDIR of a file: %v", err)
	}

	if err := os.Mkdir(p.hostPath("APPS"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(p.hostPath("APPS"), p.hostPath("LINK")); err != nil {
		t.Skipf("no symlink to test with: %v", err)
	}
	if r := p.pathOp(udpfs.MsgRmdirReq, "LINK"); r != -enotdir {
		t.Errorf("RMDIR of a symlink to a directory: %d, want %d", r, -enotdir)
	}
	if _, err := os.Lstat(p.hostPath("LINK")); err != nil {
		t.Fatalf("RMDIR of a symlink to a directory: %v", err)
	}
}

func TestRmdirDirectory(t *testing.T) {
	p := newPeer(t)
	if err := os.MkdirAll(p.hostPath("APPS/OPL"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.hostPath("APPS/OPL/OPL.ELF"), []byte("ELF"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"APPS", "APPS/OPL"} {
		if r := p.pathOp(udpfs.MsgRmdirReq, path); r >= 0 {
			t.Errorf("RMDIR of %s, which is not empty: %d, want an error", path, r)
		}
	}
	if _, err := os.Stat(p.hostPath("APPS/OPL/OPL.ELF")); err != nil {
		t.Fatalf("RMDIR of a directory that is not empty: %v", err)
	}

	if err := os.Remove(p.hostPath("APPS/OPL/OPL.ELF")); err != nil {
		t.Fatal(err)
	}
	if r := p.pathOp(udpfs.MsgRmdirReq, "APPS/OPL"); r != 0 {
		t.Fatalf("RMDIR of an empty directory: %d", r)
	}
	if _, err := os.Stat(p.hostPath("APPS/OPL")); !os.IsNotExist(err) {
		t.Fatalf("RMDIR of an empty directory left it in place: %v", err)
	}
}

// MKDIR succeeds for a directory that already exists, but not for a file
func TestMkdirExistingPath(t *testing.T) {
	p := newPeer(t)
	if r := p.pathOp(udpfs.MsgMkdirReq, "APPS"); r != 0 {
		t.Errorf("MKDIR: %d, want 0", r)
	}
	if r := p.pathOp(udpfs.MsgMkdirReq, "APPS"); r != 0 {
		t.Errorf("MKDIR of an existing directory: %d, want 0", r)
	}
	if st, err := os.Stat(p.hostPath("APPS")); err != nil || !st.IsDir() {
		t.Fatalf("MKDIR did not create a directory: %v", err)
	}

	if err := os.WriteFile(p.hostPath("OPL.ELF"), []byte("ELF"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := p.pathOp(udpfs.MsgMkdirReq, "OPL.ELF"); r != -udpfs.EEXIST {
		t.Errorf("MKDIR where a file exists: %d, want %d", r, -udpfs.EEXIST)
	}
}

// A request with a handle the server never returned, or asking for more data
// than fits into one reply, gets an error reply and the server keeps running
func TestInvalidHandleOrSize(t *testing.T) {
	p := newPeer(t)
	data := []byte("ELF data")
	if err := os.WriteFile(p.hostPath("OPL.ELF"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	h := p.open("OPL.ELF", fioRdonly)
	if h <= 0 {
		t.Fatalf("OPEN: %d", h)
	}

	// req builds a request of size bytes with the handle at offset 4 and sets
	// the 16-bit field at offset 2 (sector_count) or 8 (size) when given
	req := func(msg udpfs.MsgType, size int, handle int32, field16 uint16, size32 uint32) []byte {
		r := make([]byte, size)
		r[0] = byte(msg)
		binary.LittleEndian.PutUint16(r[2:], field16)
		binary.LittleEndian.PutUint32(r[4:], uint32(handle))
		if size >= 12 {
			binary.LittleEndian.PutUint32(r[8:], size32)
		}
		return r
	}
	chunk := func(r []byte) []byte {
		// WRITE_DATA header of a single 4-byte chunk, and the chunk
		return append(r, byte(udpfs.MsgWriteData), 0, 0, 0, 4, 0, 1, 0, 'D', 'A', 'T', 'A')
	}
	for _, tc := range []struct {
		name  string
		req   []byte
		reply udpfs.MsgType
	}{
		{"CLOSE 1000", req(udpfs.MsgCloseReq, 8, 1000, 0, 0), udpfs.MsgCloseReply},
		{"CLOSE -5", req(udpfs.MsgCloseReq, 8, -5, 0, 0), udpfs.MsgCloseReply},
		{"READ 1000", req(udpfs.MsgReadReq, 12, 1000, 0, 16), udpfs.MsgResultReply},
		{"WRITE 1000", chunk(req(udpfs.MsgWriteReq, 12, 1000, 0, 4)), udpfs.MsgWriteDone},
		{"LSEEK 1000", req(udpfs.MsgLseekReq, 16, 1000, 0, 0), udpfs.MsgLseekReply},
		{"DREAD 0", req(udpfs.MsgDreadReq, 8, 0, 0, 0), udpfs.MsgDreadReply},
		{"DREAD 1000", req(udpfs.MsgDreadReq, 8, 1000, 0, 0), udpfs.MsgDreadReply},
		{"DREAD -5", req(udpfs.MsgDreadReq, 8, -5, 0, 0), udpfs.MsgDreadReply},
		{"BREAD 1000", req(udpfs.MsgBreadReq, 16, 1000, 1, 0), udpfs.MsgResultReply},
		{"BWRITE 1000", chunk(req(udpfs.MsgBwriteReq, 16, 1000, 1, 0)), udpfs.MsgWriteDone},
		{"READ of 256 MB", req(udpfs.MsgReadReq, 12, h, 0, 256<<20), udpfs.MsgResultReply},
		{"BREAD of 65535 sectors", req(udpfs.MsgBreadReq, 16, h, 0xFFFF, 0), udpfs.MsgResultReply},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: server panicked: %v", tc.name, r)
				}
			}()
			if res := result(p.request(tc.req, tc.reply)); res >= 0 {
				t.Errorf("%s: %d, want an error", tc.name, res)
			}
		}()
	}

	if got := p.read(h, 64); !bytes.Equal(got, data) {
		t.Fatalf("READ of an open file afterwards: %q, want %q", got, data)
	}

	// 65535 sectors of 64 KB overflow a 32-bit int
	disk := filepath.Join(t.TempDir(), "disk.img")
	if err := os.WriteFile(disk, make([]byte, 64*512), 0o644); err != nil {
		t.Fatal(err)
	}
	p = newPeer(t, fs.WithBlockDevice(disk), fs.WithSectorSize(64<<10))
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("BREAD of 65535 sectors of 64 KB: server panicked: %v", r)
			}
		}()
		if res := result(p.request(req(udpfs.MsgBreadReq, 16, udpfs.BlockDeviceHandle, 0xFFFF, 0), udpfs.MsgResultReply)); res >= 0 {
			t.Errorf("BREAD of 65535 sectors of 64 KB: %d, want an error", res)
		}
	}()
}

// BWRITE of 65535 sectors, whose size in bytes doesn't fit into an int with
// sectors of 64 KB on 32-bit builds, writes the data it got
func TestBwriteSizeOverflow(t *testing.T) {
	disk := filepath.Join(t.TempDir(), "disk.img")
	if err := os.WriteFile(disk, make([]byte, 64*512), 0o644); err != nil {
		t.Fatal(err)
	}
	// The smallest sector size for which 65535 sectors overflow an int:
	// 64 KB on 32-bit builds, 256 TB on 64-bit ones
	p := newPeer(t, fs.WithBlockDevice(disk), fs.WithSectorSize(1<<(strconv.IntSize-16)))
	req := make([]byte, 16)
	req[0] = byte(udpfs.MsgBwriteReq)
	binary.LittleEndian.PutUint16(req[2:], 0xFFFF) // sector_count
	binary.LittleEndian.PutUint32(req[4:], uint32(udpfs.BlockDeviceHandle))
	// WRITE_DATA header of a single 4-byte chunk, and the chunk
	req = append(req, byte(udpfs.MsgWriteData), 0, 0, 0, 4, 0, 1, 0, 'D', 'A', 'T', 'A')
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("BWRITE of 65535 sectors: server panicked: %v", r)
			}
		}()
		if res := result(p.request(req, udpfs.MsgWriteDone)); res != 0 {
			t.Errorf("BWRITE of 65535 sectors: %d, want 0", res)
		}
	}()
	if data, err := os.ReadFile(disk); err != nil || !bytes.HasPrefix(data, []byte("DATA")) {
		t.Errorf("block device after BWRITE: % x..., %v; want it to start with \"DATA\"", data[:min(len(data), 8)], err)
	}
}

// A WRITE_DATA without a write in progress, such as a chunk the console sent
// before it got the WRITE_DONE that failed the write, is acknowledged and
// counted as an error, not as data received
func TestWriteDataWithoutWrite(t *testing.T) {
	p := newPeerConn(t, true)
	// WRITE_DATA header of a single 4-byte chunk, and the chunk
	chunk := []byte{byte(udpfs.MsgWriteData), 0, 0, 0, 4, 0, 1, 0, 'D', 'A', 'T', 'A'}
	for _, r := range p.send(chunk) {
		if len(r) != 0 {
			t.Fatalf("reply to WRITE_DATA without a write: % x, want ACKs only", r)
		}
	}
	m := p.conn.GetMetrics()
	if errs := m.ErrorCounts[udpfs.MsgWriteData]; errs != 1 || m.BytesRx != 0 {
		t.Fatalf("metrics after WRITE_DATA without a write: %d errors, %d bytes received; want 1 error, 0 bytes", errs, m.BytesRx)
	}
}

// copyTestImage copies a file from the compression test data into the served directory
func (p *peer) copyTestImage(name, path string) {
	p.t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "internal", "fs", "compression", "test", name))
	if err != nil {
		p.t.Fatal(err)
	}
	if err := os.WriteFile(p.hostPath(path), data, 0o644); err != nil {
		p.t.Fatal(err)
	}
}

// openFiles returns the number of files this process has open in dir, dir
// included. Files that other tests left for the garbage collector to close,
// which it can do at any time, don't count.
func openFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot count open files: %v", err)
	}
	if dir, err = filepath.EvalSymlinks(dir); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err == nil && (target == dir || strings.HasPrefix(target, dir+string(filepath.Separator))) {
			n++
		}
	}
	return n
}

// Listing compressed images and GETSTAT of their .iso names read the image
// headers, and must close the images afterwards
func TestCompressedImageStatClosesFiles(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	openFiles(t, p.root) // skip early where open files cannot be counted
	p.copyTestImage("test.cso", "a.cso")
	p.copyTestImage("test.zso", "b.zso")
	// The garbage collector closes files nothing refers to, which would hide a leak
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	before := openFiles(t, p.root)
	for range 10 {
		if got := p.list(""); len(got) != 2 {
			t.Fatalf("listing: %+v, want 2 entries", got)
		}
	}
	if leaked := openFiles(t, p.root) - before; leaked != 0 {
		t.Errorf("listing 2 compressed images 10 times left %d files open", leaked)
	}

	before = openFiles(t, p.root)
	for range 10 {
		for _, name := range []string{"a.cso.iso", "b.zso.iso"} {
			if res, _ := p.getstat(name); res != 0 {
				t.Fatalf("GETSTAT %s: %d", name, res)
			}
		}
	}
	if leaked := openFiles(t, p.root) - before; leaked != 0 {
		t.Errorf("GETSTAT of 2 compressed images 10 times left %d files open", leaked)
	}
}

// A file removed while the directory is open is left out of the listing,
// and the entries after it are still listed
func TestListingSkipsRemovedFile(t *testing.T) {
	p := newPeer(t)
	for _, name := range []string{"a.elf", "b.elf", "c.elf"} {
		if err := os.WriteFile(p.hostPath(name), []byte("ELF"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	h := p.opendir("")
	if err := os.Remove(p.hostPath("b.elf")); err != nil {
		t.Fatal(err)
	}
	want := []dirEntry{{"a.elf", udpfs.FIO_S_IFREG, 3}, {"c.elf", udpfs.FIO_S_IFREG, 3}}
	if got := p.readdir(h); !slices.Equal(got, want) {
		t.Fatalf("listing after b.elf was removed: %+v, want %+v", got, want)
	}
}

// A file with the extension of a compressed image that is not a valid image
// is listed as the plain file OPEN finds under that name, and the entries
// after it are still listed
func TestListingInvalidCompressedImage(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	for name, data := range map[string]string{"a.iso": "ISO", "b.cso": "not a CSO image", "c.zso": ""} {
		if err := os.WriteFile(p.hostPath(name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p.copyTestImage("test.cso", "d.cso")
	want := []dirEntry{
		{"a.iso", udpfs.FIO_S_IFREG, 3},
		{"b.cso", udpfs.FIO_S_IFREG, 15},
		{"c.zso", udpfs.FIO_S_IFREG, 0},
		{"d.cso.iso", udpfs.FIO_S_IFREG, 1110016},
	}
	if got := p.list(""); !slices.Equal(got, want) {
		t.Fatalf("listing: %+v, want %+v", got, want)
	}
}

// OPEN finds a compressed image by <name>.iso whatever the case of its
// extension, so the listing has to show it under that name too
func TestListingUpperCaseCompressedImage(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	p.copyTestImage("test.cso", "GAME.CSO")
	p.copyTestImage("test.zso", "GAME2.ZsO")
	want := []dirEntry{
		{"GAME.CSO.iso", udpfs.FIO_S_IFREG, 1110016},
		{"GAME2.ZsO.iso", udpfs.FIO_S_IFREG, 1110016},
	}
	got := p.list("")
	if !slices.Equal(got, want) {
		t.Fatalf("listing: %+v, want %+v", got, want)
	}
	for _, e := range got {
		if h := p.open(e.name, fioRdonly); h <= 0 {
			t.Errorf("OPEN %s: %d", e.name, h)
		}
	}
}

// A symlink is listed with the type and size of what it points to, which is
// what OPEN and GETSTAT find under its name
func TestListingSymlinks(t *testing.T) {
	p := newPeer(t)
	if err := os.WriteFile(p.hostPath("a.elf"), []byte("ELF data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p.hostPath("b"), 0o755); err != nil {
		t.Fatal(err)
	}
	for link, target := range map[string]string{"c.elf": "a.elf", "d": "b"} {
		if err := os.Symlink(target, p.hostPath(link)); err != nil {
			t.Skipf("cannot create symlinks: %v", err)
		}
	}
	dir, err := os.Stat(p.hostPath("b"))
	if err != nil {
		t.Fatal(err)
	}
	want := []dirEntry{
		{"a.elf", udpfs.FIO_S_IFREG, 8},
		{"b", udpfs.FIO_S_IFDIR, uint32(dir.Size())},
		{"c.elf", udpfs.FIO_S_IFREG, 8},
		{"d", udpfs.FIO_S_IFDIR, uint32(dir.Size())},
	}
	if got := p.list(""); !slices.Equal(got, want) {
		t.Fatalf("listing: %+v, want %+v", got, want)
	}
	for _, e := range want {
		if res, st := p.getstat(e.name); res != 0 || st.Mode != e.mode || st.Size != e.size {
			t.Errorf("GETSTAT %s: result %d, mode 0x%x, size %d; want 0, mode 0x%x, size %d", e.name, res, st.Mode, st.Size, e.mode, e.size)
		}
	}
}

// compressedImage returns a CSO or ZISO header, the block map and data
func compressedImage(magic string, size uint64, blockSize uint32, blockMap []uint32, data int) []byte {
	b := make([]byte, 24)
	copy(b, magic)
	binary.LittleEndian.PutUint32(b[4:], 24) // header size
	binary.LittleEndian.PutUint64(b[8:], size)
	binary.LittleEndian.PutUint32(b[16:], blockSize)
	b[20] = 1 // version
	for _, offset := range blockMap {
		b = binary.LittleEndian.AppendUint32(b, offset)
	}
	return append(b, make([]byte, data)...)
}

// A compressed image with a corrupt header or block map can't be opened or
// read, and the server keeps running
func TestCorruptCompressedImage(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	// Blocks of 2 KB that decompress to 64 KB
	bomb := deflated(t, make([]byte, 64<<10))
	zbomb := zlibCompressed(t, make([]byte, 64<<10))
	images := map[string][]byte{
		"bomb.cso":  append(compressedImage("CISO", 2048, 2048, []uint32{32, 32 + uint32(len(bomb))}, 0), bomb...),
		"zbomb.cso": append(compressedImage("CISO", 2048, 2048, []uint32{32, 32 + uint32(len(zbomb))}, 0), zbomb...),
		// Uncompressed size above 2^63
		"negative.cso": compressedImage("CISO", 1<<63, 2048, nil, 0),
		"negative.zso": compressedImage("ZISO", 1<<63, 2048, nil, 0),
		// 2^62 blocks, which need a block map far larger than the file
		"huge.cso": compressedImage("CISO", 1<<62, 1, []uint32{0}, 0),
		"huge.zso": compressedImage("ZISO", 1<<62, 1, []uint32{0}, 0),
		// Two blocks, the second one before the first one
		"reversed.cso": compressedImage("CISO", 4096, 2048, []uint32{200, 100, 300}, 300),
		"reversed.zso": compressedImage("ZISO", 4096, 2048, []uint32{200, 100, 300}, 300),
	}
	// One block that ends about 2^51 bytes into the file, as offsets are in units of 2^20
	// bytes: more than a slice can hold
	for name, magic := range map[string]string{"far.cso": "CISO", "far.zso": "ZISO"} {
		images[name] = compressedImage(magic, 2048, 2048, []uint32{0, 0x7FFFFFFF}, 16)
		images[name][21] = 20 // align
	}
	// One block whose data is in the file and decompresses, but which the block
	// map says ends 2 GB into the file, past its end
	block := make([]byte, 2048)
	images["pastend.cso"] = append(append(compressedImage("CISO", 2048, 2048, []uint32{32, 0x7FFFFFFF}, 0),
		deflated(t, block)...), make([]byte, 4096)...)
	images["pastend.zso"] = append(append(compressedImage("ZISO", 2048, 2048, []uint32{32, 0x7FFFFFFF}, 0),
		lz4Compressed(t, block)...), make([]byte, 4096)...)
	for name, data := range images {
		if err := os.WriteFile(p.hostPath(name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name := range images {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: server panicked: %v", name, r)
				}
			}()
			h := p.open(name+".iso", fioRdonly)
			if h <= 0 {
				return // can't be opened
			}
			req := make([]byte, 12)
			req[0] = byte(udpfs.MsgReadReq)
			binary.LittleEndian.PutUint32(req[4:], uint32(h))
			binary.LittleEndian.PutUint32(req[8:], 4096)
			if res := result(p.request(req, udpfs.MsgResultReply)); res >= 0 {
				t.Errorf("%s: OPEN succeeded and READ returned %d, want an error", name, res)
			}
			p.close(h)
		}()
	}

	p.copyTestImage("test.cso", "game.cso")
	if h := p.open("game.cso.iso", fioRdonly); h <= 0 {
		t.Fatalf("OPEN of a valid image afterwards: %d", h)
	}
}

// A compressed image with a header OPEN rejects is listed as the file it is,
// and GETSTAT doesn't report it as an ISO image either
func TestCorruptCompressedImageListing(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	images := []struct {
		name string
		data []byte
	}{
		// One block of 2 GB, which READ would have to allocate
		{"big.cso", compressedImage("CISO", 2048, 0x7FFFFFFF, []uint32{32, 48}, 16)},
		{"big.zso", compressedImage("ZISO", 2048, 0x7FFFFFFF, []uint32{32 | 0x80000000, 48}, 16)},
		{"huge.zso", compressedImage("ZISO", 1<<62, 1, []uint32{0}, 0)},
		{"negative.cso", compressedImage("CISO", 1<<63, 2048, nil, 0)},
		// Two blocks, and the file ends before the entry for the end of the second one
		{"short.cso", compressedImage("CISO", 4096, 2048, []uint32{32, 36}, 0)},
		{"short.zso", compressedImage("ZISO", 4096, 2048, []uint32{32, 36}, 0)},
		{"truncated.cso", compressedImage("CISO", 1<<20, 1, []uint32{0}, 0)}, // block map longer than the file
		{"truncated.zso", compressedImage("ZISO", 1<<20, 1, []uint32{0}, 0)},
		{"zero.zso", compressedImage("ZISO", 4096, 0, []uint32{0, 0, 0}, 0)}, // block size 0
	}
	var want []dirEntry
	for _, img := range images {
		if err := os.WriteFile(p.hostPath(img.name), img.data, 0o644); err != nil {
			t.Fatal(err)
		}
		want = append(want, dirEntry{img.name, udpfs.FIO_S_IFREG, uint32(len(img.data))})
	}
	if got := p.list(""); !slices.Equal(got, want) {
		t.Errorf("listing: %+v, want %+v", got, want)
	}
	for _, img := range images {
		if res, st := p.getstat(img.name + ".iso"); res != -udpfs.ENOENT {
			t.Errorf("GETSTAT %s.iso: result %d, size %d (high %d); want %d", img.name, res, st.Size, st.Hisize, -udpfs.ENOENT)
		}
		if h := p.open(img.name+".iso", fioRdonly); h != -udpfs.ENOENT {
			t.Errorf("OPEN %s.iso: %d, want %d", img.name, h, -udpfs.ENOENT)
		}
	}
}

// OPEN of a compressed image replies with the size and time of the ISO image,
// as it does for a plain file
func TestOpenCompressedImageStat(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	iso, err := os.Stat("../internal/fs/compression/test/test.iso")
	if err != nil {
		t.Fatal(err)
	}
	mtime := time.Date(2024, time.March, 5, 6, 7, 8, 0, time.Local)
	ps2Time := [8]byte{0, 8, 7, 6, 5, 3, 0xE8, 0x07} // 2024-03-05 06:07:08
	want := udpfs.StatInfo{Mode: udpfs.FIO_S_IFREG, Size: uint32(iso.Size()), Ctime: ps2Time, Mtime: ps2Time}

	for _, image := range []string{"test.cso", "test.zso"} {
		p.copyTestImage(image, image)
		if err := os.Chtimes(p.hostPath(image), mtime, mtime); err != nil {
			t.Fatal(err)
		}
		h, got := p.openStat(image+".iso", fioRdonly)
		if h <= 0 {
			t.Fatalf("OPEN %s.iso: %d", image, h)
		}
		if got != want {
			t.Errorf("OPEN_REPLY of %s.iso: %+v, want %+v", image, got, want)
		}
	}
}

// REMOVE and RMDIR never remove the served directory itself, even when it is empty
func TestRemoveRoot(t *testing.T) {
	p := newPeer(t)
	for _, msg := range []udpfs.MsgType{udpfs.MsgRemoveReq, udpfs.MsgRmdirReq} {
		for _, path := range []string{"", "/", ".", "APPS/.."} {
			if r := p.pathOp(msg, path); r >= 0 {
				t.Errorf("%v %q: %d, want an error", msg, path, r)
			}
			if st, err := os.Stat(p.root); err != nil || !st.IsDir() {
				t.Fatalf("served directory after %v %q: %v", msg, path, err)
			}
		}
	}
}

// Every request for a path outside of the served directory fails with -EACCES,
// which UDPFS.md gives for path traversal, and leaves the files there alone
func TestPathOutsideRoot(t *testing.T) {
	p := newPeer(t)
	outside := filepath.Join(p.root, "..", "outside.elf")
	if err := os.WriteFile(outside, []byte("ELF"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../outside.elf", "APPS/../../outside.elf", ".."} {
		for _, flags := range []uint16{fioRdonly, fioWronly | fioCreat | fioTrunc} {
			if h := p.open(path, flags); h != -udpfs.EACCES {
				t.Errorf("OPEN %s with flags 0x%x: %d, want %d", path, flags, h, -udpfs.EACCES)
			}
		}
		req := []byte{byte(udpfs.MsgOpenReq), 1, 0, 0, 0, 0, 0, 0} // is_dir
		if h := result(p.request(withPath(req, path), udpfs.MsgOpenReply)); h != -udpfs.EACCES {
			t.Errorf("OPEN directory %s: %d, want %d", path, h, -udpfs.EACCES)
		}
		if res, _ := p.getstat(path); res != -udpfs.EACCES {
			t.Errorf("GETSTAT %s: %d, want %d", path, res, -udpfs.EACCES)
		}
		for _, msg := range []udpfs.MsgType{udpfs.MsgMkdirReq, udpfs.MsgRemoveReq, udpfs.MsgRmdirReq} {
			if r := p.pathOp(msg, path); r != -udpfs.EACCES {
				t.Errorf("%v %s: %d, want %d", msg, path, r, -udpfs.EACCES)
			}
		}
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "ELF" {
		t.Errorf("file outside of the served directory: %q, %v; want \"ELF\"", data, err)
	}
}

// A compressed image whose header claims more blocks than a disc image has is
// refused even when its block map fits into the file, instead of reading the map
func TestCompressedImageWithTooManyBlocks(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	const blocks = 1<<25 + 1 // of one byte each
	// The header, an entry for every block and one for the end of the last block
	const size = 24 + (blocks+1)*4
	var want []dirEntry
	for _, img := range []struct{ name, magic string }{{"huge.cso", "CISO"}, {"huge.zso", "ZISO"}} {
		f, err := os.Create(p.hostPath(img.name))
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(compressedImage(img.magic, blocks, 1, nil, 0))
		if err == nil {
			err = f.Truncate(size) // sparse where the file system allows it
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, dirEntry{img.name, udpfs.FIO_S_IFREG, size})
	}
	if got := p.list(""); !slices.Equal(got, want) {
		t.Errorf("listing: %+v, want %+v", got, want)
	}
	for _, img := range want {
		if h := p.open(img.name+".iso", fioRdonly); h != -udpfs.ENOENT {
			t.Errorf("OPEN %s.iso: %d, want %d", img.name, h, -udpfs.ENOENT)
		}
	}
}

// deflated compresses data with raw deflate, as CSO blocks are
func deflated(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.BestCompression)
	if err == nil {
		_, err = w.Write(data)
	}
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// zlibCompressed compresses data with zlib, which CSO blocks are read in
// when they are not raw deflate
func zlibCompressed(t *testing.T, data []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(data)
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// Compressed images with blocks of up to 1 MB are served: maxcso writes
// blocks of up to 256 KB
func TestCompressedImageLargeBlocks(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	const blockSize = 1 << 20
	data := bytes.Repeat([]byte("0123456789ABCDEF"), blockSize/16)
	z := deflated(t, data)
	images := map[string][]byte{
		// One block, deflated
		"a.cso": append(compressedImage("CISO", blockSize, blockSize, []uint32{32, 32 + uint32(len(z))}, 0), z...),
		// One block, stored uncompressed
		"b.zso": append(compressedImage("ZISO", blockSize, blockSize, []uint32{32 | 0x80000000, 32 + blockSize}, 0), data...),
	}
	for name, img := range images {
		if err := os.WriteFile(p.hostPath(name), img, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want := []dirEntry{{"a.cso.iso", udpfs.FIO_S_IFREG, blockSize}, {"b.zso.iso", udpfs.FIO_S_IFREG, blockSize}}
	if got := p.list(""); !slices.Equal(got, want) {
		t.Fatalf("listing: %+v, want %+v", got, want)
	}
	for _, e := range want {
		h := p.open(e.name, fioRdonly)
		if h <= 0 {
			t.Fatalf("OPEN %s: %d", e.name, h)
		}
		if got := p.read(h, 64); !bytes.Equal(got, data[:64]) {
			t.Errorf("READ %s: %q, want %q", e.name, got, data[:64])
		}
	}
}

// lz4Compressed compresses data into one LZ4 block, as ZSO blocks are
func lz4Compressed(t *testing.T, data []byte) []byte {
	t.Helper()
	out := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, out, nil)
	if err != nil || n == 0 {
		t.Fatalf("LZ4 block of %d bytes: %d bytes, %v", len(data), n, err)
	}
	return out[:n]
}

// A block whose next entry in the block map is far away, as a corrupt map can
// make it, is read without taking memory for all of the file up to there: the
// data of a compressed block is smaller than the block
func TestCompressedImageBlockSpanningFile(t *testing.T) {
	p := newPeer(t, fs.WithCompression())
	data := make([]byte, 2048)
	copy(data, "the first block")
	// The block's data, then a hole up to the next entry of the block map
	const span = 16 << 20
	images := map[string][]byte{
		"a.cso": append(compressedImage("CISO", 2048, 2048, []uint32{32, 32 + span}, 0), deflated(t, data)...),
		"b.zso": append(compressedImage("ZISO", 2048, 2048, []uint32{32, 32 + span}, 0), lz4Compressed(t, data)...),
	}
	for name, img := range images {
		if err := os.WriteFile(p.hostPath(name), img, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(p.hostPath(name), 32+span); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a.cso.iso", "b.zso.iso"} {
		h := p.open(name, fioRdonly)
		if h <= 0 {
			t.Fatalf("OPEN %s: %d", name, h)
		}
		// The READ decompresses the whole block
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		got := p.read(h, 64)
		runtime.ReadMemStats(&after)
		if !bytes.Equal(got, data[:64]) {
			t.Errorf("READ %s: %q, want %q", name, got, data[:64])
		}
		if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4<<20 {
			t.Errorf("READ %s of a %d-byte block allocated %d MiB", name, len(data), alloc>>20)
		}
		p.close(h)
	}
}
