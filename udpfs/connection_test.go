package udpfs_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

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

func newPeer(t *testing.T, opts ...fs.BackendOptFunc) *peer {
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

// openFiles returns the number of files open in this process
func openFiles(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot count open files: %v", err)
	}
	return len(entries)
}

// Listing compressed images and GETSTAT of their .iso names read the image
// headers, and must close the images afterwards
func TestCompressedImageStatClosesFiles(t *testing.T) {
	openFiles(t) // skip early where open files cannot be counted
	p := newPeer(t, fs.WithCompression())
	p.copyTestImage("test.cso", "a.cso")
	p.copyTestImage("test.zso", "b.zso")
	// The garbage collector closes files nothing refers to, which would hide a leak
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	before := openFiles(t)
	for range 10 {
		if got := p.list(""); len(got) != 2 {
			t.Fatalf("listing: %+v, want 2 entries", got)
		}
	}
	if leaked := openFiles(t) - before; leaked != 0 {
		t.Errorf("listing 2 compressed images 10 times left %d files open", leaked)
	}

	before = openFiles(t)
	for range 10 {
		for _, name := range []string{"a.cso.iso", "b.zso.iso"} {
			if res, _ := p.getstat(name); res != 0 {
				t.Fatalf("GETSTAT %s: %d", name, res)
			}
		}
	}
	if leaked := openFiles(t) - before; leaked != 0 {
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
