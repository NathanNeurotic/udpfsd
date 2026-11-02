package fs_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pcm720/udpfsd/internal/fs"
)

// openFiles returns the number of files this process has open in dir, or -1
// where it can't be counted. Files that other tests left for the garbage
// collector to close, which it can do at any time, don't count.
func openFiles(dir string) int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	if dir, err = filepath.EvalSymlinks(dir); err != nil {
		return -1
	}
	n := 0
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err == nil && strings.HasPrefix(target, dir+string(filepath.Separator)) {
			n++
		}
	}
	return n
}

// Shutdown closes every handle, the last one in the table included
func TestShutdownClosesAllHandles(t *testing.T) {
	root := t.TempDir()
	b, err := fs.NewBackend(fs.WithFSRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	before := openFiles(root)
	var handles []int32
	for i := range 64 {
		name := strconv.Itoa(i) + ".bin"
		if err := os.WriteFile(filepath.Join(root, name), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
		h, _, err := b.Open(name, os.O_RDONLY, false)
		if err != nil || h <= 0 {
			t.Fatalf("Open %s: %d, %v", name, h, err)
		}
		handles = append(handles, h)
	}
	buf := make([]byte, 16)
	for _, h := range handles {
		if n, _, err := b.Read(h, 4, buf); err != nil || n != 4 {
			t.Fatalf("handle %d read %d bytes before Shutdown, %v; want 4 bytes", h, n, err)
		}
	}

	b.Shutdown()
	for _, h := range handles {
		if n, _, err := b.Read(h, 4, buf); err == nil {
			t.Errorf("handle %d still read %d bytes after Shutdown", h, n)
		}
	}
	// The files are closed, not only their handles refused
	if before >= 0 {
		if open := openFiles(root) - before; open != 0 {
			t.Errorf("Shutdown left %d of the 64 files open", open)
		}
	}
}
