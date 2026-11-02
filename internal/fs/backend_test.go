package fs_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/pcm720/udpfsd/internal/fs"
)

// Shutdown closes every handle, the last one in the table included
func TestShutdownClosesAllHandles(t *testing.T) {
	root := t.TempDir()
	b, err := fs.NewBackend(fs.WithFSRoot(root))
	if err != nil {
		t.Fatal(err)
	}
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

	b.Shutdown()
	buf := make([]byte, 16)
	for _, h := range handles {
		if n, _, err := b.Read(h, 4, buf); err == nil {
			t.Errorf("handle %d still read %d bytes after Shutdown", h, n)
		}
	}
}
