package common

import (
	"os"
	"time"
)

// MaxBlocks is the largest number of blocks a CSO or ZSO image is accepted with.
// A dual-layer DVD image (8.5 GB) has fewer even in blocks of 256 bytes, so more
// blocks come from a corrupt header, and reading its block map could exhaust
// memory or, on 32-bit builds, overflow the size of the map.
const MaxBlocks = 1 << 25

// CompressedInfo holds uncompressed size and format name for stat/listdir.
type CompressedInfo struct {
	modTime time.Time
	name    string
	size    int64
}

func NewCompressedInfo(name string, size int64, modTime time.Time) *CompressedInfo {
	return &CompressedInfo{
		name:    name,
		size:    size,
		modTime: modTime,
	}
}

func (c *CompressedInfo) Name() string {
	return c.name
}
func (c *CompressedInfo) Size() int64 {
	return c.size
}
func (c *CompressedInfo) Mode() os.FileMode {
	return 0
}
func (c *CompressedInfo) ModTime() time.Time {
	return c.modTime
}
func (c *CompressedInfo) IsDir() bool {
	return false
}
func (c *CompressedInfo) Sys() any {
	return nil
}
