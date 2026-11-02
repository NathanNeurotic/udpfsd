package cso

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pcm720/udpfsd/internal/fs/compression/common"
)

var (
	csoMagic   = []byte{'C', 'I', 'S', 'O'}
	Extensions = []string{".cso", ".ciso"}
)

// CSO implements BlockReader for CSO/CISO (zlib block-based) format.
type CSO struct {
	f         *os.File
	stat      *common.CompressedInfo
	offsets   []uint32
	blockSize int
	numBlocks int
	size      int64
	fileSize  int64
	modTime   time.Time
	align     uint8
}

// Open opens a CSO file and parses its header.
func Open(path string) (*CSO, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	c := &CSO{f: f}
	if err := c.parseHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return c, nil
}

func (c *CSO) parseHeader() error {
	if err := c.checkHeader(); err != nil {
		return err
	}

	// Seek to the start of the block map and initialize offset map
	_, err := c.f.Seek(24, io.SeekStart)
	if err != nil {
		return err
	}
	offsetBytes := make([]byte, (c.numBlocks+1)*4)
	if _, err := io.ReadFull(c.f, offsetBytes); err != nil {
		return err
	}
	c.offsets = make([]uint32, c.numBlocks+1)
	for i := 0; i <= c.numBlocks; i++ {
		c.offsets[i] = binary.LittleEndian.Uint32(offsetBytes[i*4 : (i+1)*4])
	}

	c.stat = common.NewCompressedInfo(filepath.Base(c.f.Name()), int64(c.size), c.modTime)
	return nil
}

// checkHeader reads the header and checks that the image it describes fits into the file.
// It doesn't read the block map.
func (c *CSO) checkHeader() error {
	_, err := c.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	b := make([]byte, 24)
	if _, err := io.ReadFull(c.f, b); err != nil {
		return err
	}
	// Validate CISO header
	if !bytes.Equal(b[0:4], csoMagic) {
		return fmt.Errorf("invalid CSO magic: 0x%08X", b[0:4])
	}
	c.size = int64(binary.LittleEndian.Uint64(b[8:16]))
	c.blockSize = int(binary.LittleEndian.Uint32(b[16:20]))
	c.align = b[21]
	if c.blockSize <= 0 || c.blockSize > common.MaxBlockSize {
		return fmt.Errorf("invalid CSO: block_size is %d", c.blockSize)
	}
	if c.size < 0 {
		return fmt.Errorf("invalid CSO: uncompressed size %d is too large", uint64(c.size))
	}
	fstat, err := c.f.Stat()
	if err != nil {
		return err
	}
	c.fileSize = fstat.Size()
	// The block map has an entry for every block and one for the end of the last block,
	// so it has to fit into the file. Checking this first keeps a corrupt header from
	// making the block map larger than memory.
	numBlocks := c.size / int64(c.blockSize)
	if c.size%int64(c.blockSize) != 0 {
		numBlocks++
	}
	if numBlocks > common.MaxBlocks {
		return fmt.Errorf("invalid CSO: %d blocks is more than a disc image has", numBlocks)
	}
	if numBlocks >= (c.fileSize-24)/4 {
		return fmt.Errorf("invalid CSO: block map for %d blocks does not fit into the file", numBlocks)
	}
	c.numBlocks = int(numBlocks)
	c.modTime = fstat.ModTime()
	return nil
}

func (c *CSO) BlockSize() int          { return c.blockSize }
func (c *CSO) NumBlocks() int          { return c.numBlocks }
func (c *CSO) UncompressedSize() int64 { return c.size }

func (c *CSO) ReadBlock(blockIdx int) ([]byte, error) {
	if blockIdx < 0 || blockIdx >= c.numBlocks {
		return nil, fmt.Errorf("block index %d out of range", blockIdx)
	}
	// Get block properties
	isPlain := (c.offsets[blockIdx] & 0x80000000) != 0
	readOffset := int64(c.offsets[blockIdx]&0x7FFFFFFF) << c.align
	readSize := c.blockSize
	if !isPlain {
		// If the block is compressed, get the read size by getting the next block offset
		readSize = int((int64(c.offsets[blockIdx+1]&0x7FFFFFFF) << c.align) - readOffset)
	}
	// A corrupt block map can point anywhere
	if readOffset < 0 || readSize < 0 || int64(readSize) > c.fileSize-readOffset {
		return nil, fmt.Errorf("invalid CSO: block %d (%d bytes at %d) is outside of the file", blockIdx, readSize, readOffset)
	}
	// Read the block
	_, err := c.f.Seek(readOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, readSize)
	if _, err := io.ReadFull(c.f, buf); err != nil {
		return nil, err
	}
	if isPlain {
		return buf, nil
	}
	// Try raw deflate first, then zlib
	out, err := decompressDeflate(buf, c.blockSize)
	if err != nil {
		out, err = decompressZlib(buf, c.blockSize)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decompressDeflate(compressed []byte, blockSize int) ([]byte, error) {
	dec := flate.NewReader(bytes.NewReader(compressed))
	out := make([]byte, 0, blockSize)
	buf := make([]byte, 4096)
	for {
		n, err := dec.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			dec.Close()
			return nil, err
		}
	}
	dec.Close()
	if len(out) < blockSize {
		pad := make([]byte, blockSize-len(out))
		out = append(out, pad...)
	}
	return out, nil
}

func decompressZlib(compressed []byte, blockSize int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(out) < blockSize {
		pad := make([]byte, blockSize-len(out))
		out = append(out, pad...)
	}
	return out, nil
}

func (c *CSO) Stat() os.FileInfo {
	return c.stat
}

func (c *CSO) Close() error {
	if c.f == nil {
		return nil
	}
	err := c.f.Close()
	c.f = nil
	return err
}

func GetStat(path string) *common.CompressedInfo {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	// Report only images that Open accepts
	c := &CSO{f: f}
	if c.checkHeader() != nil {
		return nil
	}
	return common.NewCompressedInfo(filepath.Base(path), c.size, c.modTime)
}
