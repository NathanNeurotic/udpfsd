package zso

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pcm720/udpfsd/internal/fs/compression/common"
	"github.com/pierrec/lz4/v4"
)

// ZISO/ZSO magic
var (
	zsoMagic   = []byte{'Z', 'S', 'O', 0}
	zisoMagic  = []byte{'Z', 'I', 'S', 'O'}
	Extensions = []string{".zso", ".ziso"}
)

// maxTrim is the most ReadBlock trims off the end of what it reads for an LZ4
// block to find where the block's data ends. The padding after a block is
// shorter than the alignment of the image, at most 2 KiB in images OPL reads,
// and ReadBlock reads at most 4 KiB for a 2048-byte block.
const maxTrim = 4 << 10

// ZSO implements BlockReader for ZSO/ZISO (LZ4 block-based) format.
type ZSO struct {
	f          *os.File
	stat       *common.CompressedInfo
	offsets    []uint32
	blockSize  int
	numBlocks  int
	size       int64
	fileSize   int64
	headerSize int64
	modTime    time.Time
	align      uint8
	ziso       bool
}

// Open opens a ZSO or ZISO file and parses its header.
func Open(path string) (*ZSO, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	z := &ZSO{f: f}
	if err := z.parseHeader(); err != nil {
		f.Close()
		return nil, err
	}
	return z, nil
}

func (z *ZSO) parseHeader() error {
	if err := z.checkHeader(); err != nil {
		return err
	}

	numOffsetEntries := z.numBlocks + 1
	if !z.ziso {
		numOffsetEntries = z.numBlocks
	}
	_, err := z.f.Seek(z.headerSize, io.SeekStart)
	if err != nil {
		return err
	}
	offsetBytes := make([]byte, numOffsetEntries*4)
	if _, err := io.ReadFull(z.f, offsetBytes); err != nil {
		return err
	}
	z.offsets = make([]uint32, numOffsetEntries)
	for i := 0; i < numOffsetEntries; i++ {
		z.offsets[i] = binary.LittleEndian.Uint32(offsetBytes[i*4 : (i+1)*4])
	}

	z.stat = common.NewCompressedInfo(filepath.Base(z.f.Name()), int64(z.size), z.modTime)
	return nil
}

// checkHeader reads the header and checks that the image it describes fits into the file.
// It doesn't read the block map.
func (z *ZSO) checkHeader() error {
	_, err := z.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	b := make([]byte, 24)
	if _, err := io.ReadFull(z.f, b); err != nil {
		return err
	}
	magic := b[0:4]

	if bytes.Equal(magic, zisoMagic) {
		z.ziso = true
	} else if !bytes.Equal(magic, zsoMagic) {
		return fmt.Errorf("invalid ZSO magic: %q", magic)
	}

	z.headerSize = int64(binary.LittleEndian.Uint32(b[4:8]))
	z.size = int64(binary.LittleEndian.Uint64(b[8:16]))
	z.blockSize = int(binary.LittleEndian.Uint32(b[16:20]))
	if z.ziso {
		z.align = b[21]
	}
	if z.blockSize <= 0 || z.blockSize > common.MaxBlockSize {
		return fmt.Errorf("invalid ZSO: block_size is %d", z.blockSize)
	}
	if z.size < 0 {
		return fmt.Errorf("invalid ZSO: uncompressed size %d is too large", uint64(z.size))
	}
	fstat, err := z.f.Stat()
	if err != nil {
		return err
	}
	z.fileSize = fstat.Size()
	// The block map has an entry for every block, and ZISO one more for the end of the last block,
	// so it has to fit into the file. Checking this first keeps a corrupt header from
	// making the block map larger than memory.
	numBlocks := z.size / int64(z.blockSize)
	if z.size%int64(z.blockSize) != 0 {
		numBlocks++
	}
	if numBlocks > common.MaxBlocks {
		return fmt.Errorf("invalid ZSO: %d blocks is more than a disc image has", numBlocks)
	}
	endEntries := int64(0)
	if z.ziso {
		endEntries = 1
	}
	if numBlocks > (z.fileSize-z.headerSize)/4-endEntries {
		return fmt.Errorf("invalid ZSO: block map for %d blocks does not fit into the file", numBlocks)
	}
	z.numBlocks = int(numBlocks)
	z.modTime = fstat.ModTime()
	return nil
}

func (z *ZSO) BlockSize() int          { return z.blockSize }
func (z *ZSO) NumBlocks() int          { return z.numBlocks }
func (z *ZSO) UncompressedSize() int64 { return z.size }

func (z *ZSO) Stat() os.FileInfo {
	return z.stat
}

func (z *ZSO) ReadBlock(blockIdx int) ([]byte, error) {
	if blockIdx < 0 || blockIdx >= z.numBlocks {
		return nil, fmt.Errorf("block index %d out of range", blockIdx)
	}
	rawOffset := z.offsets[blockIdx]
	var offset int64
	var compressedSize int
	if z.ziso {
		nextRaw := z.offsets[blockIdx+1]
		offset = int64(rawOffset&0x7FFFFFFF) << z.align
		nextOffset := int64(nextRaw&0x7FFFFFFF) << z.align
		compressedSize = int(nextOffset - offset)
	} else {
		if blockIdx+1 < z.numBlocks {
			compressedSize = int(z.offsets[blockIdx+1] - rawOffset)
		} else {
			info, err := z.f.Stat()
			if err != nil {
				return nil, err
			}
			compressedSize = int(info.Size() - int64(rawOffset))
		}
		offset = int64(rawOffset)
	}
	// A corrupt block map can point anywhere
	if offset < 0 || compressedSize < 0 || int64(compressedSize) > z.fileSize-offset {
		return nil, fmt.Errorf("invalid ZSO: block %d (%d bytes at %d) is outside of the file", blockIdx, compressedSize, offset)
	}
	// The data of a compressed block is smaller than the block, or the block would
	// be stored uncompressed, but a corrupt block map can make it run on for most
	// of the file. Read no more than twice the block size: the retries below trim
	// what follows the data one byte at a time.
	compressedSize = min(compressedSize, 2*z.blockSize)
	uncompressed := (rawOffset & 0x80000000) != 0

	_, err := z.f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	compressed := make([]byte, compressedSize)
	if _, err := io.ReadFull(z.f, compressed); err != nil {
		return nil, err
	}
	if uncompressed {
		out := make([]byte, z.blockSize)
		copy(out, compressed)
		return out, nil
	}
	// LZ4 with OPL-style retry (trim trailing byte for alignment padding).
	// Each attempt writes out from the start, and can decode most of a block
	// before it fails, so trim no more than maxTrim bytes.
	out := make([]byte, z.blockSize)
	for trim := range min(maxTrim, len(compressed)) {
		n, err := lz4.UncompressBlock(compressed[:len(compressed)-trim], out)
		if err == nil {
			return out[:n], nil
		}
	}
	return nil, fmt.Errorf("LZ4 decompression failed for block %d", blockIdx)
}

func (z *ZSO) Close() error {
	if z.f == nil {
		return nil
	}
	err := z.f.Close()
	z.f = nil
	return err
}

func GetStat(path string) *common.CompressedInfo {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	// Report only images that Open accepts
	z := &ZSO{f: f}
	if z.checkHeader() != nil {
		return nil
	}
	return common.NewCompressedInfo(filepath.Base(path), z.size, z.modTime)
}
