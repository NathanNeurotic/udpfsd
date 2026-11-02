package udpfs

import (
	"fmt"
	"os"
)

// FS is the filesystem abstraction used by the UDPFS protocol layer.
// The protocol parses packets and builds replies; it calls FS only for operations and data.
type FS interface {
	// Open opens a file or directory. flags is the PS2 open flags (bytes 2-3 of payload).
	// Returns (handle, stat, nil) on success, or (errno, StatInfo{}, nil) with errno < 0 for protocol errors, or (0, StatInfo{}, err) for system errors (errno will be derived from err).
	Open(path string, flag int, isDir bool) (handle int32, stat StatInfo, err error)
	// Close closes a handle. Returns nil or error (mapped to errno by protocol).
	Close(handle int32) error
	// Read reads up to size bytes from handle into readBuffer. Returns bytes read (>=0), data slice, and optional error.
	Read(handle int32, size uint32, readBuffer []byte) (n int32, data []byte, err error)
	// WriteStart starts a multi-chunk write.
	WriteStart(handle int32) error
	// WriteChunk adds a chunk. Returns done=true when all chunks received; then protocol will call CompleteWrite.
	WriteChunk(handle int32, chunkNr, chunkSize, totalChunks uint16, chunk []byte) (done bool, err error)
	// CompleteWrite completes the current write (regular or block). Returns bytes written for regular write, or 0 for block write on success.
	CompleteWrite(handle int32) (n int32, err error)
	// Lseek seeks. Returns new position or error.
	Lseek(handle int32, offset int64, whence int) (position int64, err error)
	// Dread returns the next directory entry. ok=false means no more entries (result 0); name and stat are for the next entry.
	Dread(handle int32) (ok bool, name string, stat StatInfo, err error)
	// Getstat returns stat for path. Empty path means block device info if available.
	Getstat(path string) (stat StatInfo, err error)
	// Mkdir creates a directory.
	Mkdir(path string) error
	// Remove removes a file.
	Remove(path string) error
	// Rmdir removes a directory.
	Rmdir(path string) error
	// Bread reads sectors into readBuffer. Returns data (exactly sectorCount*sectorSize bytes or shorter on error).
	Bread(handle int32, sectorNr int64, sectorCount uint16, readBuffer []byte) (data []byte, err error)
	// BwriteStart starts a block write. Subsequent chunks are delivered via WriteChunk (same as regular write).
	BwriteStart(handle int32, sectorNr int64, sectorCount uint16) error
}

// Errno is a negative PS2 errno for reply result fields.
type Errno int32

func (e Errno) Error() string {
	return fmt.Sprintf("UDPFS errno %d", e)
}

const (
	EOK     = 0
	ENOENT  = 2
	EIO     = 5
	EBADF   = 9
	EACCES  = 13
	EEXIST  = 17
	ENODEV  = 19
	ENOTDIR = 20
	EINVAL  = 22
	EMFILE  = 24
)

// errToErrno maps common errors to PS2 errno (positive); returns 5 for unknown.
func errToErrno(err error) int32 {
	if err == nil {
		return EOK
	}
	if os.IsNotExist(err) {
		return ENOENT
	}
	if os.IsExist(err) {
		return EEXIST
	}
	if os.IsPermission(err) {
		return EACCES
	}
	if e, ok := err.(Errno); ok && e < 0 {
		return int32(-e)
	}
	return EIO
}

// Converts between UDPFS flags and OS flags
func udpfsFlagsToOSFlags(flags uint16) int {
	parseMode := func() int {
		f := 0
		if (flags & Flag_Append) > 0 {
			f |= os.O_APPEND
		}
		if (flags & Flag_Create) > 0 {
			f |= os.O_CREATE
		}
		if (flags & Flag_Truncate) > 0 {
			f |= os.O_TRUNC
		}
		return f
	}

	switch flags & 0x03 {
	case Flag_ReadOnly:
		return os.O_RDONLY
	case Flag_WriteOnly:
		return os.O_WRONLY | parseMode()
	case Flag_ReadWrite:
		return os.O_RDWR | parseMode()
	}

	return os.O_RDONLY
}

// Reports whether OS flags open a file without writing, creating or truncating it
func isReadOnlyOpen(flag int) bool {
	return flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) == 0
}
