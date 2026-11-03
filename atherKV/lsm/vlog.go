package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/cutaret/aetherkv/vfs"
)

const (
	VLogHeaderSize = 12 // 4B CRC32C + 4B KeyLen + 4B ValLen
)

type vlogRecord struct {
	key []byte
	val []byte
	ptr VLogPointer
}

// ValueLog manages append-only WiscKey value segments (vlog-%06d.log).
type ValueLog struct {
	fs           vfs.FS
	dir          string
	activeSegID  uint64
	activeFile   vfs.File
	activeOffset uint64
	maxSegBytes  uint64
	bugs         LSMBugConfig
	bytesWritten uint64
}

func OpenValueLog(fs vfs.FS, dir string, maxSegBytes uint64, bugs LSMBugConfig) (*ValueLog, error) {
	if maxSegBytes == 0 {
		maxSegBytes = 16 * 1024 * 1024 // Default 16 MiB segment size
	}
	vl := &ValueLog{
		fs:          fs,
		dir:         dir,
		maxSegBytes: maxSegBytes,
		bugs:        bugs,
	}
	if err := vl.recover(); err != nil {
		return nil, err
	}
	return vl, nil
}

func (vl *ValueLog) segPath(segID uint64) string {
	return filepath.Join(vl.dir, fmt.Sprintf("vlog-%06d.log", segID))
}

func (vl *ValueLog) listSegments() ([]uint64, error) {
	files, err := vl.fs.List(vl.dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, f := range files {
		if strings.HasPrefix(f, "vlog-") && strings.HasSuffix(f, ".log") {
			numStr := strings.TrimSuffix(strings.TrimPrefix(f, "vlog-"), ".log")
			if id, pErr := strconv.ParseUint(numStr, 10, 64); pErr == nil {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return ids, nil
}

func (vl *ValueLog) recover() error {
	ids, err := vl.listSegments()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return vl.rotateSegment(1)
	}

	lastID := ids[len(ids)-1]
	f, err := vl.fs.Open(vl.segPath(lastID))
	if err != nil {
		return err
	}
	sz, err := f.Size()
	if err != nil {
		_ = f.Close()
		return err
	}
	vl.activeSegID = lastID
	vl.activeFile = f
	vl.activeOffset = uint64(sz)
	return nil
}

func (vl *ValueLog) rotateSegment(newSegID uint64) error {
	if vl.activeFile != nil {
		_ = vl.activeFile.Sync()
		_ = vl.activeFile.Close()
	}
	f, err := vl.fs.Create(vl.segPath(newSegID))
	if err != nil {
		return err
	}
	_ = f.Sync()
	_ = vl.fs.SyncDir(vl.dir)
	vl.activeSegID = newSegID
	vl.activeFile = f
	vl.activeOffset = 0
	return nil
}

// Append writes (key, value) into the active ValueLog segment and returns its VLogPointer.
func (vl *ValueLog) Append(key, value []byte) (VLogPointer, error) {
	recSize := uint64(VLogHeaderSize + len(key) + len(value))
	if vl.activeOffset > 0 && vl.activeOffset+recSize > vl.maxSegBytes {
		if err := vl.rotateSegment(vl.activeSegID + 1); err != nil {
			return VLogPointer{}, err
		}
	}

	valCRC := crc32.Checksum(value, walCRCTable)
	var hdr [VLogHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], valCRC)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(value)))

	buf := make([]byte, recSize)
	copy(buf[0:VLogHeaderSize], hdr[:])
	copy(buf[VLogHeaderSize:VLogHeaderSize+len(key)], key)
	copy(buf[VLogHeaderSize+len(key):], value)

	recOffset := vl.activeOffset
	if _, err := vl.activeFile.WriteAt(buf, int64(recOffset)); err != nil {
		return VLogPointer{}, err
	}
	vl.activeOffset += recSize
	vl.bytesWritten += recSize

	return VLogPointer{
		SegmentID: vl.activeSegID,
		Offset:    recOffset,
		Length:    uint32(recSize),
		Checksum:  valCRC,
	}, nil
}

func (vl *ValueLog) Sync() error {
	if vl.activeFile != nil {
		return vl.activeFile.Sync()
	}
	return nil
}

// Read resolves a VLogPointer to its underlying value, verifying segment bounds and CRC32C.
func (vl *ValueLog) Read(ptr VLogPointer) ([]byte, error) {
	f, err := vl.fs.Open(vl.segPath(ptr.SegmentID))
	if err != nil {
		return nil, ErrDanglingVLog
	}
	defer f.Close()

	fileSize, err := f.Size()
	if err != nil {
		return nil, err
	}

	if ptr.Length < VLogHeaderSize {
		return nil, ErrDanglingVLog
	}

	if int64(ptr.Offset)+int64(ptr.Length) > fileSize {
		if vl.bugs.Bug17AcceptTornVLog && int64(ptr.Offset) < fileSize {
			// Zoo #17: Return partial torn tail value without checking length/CRC!
			avail := fileSize - int64(ptr.Offset)
			raw := make([]byte, avail)
			_, _ = f.ReadAt(raw, int64(ptr.Offset))
			if len(raw) > VLogHeaderSize {
				return raw[VLogHeaderSize:], nil
			}
			return raw, nil
		}
		return nil, ErrDanglingVLog
	}

	buf := make([]byte, ptr.Length)
	n, rErr := f.ReadAt(buf, int64(ptr.Offset))
	if (rErr != nil && rErr != io.EOF) || uint32(n) != ptr.Length {
		return nil, ErrDanglingVLog
	}

	hdrCRC := binary.LittleEndian.Uint32(buf[0:4])
	kLen := binary.LittleEndian.Uint32(buf[4:8])
	vLen := binary.LittleEndian.Uint32(buf[8:12])
	if VLogHeaderSize+kLen+vLen != ptr.Length {
		if vl.bugs.Bug17AcceptTornVLog {
			return bytes.Clone(buf[VLogHeaderSize:]), nil
		}
		return nil, ErrCorruptVLogCRC
	}

	val := buf[VLogHeaderSize+kLen : VLogHeaderSize+kLen+vLen]
	actualCRC := crc32.Checksum(val, walCRCTable)
	if actualCRC != ptr.Checksum || actualCRC != hdrCRC {
		if vl.bugs.Bug17AcceptTornVLog {
			// Zoo #17: Ignore CRC32C mismatch on torn value-log sector and return corrupted value!
			return bytes.Clone(val), nil
		}
		return nil, ErrCorruptVLogCRC
	}

	return bytes.Clone(val), nil
}

func (vl *ValueLog) readSegmentRecords(segID uint64) ([]vlogRecord, error) {
	f, err := vl.fs.Open(vl.segPath(segID))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sz, err := f.Size()
	if err != nil {
		return nil, err
	}

	var records []vlogRecord
	var offset uint64
	for int64(offset+VLogHeaderSize) <= sz {
		var hdr [VLogHeaderSize]byte
		if _, err := f.ReadAt(hdr[:], int64(offset)); err != nil {
			break
		}
		crc := binary.LittleEndian.Uint32(hdr[0:4])
		kLen := binary.LittleEndian.Uint32(hdr[4:8])
		vLen := binary.LittleEndian.Uint32(hdr[8:12])
		recLen := uint64(VLogHeaderSize + kLen + vLen)
		if int64(offset+recLen) > sz {
			break
		}

		payload := make([]byte, kLen+vLen)
		if _, err := f.ReadAt(payload, int64(offset+VLogHeaderSize)); err != nil {
			break
		}
		key := bytes.Clone(payload[:kLen])
		val := bytes.Clone(payload[kLen:])
		if crc32.Checksum(val, walCRCTable) != crc {
			break
		}

		records = append(records, vlogRecord{
			key: key,
			val: val,
			ptr: VLogPointer{
				SegmentID: segID,
				Offset:    offset,
				Length:    uint32(recLen),
				Checksum:  crc,
			},
		})
		offset += recLen
	}
	return records, nil
}

func (vl *ValueLog) Close() error {
	if vl.activeFile != nil {
		return vl.activeFile.Close()
	}
	return nil
}
