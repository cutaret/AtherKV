package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/cutaret/aetherkv/vfs"
)

const (
	WALBlockSize  = 32 * 1024 // 32 KiB fixed block size
	WALHeaderSize = 7         // 4B CRC32C + 2B Length + 1B FragmentType
)

type FragType uint8

const (
	FragFull   FragType = 1
	FragFirst  FragType = 2
	FragMiddle FragType = 3
	FragLast   FragType = 4
)

var walCRCTable = crc32.MakeTable(crc32.Castagnoli)

// WAL writes checksummed WriteBatches split across fixed 32 KiB blocks.
type WAL struct {
	file       vfs.File
	fileOffset int64
	blockRem   int
}

func CreateWAL(fs vfs.FS, path string) (*WAL, error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:       f,
		fileOffset: 0,
		blockRem:   WALBlockSize,
	}, nil
}

// AppendBatch writes a single WriteBatch to the WAL.
func (w *WAL) AppendBatch(wb *WriteBatch) error {
	return w.appendRawRecord(wb.Encode())
}

// AppendGroup performs group commit by writing multiple WriteBatches into the WAL buffer before a single Sync.
func (w *WAL) AppendGroup(batches []*WriteBatch, syncNow bool) error {
	for _, wb := range batches {
		if err := w.appendRawRecord(wb.Encode()); err != nil {
			return err
		}
	}
	if syncNow {
		return w.Sync()
	}
	return nil
}

func (w *WAL) appendRawRecord(payload []byte) error {
	left := len(payload)
	offset := 0
	isFirst := true

	for left > 0 || isFirst {
		if w.blockRem < WALHeaderSize {
			if w.blockRem > 0 {
				// Pad the remaining < 7 bytes of the 32 KiB block with zeros
				zeros := make([]byte, w.blockRem)
				if _, err := w.file.WriteAt(zeros, w.fileOffset); err != nil {
					return err
				}
				w.fileOffset += int64(w.blockRem)
			}
			w.blockRem = WALBlockSize
		}

		avail := w.blockRem - WALHeaderSize
		fragLen := min(left, avail)

		var fType FragType
		if isFirst && fragLen == left {
			fType = FragFull
		} else if isFirst {
			fType = FragFirst
		} else if fragLen == left {
			fType = FragLast
		} else {
			fType = FragMiddle
		}

		chunk := payload[offset : offset+fragLen]
		var hdr [WALHeaderSize]byte
		binary.LittleEndian.PutUint16(hdr[4:6], uint16(fragLen))
		hdr[6] = byte(fType)

		h := crc32.New(walCRCTable)
		h.Write(hdr[6:7])
		h.Write(chunk)
		binary.LittleEndian.PutUint32(hdr[0:4], h.Sum32())

		buf := make([]byte, WALHeaderSize+fragLen)
		copy(buf[0:WALHeaderSize], hdr[:])
		copy(buf[WALHeaderSize:], chunk)

		if _, err := w.file.WriteAt(buf, w.fileOffset); err != nil {
			return err
		}

		w.fileOffset += int64(len(buf))
		w.blockRem -= len(buf)
		offset += fragLen
		left -= fragLen
		isFirst = false
	}
	return nil
}

func (w *WAL) Sync() error {
	return w.file.Sync()
}

func (w *WAL) Close() error {
	return w.file.Close()
}

// RecoverWAL reads an existing WAL file, stops at the first torn or corrupt record,
// truncates the file to the last valid batch boundary, and returns all valid WriteBatches.
func RecoverWAL(fs vfs.FS, path string, bugs LSMBugConfig) ([]*WriteBatch, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size, err := f.Size()
	if err != nil || size == 0 {
		return nil, err
	}

	var batches []*WriteBatch
	var validEndOffset int64
	var recordStartOffset int64
	var assembling bytes.Buffer
	inFragment := false

	blockIdx := int64(0)
	for blockIdx*WALBlockSize < size {
		blockStart := blockIdx * WALBlockSize
		toRead := min(int64(WALBlockSize), size-blockStart)
		blockBuf := make([]byte, toRead)
		n, rErr := f.ReadAt(blockBuf, blockStart)
		if rErr != nil && rErr != io.EOF {
			break
		}
		blockBuf = blockBuf[:n]

		pos := 0
		corrupt := false
		for pos+WALHeaderSize <= len(blockBuf) {
			expectedCRC := binary.LittleEndian.Uint32(blockBuf[pos : pos+4])
			fragLen := int(binary.LittleEndian.Uint16(blockBuf[pos+4 : pos+6]))
			fType := FragType(blockBuf[pos+6])

			// Zero-padded trailer at end of 32 KiB block
			if expectedCRC == 0 && fragLen == 0 && fType == 0 {
				break
			}

			if !inFragment {
				recordStartOffset = blockStart + int64(pos)
			}

			// Check if fragment is truncated before declared length
			if pos+WALHeaderSize+fragLen > len(blockBuf) {
				if bugs.Bug08AcceptTornWAL {
					// Zoo #8: Accept torn tail record and inject partial corrupt entry instead of truncating!
					tornBytes := blockBuf[pos+WALHeaderSize:]
					corruptWB := NewWriteBatch(999999)
					corruptWB.Put([]byte("__torn_wal_key__"), bytes.Clone(tornBytes))
					batches = append(batches, corruptWB)
				}
				corrupt = true
				break
			}

			chunk := blockBuf[pos+WALHeaderSize : pos+WALHeaderSize+fragLen]
			h := crc32.New(walCRCTable)
			h.Write(blockBuf[pos+6 : pos+7])
			h.Write(chunk)

			if h.Sum32() != expectedCRC {
				if bugs.Bug08AcceptTornWAL {
					// Zoo #8: Ignore CRC mismatch on torn sector and accept corrupted payload!
					wb, decErr := DecodeWriteBatch(chunk)
					if decErr == nil {
						batches = append(batches, wb)
					} else {
						corruptWB := NewWriteBatch(999999)
						corruptWB.Put([]byte("__torn_wal_key__"), bytes.Clone(chunk))
						batches = append(batches, corruptWB)
					}
				}
				corrupt = true
				break
			}

			switch fType {
			case FragFull:
				wb, decErr := DecodeWriteBatch(chunk)
				if decErr != nil {
					corrupt = true
					break
				}
				batches = append(batches, wb)
				pos += WALHeaderSize + fragLen
				validEndOffset = blockStart + int64(pos)
				inFragment = false

			case FragFirst:
				assembling.Reset()
				assembling.Write(chunk)
				inFragment = true
				pos += WALHeaderSize + fragLen

			case FragMiddle:
				if !inFragment {
					corrupt = true
					break
				}
				assembling.Write(chunk)
				pos += WALHeaderSize + fragLen

			case FragLast:
				if !inFragment {
					corrupt = true
					break
				}
				assembling.Write(chunk)
				wb, decErr := DecodeWriteBatch(assembling.Bytes())
				if decErr != nil {
					corrupt = true
					break
				}
				batches = append(batches, wb)
				pos += WALHeaderSize + fragLen
				validEndOffset = blockStart + int64(pos)
				inFragment = false

			default:
				corrupt = true
			}

			if corrupt {
				break
			}
		}

		// Handle < 7 trailing non-zero bytes at EOF (torn header)
		if !corrupt && len(blockBuf)-pos > 0 && len(blockBuf)-pos < WALHeaderSize {
			rem := blockBuf[pos:]
			allZero := true
			for _, b := range rem {
				if b != 0 {
					allZero = false
					break
				}
			}
			if !allZero {
				if bugs.Bug08AcceptTornWAL {
					corruptWB := NewWriteBatch(999999)
					corruptWB.Put([]byte("__torn_wal_key__"), bytes.Clone(rem))
					batches = append(batches, corruptWB)
				}
				corrupt = true
			}
		}

		if corrupt {
			break
		}
		blockIdx++
	}

	if inFragment && !bugs.Bug08AcceptTornWAL {
		validEndOffset = recordStartOffset
	}

	if !bugs.Bug08AcceptTornWAL && validEndOffset < size {
		_ = f.Truncate(validEndOffset)
		_ = f.Sync()
	}

	return batches, nil
}