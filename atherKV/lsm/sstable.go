package lsm

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"slices"

	"github.com/cutaret/aetherkv/vfs"
)

const (
	SSTDataBlockTargetSize = 4096       // 4 KiB target data block size
	SSTFooterSize          = 40         // 8B BloomOff + 8B BloomLen + 8B IdxOff + 8B IdxLen + 4B CRC + 4B Magic
	SSTMagicNumber         = 0xAE74E255 // "AETH" magic footer identifier
)

type indexEntry struct {
	maxUserKey []byte
	offset     uint64
	size       uint64
}

type sstKV struct {
	key   InternalKey
	value []byte
}

// WriteSSTable flushes a sorted sequence of InternalKey/Value pairs from an Iterator into a new SSTable file.
func WriteSSTable(fs vfs.FS, path string, it Iterator, bitsPerKey int) (minUserKey, maxUserKey []byte, err error) {
	f, err := fs.Create(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var offset uint64
	var blockBuf bytes.Buffer
	var lastUserKeyInBlock []byte
	var indexEntries []indexEntry
	var bloomKeys [][]byte

	flushDataBlock := func() error {
		if blockBuf.Len() == 0 {
			return nil
		}
		raw := blockBuf.Bytes()
		crc := crc32.Checksum(raw, walCRCTable)
		var crcBytes [4]byte
		binary.LittleEndian.PutUint32(crcBytes[:], crc)

		blockWithCRC := make([]byte, len(raw)+4)
		copy(blockWithCRC, raw)
		copy(blockWithCRC[len(raw):], crcBytes[:])

		if _, wErr := f.WriteAt(blockWithCRC, int64(offset)); wErr != nil {
			return wErr
		}
		indexEntries = append(indexEntries, indexEntry{
			maxUserKey: bytes.Clone(lastUserKeyInBlock),
			offset:     offset,
			size:       uint64(len(blockWithCRC)),
		})
		offset += uint64(len(blockWithCRC))
		blockBuf.Reset()
		return nil
	}

	it.SeekToFirst()
	for it.Valid() {
		ikey := it.Key()
		val := it.Value()

		if minUserKey == nil {
			minUserKey = bytes.Clone(ikey.UserKey)
		}
		maxUserKey = bytes.Clone(ikey.UserKey)
		lastUserKeyInBlock = ikey.UserKey

		if len(bloomKeys) == 0 || !bytes.Equal(bloomKeys[len(bloomKeys)-1], ikey.UserKey) {
			bloomKeys = append(bloomKeys, bytes.Clone(ikey.UserKey))
		}

		// Encode entry into current data block: [SeqNum(8B) | Type(1B) | KeyLen(4B) | ValLen(4B) | Key | Val]
		var hdr [17]byte
		binary.LittleEndian.PutUint64(hdr[0:8], ikey.SeqNum)
		hdr[8] = byte(ikey.Type)
		binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(ikey.UserKey)))
		binary.LittleEndian.PutUint32(hdr[13:17], uint32(len(val)))
		blockBuf.Write(hdr[:])
		blockBuf.Write(ikey.UserKey)
		blockBuf.Write(val)

		if blockBuf.Len() >= SSTDataBlockTargetSize {
			if err := flushDataBlock(); err != nil {
				return nil, nil, err
			}
		}
		it.Next()
	}

	if err := flushDataBlock(); err != nil {
		return nil, nil, err
	}

	// 2. Write Bloom Filter Block + 4B CRC32C
	bloomRaw := BuildBloomFilter(bloomKeys, bitsPerKey)
	bloomOffset := offset
	bloomBlock, err := writeChecksummedBlock(f, bloomOffset, bloomRaw)
	if err != nil {
		return nil, nil, err
	}
	offset += uint64(len(bloomBlock))

	// 3. Write Index Block + 4B CRC32C
	var idxBuf bytes.Buffer
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(len(indexEntries)))
	idxBuf.Write(countBuf[:])
	for _, ie := range indexEntries {
		var meta [20]byte
		binary.LittleEndian.PutUint32(meta[0:4], uint32(len(ie.maxUserKey)))
		binary.LittleEndian.PutUint64(meta[4:12], ie.offset)
		binary.LittleEndian.PutUint64(meta[12:20], ie.size)
		idxBuf.Write(meta[:])
		idxBuf.Write(ie.maxUserKey)
	}
	indexOffset := offset
	idxBlock, err := writeChecksummedBlock(f, indexOffset, idxBuf.Bytes())
	if err != nil {
		return nil, nil, err
	}
	offset += uint64(len(idxBlock))

	// 4. Write 40-Byte Fixed Footer
	var footer [SSTFooterSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], bloomOffset)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(bloomBlock)))
	binary.LittleEndian.PutUint64(footer[16:24], indexOffset)
	binary.LittleEndian.PutUint64(footer[24:32], uint64(len(idxBlock)))
	footerCRC := crc32.Checksum(footer[0:32], walCRCTable)
	binary.LittleEndian.PutUint32(footer[32:36], footerCRC)
	binary.LittleEndian.PutUint32(footer[36:40], SSTMagicNumber)

	if _, err := f.WriteAt(footer[:], int64(offset)); err != nil {
		return nil, nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, nil, err
	}
	return minUserKey, maxUserKey, nil
}

func writeChecksummedBlock(f vfs.File, offset uint64, raw []byte) ([]byte, error) {
	crc := crc32.Checksum(raw, walCRCTable)
	out := make([]byte, len(raw)+4)
	copy(out, raw)
	binary.LittleEndian.PutUint32(out[len(raw):], crc)
	if _, err := f.WriteAt(out, int64(offset)); err != nil {
		return nil, err
	}
	return out, nil
}

func readAndVerifyBlock(f vfs.File, offset, size uint64) ([]byte, error) {
	if size < 4 {
		return nil, ErrCorruptBlock
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, int64(offset))
	if err != nil || uint64(n) != size {
		return nil, ErrCorruptBlock
	}
	payload := buf[:size-4]
	expectedCRC := binary.LittleEndian.Uint32(buf[size-4:])
	if crc32.Checksum(payload, walCRCTable) != expectedCRC {
		return nil, ErrCorruptBlock
	}
	return payload, nil
}

// SSTableReader reads and verifies an immutable SSTable file.
type SSTableReader struct {
	fs          vfs.FS
	path        string
	bloomFilter []byte
	index       []indexEntry
}

func OpenSSTable(fs vfs.FS, path string) (*SSTableReader, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fileSize, err := f.Size()
	if err != nil || fileSize < SSTFooterSize {
		return nil, ErrCorruptFooter
	}

	var footer [SSTFooterSize]byte
	if _, err := f.ReadAt(footer[:], fileSize-SSTFooterSize); err != nil {
		return nil, ErrCorruptFooter
	}

	magic := binary.LittleEndian.Uint32(footer[36:40])
	expectedCRC := binary.LittleEndian.Uint32(footer[32:36])
	if magic != SSTMagicNumber || crc32.Checksum(footer[0:32], walCRCTable) != expectedCRC {
		return nil, ErrCorruptFooter
	}

	bloomOff := binary.LittleEndian.Uint64(footer[0:8])
	bloomSize := binary.LittleEndian.Uint64(footer[8:16])
	idxOff := binary.LittleEndian.Uint64(footer[16:24])
	idxSize := binary.LittleEndian.Uint64(footer[24:32])

	bloomPayload, err := readAndVerifyBlock(f, bloomOff, bloomSize)
	if err != nil {
		return nil, err
	}
	idxPayload, err := readAndVerifyBlock(f, idxOff, idxSize)
	if err != nil || len(idxPayload) < 4 {
		return nil, ErrCorruptBlock
	}

	count := binary.LittleEndian.Uint32(idxPayload[0:4])
	pos := 4
	index := make([]indexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+20 > len(idxPayload) {
			return nil, ErrCorruptBlock
		}
		kLen := int(binary.LittleEndian.Uint32(idxPayload[pos : pos+4]))
		bOff := binary.LittleEndian.Uint64(idxPayload[pos+4 : pos+12])
		bSize := binary.LittleEndian.Uint64(idxPayload[pos+12 : pos+20])
		pos += 20
		if pos+kLen > len(idxPayload) {
			return nil, ErrCorruptBlock
		}
		maxKey := bytes.Clone(idxPayload[pos : pos+kLen])
		pos += kLen
		index = append(index, indexEntry{
			maxUserKey: maxKey,
			offset:     bOff,
			size:       bSize,
		})
	}

	return &SSTableReader{
		fs:          fs,
		path:        path,
		bloomFilter: bloomPayload,
		index:       index,
	}, nil
}

func decodeDataBlockEntries(payload []byte) ([]sstKV, error) {
	var entries []sstKV
	pos := 0
	for pos < len(payload) {
		if pos+17 > len(payload) {
			return nil, ErrCorruptBlock
		}
		seq := binary.LittleEndian.Uint64(payload[pos : pos+8])
		vType := ValueType(payload[pos+8])
		kLen := int(binary.LittleEndian.Uint32(payload[pos+9 : pos+13]))
		vLen := int(binary.LittleEndian.Uint32(payload[pos+13 : pos+17]))
		pos += 17
		if pos+kLen+vLen > len(payload) {
			return nil, ErrCorruptBlock
		}
		k := bytes.Clone(payload[pos : pos+kLen])
		pos += kLen
		v := bytes.Clone(payload[pos : pos+vLen])
		pos += vLen
		entries = append(entries, sstKV{
			key:   InternalKey{UserKey: k, SeqNum: seq, Type: vType},
			value: v,
		})
	}
	return entries, nil
}

// Get checks the Bloom filter first, then binary-searches the Index Block and verifies Data Block CRC32C.
func (r *SSTableReader) Get(userKey []byte, snapshotSeq uint64) (val []byte, vType ValueType, found bool, err error) {
	if !BloomMayContain(r.bloomFilter, userKey) {
		return nil, 0, false, nil
	}

	blockIdx, _ := slices.BinarySearchFunc(r.index, userKey, func(ie indexEntry, target []byte) int {
		return bytes.Compare(ie.maxUserKey, target)
	})
	if blockIdx >= len(r.index) {
		return nil, 0, false, nil
	}

	f, err := r.fs.Open(r.path)
	if err != nil {
		return nil, 0, false, err
	}
	defer f.Close()

	// Scan starting at blockIdx (in case multiple versions of userKey span a block boundary)
	for bi := blockIdx; bi < len(r.index); bi++ {
		rawBlock, bErr := readAndVerifyBlock(f, r.index[bi].offset, r.index[bi].size)
		if bErr != nil {
			return nil, 0, false, bErr
		}
		entries, decErr := decodeDataBlockEntries(rawBlock)
		if decErr != nil {
			return nil, 0, false, decErr
		}
		for _, e := range entries {
			cmp := bytes.Compare(e.key.UserKey, userKey)
			if cmp == 0 && e.key.SeqNum <= snapshotSeq {
				return bytes.Clone(e.value), e.key.Type, true, nil
			}
			if cmp > 0 {
				return nil, 0, false, nil
			}
		}
	}
	return nil, 0, false, nil
}

// NewIterator returns an Iterator over all entries in the SSTable, verifying every block's CRC32C.
func (r *SSTableReader) NewIterator() (*SSTIterator, error) {
	f, err := r.fs.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var all []sstKV
	for _, ie := range r.index {
		rawBlock, bErr := readAndVerifyBlock(f, ie.offset, ie.size)
		if bErr != nil {
			return nil, bErr
		}
		entries, decErr := decodeDataBlockEntries(rawBlock)
		if decErr != nil {
			return nil, decErr
		}
		all = append(all, entries...)
	}
	return &SSTIterator{entries: all, pos: 0}, nil
}

type SSTIterator struct {
	entries []sstKV
	pos     int
}

func (it *SSTIterator) SeekToFirst() {
	it.pos = 0
}

func (it *SSTIterator) Seek(targetUserKey []byte) {
	idx, _ := slices.BinarySearchFunc(it.entries, targetUserKey, func(e sstKV, target []byte) int {
		return bytes.Compare(e.key.UserKey, target)
	})
	it.pos = idx
}

func (it *SSTIterator) Valid() bool {
	return it.pos >= 0 && it.pos < len(it.entries)
}

func (it *SSTIterator) Key() InternalKey {
	return it.entries[it.pos].key
}

func (it *SSTIterator) Value() []byte {
	return it.entries[it.pos].value
}

func (it *SSTIterator) Next() {
	it.pos++
}