# AetherKV — Binary & Wire Format Specification

This document specifies every on-disk binary layout and network frame implemented in AetherKV. All on-disk integers use **Little-Endian** encoding; TCP network framing uses **Big-Endian** (network byte order). Every checksum uses **CRC32C (Castagnoli polynomial `0x82F63B78`)**.

---

## 1. Raft Segmented WAL (`raft/wal/wal.go`)

Raft log entries are stored in sequentially numbered 64 MiB segment files (`%016d.wal`). Each entry uses a fixed **25-byte header** followed by the raw entry payload.

| Byte Offset     |   Size   | Field     | Type / Encoding | Description                                                         |
| :-------------- | :------: | :-------- | :-------------- | :------------------------------------------------------------------ |
| `0..4`          |   4 B    | `CRC32C`  | `uint32` (LE)   | Castagnoli CRC32C computed over `Header[8:25] \|\| Payload`         |
| `4..8`          |   4 B    | `Length`  | `uint32` (LE)   | Byte length of `Payload`                                            |
| `8..9`          |   1 B    | `RecType` | `uint8`         | `1 = TypeEntry`                                                     |
| `9..17`         |   8 B    | `Term`    | `uint64` (LE)   | Raft election term of the entry                                     |
| `17..25`        |   8 B    | `Index`   | `uint64` (LE)   | 1-based Raft log index                                              |
| `25..25+Length` | `Length` | `Payload` | `[]byte`        | Serialized `statemachine.Op` (empty for current-term no-op entries) |

**Recovery & OOM Defense:** Before allocating `make([]byte, Length)`, recovery checks `Length <= 64 MiB` and `offset + 25 + Length <= fileSize` (defending against torn-sector headers such as `0xAAAAAAAA`). On any bounds violation, short read, or CRC mismatch, recovery stops and calls `Truncate(offset)`.

---

## 2. LSM 32 KiB Block-Fragmented WAL (`lsm/wal.go`) & `WriteBatch` (`lsm/format.go`)

The LSM WAL (`active.wal`) divides the file into fixed **32 KiB (`32,768 B`) blocks**, matching the LevelDB/RocksDB framing model so arbitrary-sized batches split cleanly across block boundaries.

### Fragment Record Header (`7 bytes`)

| Byte Offset   |   Size   | Field      | Type / Encoding | Description                                                 |
| :------------ | :------: | :--------- | :-------------- | :---------------------------------------------------------- |
| `0..4`        |   4 B    | `CRC32C`   | `uint32` (LE)   | Castagnoli CRC32C computed over `FragType (1 B) \|\| Chunk` |
| `4..6`        |   2 B    | `Length`   | `uint16` (LE)   | Byte length of `Chunk` in this block                        |
| `6..7`        |   1 B    | `FragType` | `uint8`         | `1 = Full`, `2 = First`, `3 = Middle`, `4 = Last`           |
| `7..7+Length` | `Length` | `Chunk`    | `[]byte`        | Fragment of an encoded `WriteBatch`                         |

**Block Padding & Recovery:** If fewer than 7 bytes remain at the end of a 32 KiB block, the writer zero-pads (`0x00`) to the next 32 KiB boundary. On recovery, any corrupt CRC, incomplete fragment sequence (`First`/`Middle` without `Last`), or non-zero trailing garbage truncates `active.wal` back to the last complete `WriteBatch` boundary.

### Atomic `WriteBatch` Payload

Reassembled fragments decode into an atomic batch bundling user KV writes, client session dedup records, and `appliedIndex`:

| Byte Offset               |   Size   | Field       | Type / Encoding | Description                                                                          |
| :------------------------ | :------: | :---------- | :-------------- | :----------------------------------------------------------------------------------- |
| `0..8`                    |   8 B    | `StartSeq`  | `uint64` (LE)   | First MVCC sequence number assigned to entry 0                                       |
| `8..12`                   |   4 B    | `Count`     | `uint32` (LE)   | Number of `BatchEntry` records following                                             |
| _Repeated `Count` times:_ |          |             |                 |                                                                                      |
| `+0..1`                   |   1 B    | `ValueType` | `uint8`         | `1 = Value`, `2 = Tombstone`, `3 = VLogPtr`                                          |
| `+1..5`                   |   4 B    | `KeyLen`    | `uint32` (LE)   | Byte length of `UserKey`                                                             |
| `+5..9`                   |   4 B    | `ValLen`    | `uint32` (LE)   | Byte length of `Value` (`0` for Tombstone, `24` for `VLogPtr`)                       |
| `+9..`                    | `KeyLen` | `UserKey`   | `[]byte`        | Namespaced key (`u:<key>`, `\x00sys:applied_index`, or `\x00sys:sess:<20-digit-id>`) |
| `+9+KeyLen..`             | `ValLen` | `Value`     | `[]byte`        | Inline value bytes or 24-byte `VLogPointer`                                          |

---

## 3. Immutable SSTable Format (`lsm/sstable.go`, `lsm/bloom.go`)

```text
+---------------+---------------+-----+-------------------+---------------+------------------+
| Data Block 0  | Data Block 1  | ... | Bloom Filter Blk  | Index Block   | 40-Byte Footer   |
| (~4 KiB+4B)   | (~4 KiB+4B)   |     | (Bitset+k+4B CRC) | (Offsets+4B)  | (0xAE74E255)     |
+---------------+---------------+-----+-------------------+---------------+------------------+
```
